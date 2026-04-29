# VectorBase — Claude Code 项目指令

## 项目结构

```
vectorbase/
├── src/                  # 基础库 (libvectorbase.a) — 构建：make -C src
│   ├── vb_type.h         # 基础类型：u8/u16/u32/u64/i32/i64/f32/f64/usize/block_id_t
│   ├── interface.h       # DEFINE_CLASS / EXTENDS / VCALL 虚方法宏
│   ├── storage.h         # BlockManager, Block, BLOCK_SIZE=8192
│   ├── catalog.h/c       # 顶层 Catalog / SchemaCatalogEntry
│   └── ...               # datatable, segment, hash, vector 等基础组件
│
└── tmp/                  # 向量索引层 (活跃开发区)
    ├── src/              # 所有向量索引源文件
    └── tests/            # 所有测试 (test_*.c，自动发现)
```

## 构建与测试

```bash
# 构建并运行所有测试（主要命令）
make -C tmp test

# 仅构建
make -C tmp

# 重新构建基础库（修改 src/*.h 后必须执行）
make -C src clean && make -C src

# 清理测试产物
make -C tmp clean
```

**注意**：`src/Makefile` 无头文件依赖追踪，修改 `src/*.h` 后必须手动执行
`make -C src clean && make -C src`。

## 核心架构

### 物理地址：ctid（唯一标识符）

```c
typedef ItemPtr NodePtr;   // {ip_blkid_hi, ip_blkid_lo, ip_posid} = 6 bytes
// 完全对齐 PostgreSQL ItemPointerData
```

- **没有 `next_internal_id` 或顺序 iid 计数器** — 已彻底消除
- `itemptr_pack(p) → u64`：用于 hmap / free_list / 磁盘格式存储
- `itemptr_unpack(u64) → ItemPtr`：反序列化
- `heap_ctid` ≠ `emb_ctid`：两者是不同地址空间的 ItemPtr（Option C 解耦设计）

### 三层存储引擎（StorageTable）

```
StorageTable
├── TamHeapTable   — RowStore（MVCC 权威 + 用户列），隐藏列 _emb_iid 存 packed emb_ctid
├── TamEmbTable    — EmbeddingStore（f32 向量，SegmentTree + free_list）
└── TamColTable    — ColumnStore/DataTable（标量列，append-only）
```

每个引擎实现 `TamRoutine` vtable（**数据库表的统一抽象**）：

```c
typedef struct {
    void (*append)(TableAm* am, const void* data);
    int  (*get)(TableAm* am, u64 seq_idx, void* out);
    void (*write_blocks)(TableAm* am, BlockManager* bm, MetaBlockWriter* w);
    void (*load_blocks)(TableAm* am, BlockManager* bm, MetaBlockReader* r);
    u64  (*count)(TableAm* am);
    void (*destroy)(TableAm* am);
    void (*append_chunk)(TableAm* am, const VbChunk* chunk, TamInsertCtx* ctx);
    int  (*read_chunk)(TableAm* am, const TamReadCtx* ctx, VbChunk* out_chunk,
                       usize* out_idx, usize count);
} TamRoutine;
```

### VbChunk / VectorBase 设计哲学

**VectorBase 是统一的向量原语**——既可以是嵌入式向量，也可以是行式向量：

```c
typedef struct {
    TypeID     type;   // TYPE_FLOAT32 / TYPE_INT64 / ...
    usize      count;  // count=dim → 嵌入向量；count=1 → 标量
    data_ptr_t data;   // 指向实际数据的指针
} VectorBase;
```

**VbChunk 是统一的批量数据容器**，`append_chunk`（数据入）与 `read_chunk`（数据出）完全对称：

```
VBCHUNK_EMBED 模式（行格式）：
  arrays[i]    → 第 i 行的嵌入向量  VectorBase{FLOAT32, dim, f32*}
  payloads[i]  → 第 i 行的 HEAP 用户列  RowVal[user_cols]
  col_rows[i]  → 第 i 行的标量列   RowVal[ncols]

VBCHUNK_COLUMN 模式（列格式）：
  arrays[i]    → 第 i 列的所有行值  VectorBase{TypeID, nrows, data*}
```

**read_chunk 字段归属**（引擎不修改 `*out_idx`，由 orchestrator 管理）：

| 引擎 | 写入字段 | 数据类型 |
|------|---------|---------|
| `TamEmbTable` | `out_chunk->arrays[*out_idx]` | `VectorBase{FLOAT32, dim, f32*}` 零拷贝 |
| `TamColTable` | `out_chunk->col_rows[*out_idx]` | `RowVal[ncols]` |
| `TamHeapTable` | `out_chunk->payloads[*out_idx]` | `RowVal[user_cols]` |

`get(seq_idx)` 使用顺序索引，不是 ctid。三个引擎地址空间不同（heap_ctid ≠ emb_ctid ≠
col_seq_idx），无法统一用 ctid。`fill_out_vals` 通过 `read_chunk` vtable 调度各引擎，
再由 bridge 步骤将 VbChunk 压平为 RowVal* `[COL scalars..., EMB vector, HEAP user cols...]`。

### 批量插入上下文（TamInsertCtx）

```c
typedef struct {
    u64*       out_row_ids; // HEAP 填充；ANN 在 row_ids==NULL 时读取
    ItemPtr*   emb_ctids;   // EMB 先填充；HEAP 读取写入隐藏列
    const u64* row_ids;     // 调用方提供（NULL = 自动分配）
    usize      count;
} TamInsertCtx;
// 顺序：EMB → HEAP → COL → ANN
```

### MVCC 设计（对齐 PostgreSQL）

- `TupleHdr`（40B）= PG `HeapTupleHeaderData`：xmin / xmax / t_ctid / infomask
- ANN 节点：`NodeMvcc{xmax}` — 轻量活跃性缓存，权威来源在 RowStore
- `NODE_IS_ALIVE(nm)` ↔ `xmax == INVALID_TXN_ID`（0）

#### DELETE 机制（row_store_delete_impl）

DELETE 只写 header，物理数据原地不动，分三阶段：

**阶段一：DELETE 瞬间**（`storage_table_delete_row` → `row_store_delete_impl`）
- `id_map` 查 row_id → 找 ctid → 定位物理 tuple
- **原地修改 TupleHdr**（仅改头，列数据/f32 不动）：
  - `t_xmax = txn_id`（记录 deleter XID）
  - `t_infomask &= ~(HEAP_XMAX_INVALID | HEAP_XMAX_COMMITTED)`（清除"xmax 无效"标志）
  - `t_ctid` 保持 self-pointing（最新版本 insert 时已设为自指）
- `id_map` **不动**（row_id 仍指向这个 ctid）
- ANN：`VCALL(ann_index, remove, row_id)` 设置 `NodeMvcc.xmax`

**阶段二：DELETE 后可见性判断**
- `row_store_is_alive`：`HEAP_XMAX_INVALID` 还在 → alive；被清除 → dead
- `row_store_get_by_ctid` 中 `t_ctid` 的双重含义（对齐 PG）：
  - `t_ctid` **self-pointing**（block_id+slot == 自身）→ 已删除，返回 -1
  - `t_ctid` **forward-pointing**（指向别处）→ UPDATE 旧版本，允许 chain traversal

**阶段三：Vacuum 物理回收**（`safe_xid` 后，`storage_table_vacuum`）

dead 判定：`HEAP_XMAX_INVALID` cleared && `t_xmax != 0` && `t_xmax <= safe_xid`

两个独立 free_list 在两处分别填充：

| | RowStore.free_list | EmbeddingStore.free_list |
|---|---|---|
| 填充 | vacuum Pass 2（`vacuum_row_store`） | vacuum Pass 1（`table_am.c`） |
| 内容 | packed heap ctid | packed emb ctid |
| 复用 | 下次 `row_store_insert`（`rs_reuse_slot`） | 下次 `embedding_store_append_and_get_ctid` |
| 复用方式 | 写入 LP_UNUSED 槽 | 原地 overwrite f32 |

Pass 2（`vacuum_row_store`）额外操作：`lp_off=0, lp_len=0`（标 LP_UNUSED）；
`hmap_delete(&id_map, &row_id)`；id_map 负载 < 25% 时 shrink bucket。

#### RowStore 页面布局（对齐 PG slotted page）

每个 BlockSegment 是一个 8KB（`BLOCK_SIZE=8192`）slotted page：

```
offset 0
┌─────────────────────────────────────────┐
│ pd_lower (u16)  │  pd_upper (u16)       │  ← RS_BLOCK_HDR_SIZE = 4B
├─────────────────────────────────────────┤
│ RowSlotId[0]: lp_off(u16) + lp_len(u16)│  ← RS_SLOT_SIZE = 4B 每条
│ RowSlotId[1]: ...                       │  slot 数组向下增长（pd_lower++）
│ ...                                     │
├─────────────────────────────────────────┤
│            free space                   │  = pd_upper - pd_lower
├─────────────────────────────────────────┤
│ tuple data (新插入的 tuple 在最低)      │  tuple 数据向上增长（pd_upper--）
│ ...                                     │
│ tuple[0] data                           │
└─────────────────────────────────────────┘
offset 8192
```

常量：`RS_BLOCK_HDR_SIZE=4`，`RS_SLOT_SIZE=4`，`RS_MAX_TUPLE_SIZE = BLOCK_SIZE - 4 - 4 = 8184B`

**物理 ctid 编码**（对齐 PG `ItemPointerData`，6B）：
```
ip_blkid_hi(u16) | ip_blkid_lo(u16) = block_id 低 32 位（BlockManager 分配的真实磁盘块号；bm==NULL 时为本地顺序 0,1,2,…）
ip_posid(u16)                        = 1-based slot 号（PG OffsetNumber）
packed u64 = (block_id << 16) | ip_posid
iid  = seg.base.start + (ip_posid - 1)   // seq_idx 映射
```

**on-disk tuple 格式**（每个 slot 内）：
```
[TupleHdr (40B)][row_id (u64, 8B)][serialized col_0][col_1]...
```

**TupleHdr（40B，对齐 PG `HeapTupleHeaderData`）**：

| offset | size | 字段 | 含义 |
|--------|------|------|------|
| 0 | 8 | `t_xmin` | 插入事务 XID（64位，无需回卷） |
| 8 | 8 | `t_xmax` | 删除/更新事务 XID（0 = `INVALID_TXN_ID` = 存活） |
| 16 | 4 | `t_cid` | Command ID |
| 20 | 6 | `t_ctid` | 物理位置（block_id, slot+1），UPDATE 后指向新版本 |
| 26 | 2 | `t_infomask2` | 低 11 位 = natts |
| 28 | 2 | `t_infomask` | `HEAP_XMAX_INVALID`（0x0800）等可见性 flags |
| 30 | 1 | `t_hoff` | 固定 = `sizeof(TupleHdr)` = 40 |
| 31 | 1 | `_pad` | 对齐填充 |
| 32 | 8 | `null_bits` | bit i=1 → col i 为 NULL（最多 64 列）|

与 PG 的差异：`t_xmin/t_xmax` 64位（PG 32位，有回卷问题）；`null_bits` 固定 u64（PG 可变长 `t_bits[]`）；`t_hoff` 恒为 40（PG 可变）。

**LP_UNUSED 标志**：`lp_off=0, lp_len=0`（slot 占位保留，不从 slot 数组移除）。

#### RowStore free_list 槽复用机制（rs_reuse_slot）

RowStore 页面布局见上节。

`row_store_insert` 优先复用 free_list（`RowStore.free_list: Vector<u64>`，存 packed heap ctid）：

```
pop free_list → free_ctid → rs_reuse_slot(store, free_ctid, tuple)
    │
    ├─ 找 segment/page（按 block_id）
    ├─ 确认 slot lp_len == 0（LP_UNUSED，安全检查）
    ├─ 计算 ser_size；检查 rs_free_space(page) >= ser_size
    │      不够 → rs_page_compact(page)（碎片整理，见下）
    │             仍不够 → 返回 INVALID_ITEM_PTR → 回退到 rs_append_to_store
    ├─ 设 tuple->hdr.t_ctid = (block_id, slot_idx)（物理自指）
    ├─ pd_upper -= ser_size；serialize_tuple_into(page + pd_upper, ...)
    └─ 激活 slot：lp_off = pd_upper，lp_len = ser_size

free_list 为空 → rs_append_to_store（高水位线增长）
```

`rs_page_compact`（对齐 PG `PageRepairFragmentation`）：
- 将页内所有 live tuples（lp_len != 0）整体上移至页顶，消除 LP_UNUSED 留下的空洞
- 更新每个 live slot 的 lp_off；重置 pd_upper
- dead slot 在 slot 数组中原地保留（lp_len=0），不移动
- 方向：从低到高（src ≤ dest），`memmove` 安全；O(page_size)

- Vacuum = `storage_table_vacuum(safe_xid)` 两个 Pass，实现在 `table_am.c`：
  - **Pass 1**：扫描所有 RowStore segments/slots，找 dead tuple，读 hidden `_emb_iid`（`cols[0]` = packed emb_ctid），push 到 `EmbeddingStore.free_list`。**必须在 Pass 2 之前**，否则 LP_UNUSED 后无法再读 tuple。
  - **Pass 2**：调 `vacuum_row_store(rs, safe_xid)`，标 LP_UNUSED，清 id_map。
  - `EmbeddingStore` **故意没有 delete**——生命周期由 heap xmax 决定，f32 在 free_list 复用时原地 overwrite，高水位线不收缩。
  - ANN 索引另有 `index_catalog_repair_graph`（填充 ANN free_list，修复图结构）

### 向量索引（ANN）

所有索引实现 `VectorIndex` vtable（`DEFINE_CLASS` 宏，`VCALL` 调用）：

| 索引 | 节点结构 | free_list | 槽复用函数 |
|------|---------|-----------|-----------|
| FlatIndex | `NodeMvcc mvcc_arr[]` | — | — |
| HNSWIndex | `HNSWNode{emb_ctid, mvcc}` | `free_list` | `hnsw_repair_graph` |
| IVFIndex | `IVFEntry{emb_ctid, mvcc}` | `free_slots`（standalone） | `ivf_repair` |
| DiskANNIndex | `DiskANNNode{emb_ctid, mvcc}` | `free_list` | `diskann_repair_graph` |

- `emb_ctid: ItemPtr` — 节点到 EmbeddingStore 的物理指针，与节点 slot 解耦
- IVF 用 `free_slots`（standalone 模式），HNSW/DiskANN 用 `free_list` — 有意区分，不是冗余

### 页面感知磁盘格式（index_page.h）

| 索引 | Magic | Page ID | Opaque | 对齐目标 |
|------|-------|---------|--------|---------|
| HNSW | `0xA953A953` | `0xFF90` | 8B | pgvector |
| IVF | `0x14FF1A7` | `0xFF84` | 8B | pgvector IvfFlat |
| DiskANN | `0x44534E4E` | `0xA202` | 4B（无 next_block）| pgvectorscale |

磁盘结构字段名 `u64 emb_iid` 保留（存储 `itemptr_pack(emb_ctid)` 值）。
隐藏列字符串 `"_emb_iid"` 保留。两者均为 checkpoint 格式兼容保留，**不得修改**。

## 命名约定

| 概念 | 正确命名 | 禁用命名 |
|------|---------|---------|
| ANN 节点到向量的指针 | `emb_ctid: ItemPtr` | ~~emb_iid~~ |
| EmbeddingStore 最新槽 | `last_emb_ctid` | ~~last_emb_iid~~ |
| 批量插入上下文 heap 输出 | `out_row_ids` | ~~iids~~ |
| 搜索结果物理位置 | `emb_ctid_packed` | ~~internal_id~~ |
| ANN 旧槽索引 | `old_slot` | ~~old_iid~~ |
| ColumnStore 顺序索引 | `seq_idx` | ~~iid~~ |
| RowStore 槽计数 | `row_store_slot_count()` | ~~next_internal_id~~ |

## 编码规范

- **类型**：使用 `u8/u16/u32/u64/i32/i64/f32/f64/usize`，不用 `int/long/size_t`
- **虚方法**：`VCALL(obj, method, args...)` — 不直接调用函数指针
- **内存**：手动管理，无 GC。`malloc/free` 显式配对；`TAM_DESTROY(am)` 释放引擎自身
- **错误返回**：`int`（0 成功 / -1 失败）或 `ItemPtr`（`INVALID_ITEM_PTR` = 失败）
- **不得过度防御**：只在系统边界（外部输入）做校验，内部不变量用 `assert()`

## 禁止行为
- **不得修改src下代码(除非我指示你),只能在tmp下修改**
- **不得修改src下代码的测试用例写到tests, tmp/src下的代码的测试用例写到tmp/tests**
- **不得引入顺序 iid 计数器**（如 `next_internal_id`、新增 `emb_iid` 字段）
- **不得修改磁盘格式字段名** `u64 emb_iid` 及字符串 `"_emb_iid"` — checkpoint 兼容
- **不得在 `src/store.h` 中定义 `EmbeddingStore` 结构体主体** — 与 `tmp/src/embedding_store.h` 冲突
- **不得对 `get(seq_idx)` 统一改为 ctid** — 三引擎地址空间不同，无法统一
- **修改代码后必须运行 `make -C tmp test` 验证全部测试通过**（测试数量随新增测试文件增长）

## 测试规范

```c
// 所有测试文件遵循此模式
static int pass_count = 0, fail_count = 0;
#define CHECK(cond, msg) do { \
    if (cond) { pass_count++; printf("[PASS] %s\n", msg); } \
    else      { fail_count++; printf("[FAIL] %s\n", msg); } \
} while(0)
// main() 返回 fail_count > 0 ? 1 : 0
```

Catalog 测试套路：
```c
catalog_create() → catalog_create_schema() → index_catalog_create() → index_catalog_get()
```

## 关键 API 速查

```c
// RowStore
ItemPtr  row_store_insert(store, row_id, tup);
int      row_store_get_by_ctid(store, ItemPtr, RowTuple*);
int      row_store_get_by_seqidx(store, u64 seq_idx, RowTuple*);  // ColumnStore compat
ItemPtr  row_store_get_latest_ctid(store, row_id);
u64      row_store_ctid_to_idx(store, ItemPtr);    // ctid → seq_idx
u64      row_store_slot_count(store);              // 替代旧 next_internal_id

// EmbeddingStore
ItemPtr    embedding_store_append_and_get_ctid(store, vec);  // 优先复用 free_list slot，否则顺序 append；设置 last_emb_ctid
const f32* embedding_store_get_ptr_ctid(store, ItemPtr);
void       embedding_store_write_at_ctid(store, ItemPtr, f32*);
//
// EmbeddingStore GC 机制（free_list slot 复用）：
//   EmbeddingStore 故意没有 delete — 向量生命周期由 RowStore MVCC (heap xmax) 决定。
//   GC 路径：
//     1. DELETE row → heap xmax 设置，f32 原地保留（旧事务仍可读）
//     2. vacuum Pass1 → 扫描 dead heap tuples，读 _emb_iid(cols[0])，
//                        push emb_ctid 到 EmbeddingStore.free_list
//     3. vacuum Pass2 → vacuum_row_store 标 LP_UNUSED
//     4. 下次 append_and_get_ctid → pop free_list，原地 overwrite f32
//   高水位线（store.count）永不收缩，free_list 在水位线内复用槽位。
//   free_list 持久化：packed u64 ctid 数组写入 checkpoint，load 时还原。

// 搜索结果
typedef struct { u64 id; u64 emb_ctid_packed; f32 distance; } SearchResult;
void result_heap_push(heap, id, itemptr_pack(node->emb_ctid), dist);

// 批量插入（唯一公开插入入口）
void storage_table_insert_datachunk(StorageTable*, const VbChunk*, const u64* row_ids);

// vtable 宏
TAM_APPEND(am, data)                              // 单行追加，无 seq_idx 参数
TAM_GET(am, seq_idx, out)                         // 按 seq_idx 读取
TAM_COUNT(am)
TAM_DESTROY(am)
TAM_APPEND_CHUNK(am, chunk, ctx)                  // 批量插入（主路径）
TAM_READ_CHUNK(am, ctx, out_chunk, out_idx, count) // 批量读取（对称主路径）
VCALL(idx, insert, row_id, vec)
VCALL(idx, search, query, k, results)
```

## StorageTable CRUD 必须通过 TAM vtable（架构约束）

`storage_table_*` 层（增删改查）**禁止**直接调用底层 Store API（`row_store_*`、`embedding_store_*`），也**禁止**将 `TableAm*` 强制转换为具体引擎类型（`TamHeapTable*`、`TamEmbTable*`、`TamColTable*`）来访问成员。

必须且只能使用以下 TAM vtable 宏：

```c
TAM_SCAN(am, ctx, chunk)                  // 推进扫描游标（heap 驱动）
TAM_GET_HDR(am, ctid, out_hdr)            // 读取 TupleHdr（仅 heap engine 有效）
TAM_PREPARE_READ(am, ctid, ctx, tup_buf)  // 预加载 heap tuple 到 TamReadCtx
TAM_FREE_READ(am, ctx)                    // 释放 prepare_read 分配的资源
TAM_DELETE(am, ctx)                       // 删除（支持 ctx->xid 事务路径）
TAM_APPEND_CHUNK(am, chunk, ctx)          // 批量插入
TAM_READ_CHUNK(am, ctx, out_chunk, ...)   // 批量读取
```

**各引擎** vtable 内部实现可以（也必须）直接访问自己的 store：`heap_scan` 访问 `ht->store`，`emb_append_chunk` 访问 `et->store`，`col_append_chunk` 访问 `ct->cs`。封装边界在 `StorageTable` 层，不在引擎层。
