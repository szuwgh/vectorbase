#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include "table.h"
#include "segment.h"
#include "vb_type.h"
#include "vector.h"
#include "operator.h"

static void heapTable_append(TableAmRoutine* am, VectorBase* v, TupleVal* payloads,
                             usize n_payloads);
static void heapTable_append_chunk(TableAmRoutine* am, const DataChunk* chunk, TamInsertCtx* ctx);
static int heapTable_scan(TableAmRoutine* am, TamScanCtx* ctx, TableQueryResult* results,
                          usize max_results);
static void heapTable_write_blocks(TableAmRoutine* am, BlockManager* bm, MetaBlockWriter* w);
static void heapTable_load_blocks(TableAmRoutine* am, BlockManager* bm, MetaBlockReader* r);
static void heapTable_destory(TableAmRoutine* am);

static void embeddingHeapTable_append(TableAmRoutine* am, VectorBase* v, TupleVal* payloads,
                                      usize n_payloads);
static void embeddingHeapTable_append_chunk(TableAmRoutine* am, const DataChunk* chunk,
                                            TamInsertCtx* ctx);
static int embeddingHeapTable_scan_chunk(TableAmRoutine* am, TamScanCtx* ctx,
                                         TableQueryResult* results, usize max_results);
static void embeddingHeapTable_write_blocks(TableAmRoutine* am, BlockManager* bm,
                                            MetaBlockWriter* w);
static void embeddingHeapTable_load_blocks(TableAmRoutine* am, BlockManager* bm,
                                           MetaBlockReader* r);
static void embeddingHeapTable_destory(TableAmRoutine* am);

static const TableAmRoutineVTable heap_am_routine = {
    .append = heapTable_append,
    .append_chunk = heapTable_append_chunk,
    .scan = heapTable_scan,
    .write_blocks = heapTable_write_blocks,
    .load_blocks = heapTable_load_blocks,
    .destroy = heapTable_destory,
};

static const TableAmRoutineVTable embedding_heap_am_routine = {
    .append = embeddingHeapTable_append,
    .append_chunk = embeddingHeapTable_append_chunk,
    .scan = embeddingHeapTable_scan_chunk,
    .write_blocks = embeddingHeapTable_write_blocks,
    .load_blocks = embeddingHeapTable_load_blocks,
    .destroy = embeddingHeapTable_destory,
};

/* 创建旧版 DataTable 对象；当前只分配壳结构，具体存储初始化已迁移到新路径。 */
DataTable* Datatable_create(StorageManager* manager, char* schema_name, char* table_name,
                            usize column_count, TypeID* column_types)
{
    DataTable* table = malloc(sizeof(DataTable));
    (void)manager;
    (void)schema_name;
    (void)table_name;
    (void)column_count;
    (void)column_types;
    return table;
}

/* 初始化一个批量列容器，并为每一列分配连续缓冲区。 */
void DataChunk_init(DataChunk* chunk, Vector types)
{
    chunk->count = types.size;
    chunk->arrays = calloc(chunk->count, sizeof(VectorBase));
    for (usize i = 0; i < chunk->count; i++)
    {
        VectorBase_init(&chunk->arrays[i], VECTOR_AT(&types, i, TypeID));
    }

    usize size = 0;
    VECTOR_FOREACH(&types, type)
    {
        size += get_typeid_size(*(TypeID*)type) * STANDARD_VECTOR_SIZE;
    }
    assert(size > 0);
    data_ptr_t own_data = malloc(size);
    chunk->data = own_data;
    chunk->size = size;
    memset(own_data, 0, size);
    for (usize i = 0; i < chunk->count; i++)
    {
        chunk->arrays[i].data = own_data;
        own_data += get_typeid_size(VECTOR_AT(&types, i, TypeID)) * STANDARD_VECTOR_SIZE;
    }
}

/* 释放 DataChunk 持有的列数组和底层数据缓冲区。 */
void dataChunk_deinit(DataChunk* chunk)
{
    if (chunk->data)
    {
        free(chunk->data);
        chunk->data = NULL;
    }
    else
    {
        for (usize i = 0; i < chunk->count; i++) VectorBase_deinit(&chunk->arrays[i]);
    }
    free(chunk->arrays);
    chunk->arrays = NULL;
    chunk->count = 0;
}

/* 仅清空每列的元素计数，保留已分配的缓冲区以便复用。 */
void dataChunk_clear(DataChunk* chunk)
{
    for (usize i = 0; i < chunk->count; i++)
    {
        chunk->arrays[i].count = 0;
    }
}

/* 把 DataChunk 重置为初始写入状态，并恢复每列的数据指针布局。 */
void dataChunk_reset(DataChunk* chunk)
{
    data_ptr_t ptr = chunk->data;
    for (usize i = 0; i < chunk->count; i++)
    {
        chunk->arrays[i].count = 0;
        chunk->arrays[i].data = ptr;
        ptr += get_typeid_size(chunk->arrays[i].type) * STANDARD_VECTOR_SIZE;
    }
}

/* 用给定列向量覆盖指定位置的列描述。 */
void dataChunk_append(DataChunk* chunk, usize index, VectorBase src)
{
    assert(index < chunk->count);
    chunk->arrays[index] = src;
}

/* 返回当前批量里第一列的元素个数，作为整批的行数。 */
usize dataChunk_size(DataChunk* chunk)
{
    if (chunk->count == 0) return 0;
    return chunk->arrays[0].count;
}

/* 旧列存插入入口的占位实现；当前新架构不会走到这里。 */
void datatable_append_column(DataTable* table, DataChunk* chunk)
{
    (void)table;
    (void)chunk;
}

/* 旧列存扫描入口的占位实现；当前始终返回无数据。 */
bool datatable_scan(DataTable* table, ScanState* state, DataChunk* output, usize* column_ids,
                    usize col_count)
{
    (void)table;
    (void)state;
    (void)output;
    (void)column_ids;
    (void)col_count;
    return false;
}

/* 初始化旧扫描状态对象；当前实现保留为空壳。 */
void datatable_init_scan(DataTable* table, ScanState* state)
{
    (void)table;
    (void)state;
}

/* 释放旧扫描状态对象持有的资源；当前实现无实际释放动作。 */
void scanstate_deinit(ScanState* state)
{
    (void)state;
}
#define HEAP_COLS_MAX 64

#define COL_TABLE_MAX 64

/* 组装 heap 写入所需的临时元组，把 payload 和 emb_ctid 填到输出结构里。 */
static void build_heap_tuple(const HeapTable* heap, ItemPtr emb_ctid, const TupleVal* payload_vals,
                             usize nvals, TupleVal cols_buf[HEAP_COLS_MAX], HeapTuple* out)
{
    usize ncols = heap->schema.ncols < HEAP_COLS_MAX ? heap->schema.ncols : HEAP_COLS_MAX;
    u32 null_bits = 0;

    for (usize k = 0; k < ncols; k++)
    {
        cols_buf[k] =
            (payload_vals && k < nvals) ? payload_vals[k] : (TupleVal){.type = TUPLE_COL_NULL};
        if (cols_buf[k].type == TUPLE_COL_NULL) null_bits |= (1u << k);
    }

    out->cols = ncols > 0 ? cols_buf : NULL;
    out->ncols = (u16)ncols;
    out->hdr.null_bits = null_bits;
    out->hdr.t_emb_ctid = emb_ctid;
}

/* 执行一次 heap 侧真实插入，必要时从批量上下文读取 emb_ctid 和 xid。 */
static void heapTable_append_impl(TableAmRoutine* am, TamInsertCtx* ctx, VectorBase* v,
                                  const Datum* val, usize n_payloads)
{
    HeapTable* ht = (HeapTable*)am;
    TxnId xid = ctx ? ctx->xid : 0;
    ItemPtr emb_ctid =
        (ctx && ctx->emb_ctids) ? ctx->emb_ctids[ctx->current_index] : INVALID_ITEM_PTR;
    (void)v;
    (void)n_payloads;
    heapStore_insert(&ht->store, xid, emb_ctid, val, 0);
}

/* 处理单行 heap 追加；当前路径不携带 embedding 位置。 */
static void heapTable_append(TableAmRoutine* am, VectorBase* v, TupleVal* payloads,
                             usize n_payloads)
{
    HeapTable* ht = (HeapTable*)am;
    LWLockAcquire(&ht->store.lock, LW_EXCLUSIVE);
    heapStore_insert(&ht->store, 0, INVALID_ITEM_PTR, NULL, 0);
    LWLockRelease(&ht->store.lock);
    (void)v;
    (void)payloads;
    (void)n_payloads;
}

/* 批量把 DataChunk 中的行写入 heap 存储。 */
static void heapTable_append_chunk(TableAmRoutine* am, const DataChunk* chunk, TamInsertCtx* ctx)
{
    HeapTable* ht = (HeapTable*)am;
    LWLockAcquire(&ht->store.lock, LW_EXCLUSIVE);
    for (usize i = 0; i < chunk->count; i++)
    {
        ctx->current_index = i;
        heapTable_append_impl(am, ctx, &chunk->arrays[i],
                              chunk->payloads ? chunk->payloads[i] : NULL, chunk->n_payloads);
    }
    LWLockRelease(&ht->store.lock);
}

/* heap 扫描接口的旧实现占位；当前未完成具体逻辑。 */
static int heapTable_scan(TableAmRoutine* am, TamScanCtx* ctx, TableQueryResult* results,
                          usize max_results)
{
    HeapTable* ht = (HeapTable*)am;
    (void)ht;
    (void)ctx;
    (void)results;
    (void)max_results;
    return 0;
}

/* 把 heap 表写入块存储；当前为空实现。 */
static void heapTable_write_blocks(TableAmRoutine* am, BlockManager* bm, MetaBlockWriter* w)
{
    (void)am;
    (void)bm;
    (void)w;
}

/* 从块存储恢复 heap 表；当前为空实现。 */
static void heapTable_load_blocks(TableAmRoutine* am, BlockManager* bm, MetaBlockReader* r)
{
    (void)am;
    (void)bm;
    (void)r;
}

/* 初始化 HeapTable，包括底层 HeapStore 和对应的 vtable。 */
void HeapTable_init(HeapTable* table, const TableSchema* schema, BlockManager* bm)
{
    HeapStore_init(&table->store, schema, bm);
    table->schema = *schema;
    table->base.vtable = (TableAmRoutineVTable*)&heap_am_routine;
}

/* 销毁 HeapTable 的 vtable 入口；当前不额外释放资源。 */
static void heapTable_destory(TableAmRoutine* am)
{
    (void)am;
}

/* 释放 HeapTable 底层的 HeapStore 资源。 */
void HeapTable_deinit(HeapTable* table)
{
    HeapStore_deinit(&table->store);
}

/* 释放向量+heap 组合表持有的 embedding 与 heap 资源。 */
void EmbeddingHeapTable_deinit(EmbeddingHeapTable* table)
{
    EmbeddingStore_deinit(&table->embed_store);
    HeapStore_deinit(&table->heap_table.store);
    LWLockDestroy(&table->embed_store.lock);
    LWLockDestroy(&table->heap_table.store.lock);
}

/* 初始化向量+heap 组合表，使其同时具备向量存储和 heap 存储能力。 */
void EmbeddingHeapTable_init(EmbeddingHeapTable* table, i16 dimension, const TableSchema* schema,
                             BlockManager* bm)
{
    HeapTable_init(&table->heap_table, schema, bm);
    EmbeddingStore_init(&table->embed_store, dimension);
    table->base.vtable = (TableAmRoutineVTable*)&embedding_heap_am_routine;
}

/* 处理组合表的单行追加；当前未实现具体逻辑。 */
static void embeddingHeapTable_append(TableAmRoutine* am, VectorBase* v, TupleVal* payloads,
                                      usize n_payloads)
{
    (void)am;
    (void)v;
    (void)payloads;
    (void)n_payloads;
}

/* 先写 embedding，再把对应 payload 写入 heap，完成组合表批量插入。 */
static void embeddingHeapTable_append_chunk(TableAmRoutine* am, const DataChunk* chunk,
                                            TamInsertCtx* ctx)
{
    EmbeddingHeapTable* et = (EmbeddingHeapTable*)am;
    LWLockAcquire(&et->embed_store.lock, LW_EXCLUSIVE);
    for (usize i = 0; i < chunk->count; i++)
    {
        ItemPtr ctid = embeddingStore_append_and_get_ctid(&et->embed_store, &chunk->arrays[i]);
        if (ctx && ctx->emb_ctids) ctx->emb_ctids[i] = ctid;
    }
    LWLockRelease(&et->embed_store.lock);

    TableAmRoutine* heap_am = (TableAmRoutine*)&et->heap_table;
    LWLockAcquire(&et->heap_table.store.lock, LW_EXCLUSIVE);
    for (usize i = 0; i < chunk->count; i++)
    {
        ctx->current_index = i;
        heapTable_append_impl(heap_am, ctx, &chunk->arrays[i],
                              chunk->payloads ? chunk->payloads[i] : NULL, chunk->n_payloads);
    }
    LWLockRelease(&et->heap_table.store.lock);
}

typedef struct
{
    ItemPtr heap_ctid;
    f32 dist;
    Datum* vals;
    const f32* vec;
    u64 null_bits;
} EmbScanCand;

/* 比较两个扫描候选的距离，用于按距离排序。 */
static int emb_scan_cand_cmp(const void* a, const void* b)
{
    f32 da = ((const EmbScanCand*)a)->dist;
    f32 db = ((const EmbScanCand*)b)->dist;
    return da < db ? -1 : da > db ? 1 : 0;
}

/* 把一个候选压入 top-k 最大堆。 */
static void topk_push(EmbScanCand* h, usize* sz, EmbScanCand c)
{
    h[(*sz)++] = c;
    usize i = *sz - 1;
    while (i > 0)
    {
        usize p = (i - 1) / 2;
        if (h[p].dist >= h[i].dist) break;
        EmbScanCand t = h[p];
        h[p] = h[i];
        h[i] = t;
        i = p;
    }
}

/* 用更好的候选替换堆顶，并重新维护最大堆性质。 */
static void topk_replace_root(EmbScanCand* h, usize n, EmbScanCand c)
{
    free(h[0].vals);
    h[0] = c;
    usize i = 0;
    for (;;)
    {
        usize l = 2 * i + 1, r = 2 * i + 2, m = i;
        if (l < n && h[l].dist > h[m].dist) m = l;
        if (r < n && h[r].dist > h[m].dist) m = r;
        if (m == i) break;
        EmbScanCand t = h[i];
        h[i] = h[m];
        h[m] = t;
        i = m;
    }
}

#define TOPK_STACK_MAX 64

/* 对组合表执行向量扫描，返回距离最近的 top-k 结果。 */
static int embeddingHeapTable_scan_chunk(TableAmRoutine* am, TamScanCtx* ctx,
                                         TableQueryResult* results, usize max_results)
{
    const VectorCondition* vc = ctx->vec_cond;
    if (max_results == 0 || !vc) return 0;
    EmbeddingHeapTable* et = (EmbeddingHeapTable*)am;
    const TableSchema* schema = &et->heap_table.schema;
    usize heap_ncols = (usize)schema->ncols;
    usize heap_cap = max_results;
    EmbScanCand stack_buf[TOPK_STACK_MAX];
    EmbScanCand* topk =
        heap_cap <= TOPK_STACK_MAX ? stack_buf : malloc(heap_cap * sizeof(EmbScanCand));
    usize ksz = 0; /* 当前最大堆中的候选数量 */
    HeapStoreIter iter;
    heapStoreIter_begin(&iter, &et->heap_table.store);
    const TupleHdr* hdr;
    while ((hdr = heapStoreIter_next(&iter)) != NULL)
    {
        if (hdr->t_xmax != INVALID_TXN_ID) continue;
        if (!item_ptr_is_valid(hdr->t_emb_ctid)) continue;
        const f32* vec = embedding_store_get_ptr_ctid(&et->embed_store, hdr->t_emb_ctid);
        if (!vec) continue;
        VectorBase stored = {TYPE_FLOAT32, vc->query.count, (data_ptr_t)vec};
        f32 dist = vec_compute_distance(&stored, &vc->query, vc->metric);

        if (ksz == heap_cap && dist >= topk[0].dist) continue;

        Datum* vals = (heap_ncols > 0) ? malloc(heap_ncols * sizeof(Datum)) : NULL;
        if (heap_ncols > 0 && !vals) continue;
        u64 null_bits = 0;
        if (!vals) continue;

        if (heap_ncols > 0)
        {
            HeapTupleRef ref = {
                .hdr = hdr,
                .col_data = iter.curr_col_data,
                .schema = schema,
                .store = NULL,
            };
            if (heapStore_deform_tuple(&ref, vals, heap_ncols) != 0)
            {
                free(vals);
                continue;
            }
            null_bits = hdr->null_bits << 1;
        }

        EmbScanCand c = {hdr->t_ctid, dist, vals, vec, null_bits};
        if (ksz < heap_cap)
            topk_push(topk, &ksz, c);
        else
            topk_replace_root(topk, ksz, c);
    }
    heapStoreIter_end(&iter);

    qsort(topk, ksz, sizeof(EmbScanCand), emb_scan_cand_cmp);
    usize nout = 0;
    for (usize i = 0; i < ksz && nout < max_results; i++)
    {
        if (!topk[i].vals) continue;

        results[nout].heap_ctid = topk[i].heap_ctid;
        results[nout].distance = topk[i].dist;
        results[nout].payloads = topk[i].vals;
        results[nout].vector.data = (data_ptr_t)topk[i].vec;
        results[nout].vector.count = vc->query.count;
        results[nout].vector.type = TYPE_FLOAT32;
        topk[i].vals = NULL;
        nout++;
    }

    for (usize i = 0; i < ksz; i++)
    {
        if (topk[i].vals) free(topk[i].vals);
    }
    if (topk != stack_buf) free(topk);

    return (int)nout;
}

/* 把组合表写入块存储；当前为空实现。 */
static void embeddingHeapTable_write_blocks(TableAmRoutine* am, BlockManager* bm,
                                            MetaBlockWriter* w)
{
    (void)am;
    (void)bm;
    (void)w;
}

/* 从块存储恢复组合表；当前为空实现。 */
static void embeddingHeapTable_load_blocks(TableAmRoutine* am, BlockManager* bm, MetaBlockReader* r)
{
    (void)am;
    (void)bm;
    (void)r;
}

/* 销毁组合表的 vtable 入口；当前不额外释放资源。 */
static void embeddingHeapTable_destory(TableAmRoutine* am)
{
    (void)am;
}

/* 通过表的 append_chunk 接口把一个 DataChunk 写入 DataTable。 */
void dataTable_insert_datachunk(DataTable* datatable, DataChunk* chunk)
{
    ItemPtr heap_ctid_stack[TAM_CTX_STACK_MAX];
    ItemPtr emb_ctid_stack[TAM_CTX_STACK_MAX];

    TamInsertCtx ctx = {
        .emb_ctids = emb_ctid_stack,
        .heap_ctids = heap_ctid_stack,
        .count = chunk->count,
        .current_index = 0,
    };

    VCALL(datatable->table, append_chunk, chunk, &ctx);

    // 索引插入逻辑待实现
}

/* 按向量条件扫描 DataTable，并把结果写入调用方缓冲区。 */
int dataTable_scan(DataTable* datatable, const VectorCondition* vec_cond, TableQueryResult* results,
                   usize max_results)
{
    TamScanCtx ctx = {
        .vec_cond = (VectorCondition*)vec_cond,
        .k = max_results,
    };

    return VCALL(datatable->table, scan, &ctx, results, max_results);
}

/* 把 DataTable 结构体字段重置到默认空状态。 */
void DataTable_init(DataTable* datatable)
{
    datatable->schema_name = NULL;
    datatable->table_name = NULL;
    datatable->table = NULL;
    datatable->ncols = 0;
}

/* 释放 DataTable 自身持有的名称字符串并清空字段。 */
void DataTable_deinit(DataTable* datatable)
{
    if (!datatable) return;
    free(datatable->schema_name);
    free(datatable->table_name);
    datatable->schema_name = NULL;
    datatable->table_name = NULL;
    datatable->table = NULL;
}
