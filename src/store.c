#include <string.h>
#include <assert.h>

#include "store.h"
#include "table.h"
#include "segment.h"
#include "vb_type.h"
#include "vector.h"
/* ============================================================
 * embedding store
 * ============================================================ */

void EmbeddingStore_init(EmbeddingStore* store, i16 dimension)
{
    store->dimension = dimension;
    store->count = 0;
    store->elem_size = (usize)dimension * sizeof(f32);
    store->vecs_per_blk = BLOCK_SIZE / store->elem_size;

    SegmentTree_init(&store->tree);
    BlockSegment* first = BlockSegment_create2(0);
  /* Allocate a block for in-memory storage */
    first->block = Block_create(INVALID_BLOCK);
    segmentTree_append_segment(&store->tree, (SegmentBase*)first);

    Vector_init(&store->free_list, sizeof(ItemPtr), 0);
    store->last_ctid = INVALID_ITEM_PTR;
}

void EmbeddingStore_deinit(EmbeddingStore* store)
{
    if (!store) return;
    SegmentTree_deinit(&store->tree, BlockSegment_destroy);
    store->count = 0;
}

/**
 * Internal: append one embedding to the store.
 * Handles segment overflow by creating a new BlockSegment.
 */
static void embedding_store_append(EmbeddingStore* store, VectorBase* vec)
{
    BlockSegment* seg = (BlockSegment*)segmentTree_get_last_segment(&store->tree);
    usize start_pos = seg->byte_offset;
    usize remaining_space = BLOCK_SIZE - start_pos;

    if (remaining_space >= store->elem_size)
    {
        /* Fits in current segment */
        data_ptr_t data = segment_get_data(seg) + start_pos;
        memcpy(data, vec->data, store->elem_size);
        seg->base.count++;
        seg->byte_offset += store->elem_size;
    }
    else
    {
        /* Need a new segment */
        BlockSegment* new_seg = BlockSegment_create2(seg->base.start + seg->base.count);
        new_seg->block = Block_create(INVALID_BLOCK);
        segmentTree_append_segment(&store->tree, (SegmentBase*)new_seg);

        /* Write to new segment */
        data_ptr_t data = segment_get_data(new_seg);
        memcpy(data, vec->data, store->elem_size);
        new_seg->base.count++;
        new_seg->byte_offset = store->elem_size;
    }
    store->count++;
}

static inline ItemPtr emb_ctid_from_row_idx(const EmbeddingStore* store, usize row_idx)
{
    usize seg_idx = row_idx / store->vecs_per_blk;
    usize local_idx = row_idx % store->vecs_per_blk;
    return make_item_ptr((block_id_t)seg_idx, (u16)local_idx);
}

/**
 * Overwrite an existing embedding at emb_ctid in-place.
 */
static void embedding_store_write_at_ctid(EmbeddingStore* store, ItemPtr ctid, VectorBase* vec)
{
    usize seg_idx = (usize)item_ptr_block_id(ctid);
    usize local_idx = (usize)item_ptr_slot(ctid);
    SegmentNode* sn = (SegmentNode*)vector_get(store->tree.nodes, seg_idx);
    if (!sn) return;
    BlockSegment* seg = (BlockSegment*)sn->node;
    u8* dst = (u8*)segment_get_data(seg) + local_idx * store->elem_size;
    memcpy(dst, vec->data, store->elem_size);
}

ItemPtr embeddingStore_append_and_get_ctid(EmbeddingStore* store, VectorBase* vec)
{
    usize free_n = vector_size(&store->free_list);
    if (free_n > 0)
    {
        /* Pop last element from free_list */
        ItemPtr ctid;
        vector_pop_back(&store->free_list, &ctid);
        embedding_store_write_at_ctid(store, ctid, vec);
        store->last_ctid = ctid;
        return ctid;
    }
    else
    {
        usize row_idx = store->count;
        embedding_store_append(store, vec);
        store->last_ctid = emb_ctid_from_row_idx(store, row_idx);
        return store->last_ctid;
    }
}

/* ============================================================
 * heap store
 * ============================================================ */

static inline u16* hs_pd_lower(u8* page)
{
    return (u16*)page;
}

static inline u16* hs_pd_upper(u8* page)
{
    return (u16*)(page + 2);
}

static inline u16* hs_pd_flags(u8* page)
{
    return (u16*)(page + 4);
}

static inline TupleSlotId* hs_slots(u8* page)
{
    return (TupleSlotId*)(page + HS_BLOCK_HDR_SIZE);
}

static inline u16 hs_free_space(u8* page)
{
    return *hs_pd_upper(page) - *hs_pd_lower(page);
}

/** Initialise a fresh page (pd_lower=6, pd_upper=BLOCK_SIZE, pd_flags=0). */
static void rs_page_init(u8* page)
{
    *hs_pd_lower(page) = (u16)HS_BLOCK_HDR_SIZE;
    *hs_pd_upper(page) = (u16)BLOCK_SIZE;
    *hs_pd_flags(page) = 0;
}

u64 heapStore_slot_count(HeapStore* store)
{
    SegmentBase* last = segmentTree_get_last_segment(&store->tree);
    if (!last) return 0;
    return (u64)(last->start + last->count);
}

static u8* wirte_col(u8* dest, const TupleVal* col)
{
    switch (col->type)
    {
        case TUPLE_COL_BOOL:
            *dest = col->v.b ? 1 : 0;
            return dest + 1;
        case TUPLE_COL_I32:
            memcpy(dest, &col->v.i32, sizeof(i32));
            return dest + sizeof(i32);
        case TUPLE_COL_I64:
            memcpy(dest, &col->v.i64, sizeof(i64));
            return dest + sizeof(i64);
        case TUPLE_COL_F32:
            memcpy(dest, &col->v.f32, sizeof(f32));
            return dest + sizeof(f32);
        case TUPLE_COL_F64:
            memcpy(dest, &col->v.f64, sizeof(f64));
            return dest + sizeof(f64);
        case TUPLE_COL_TEXT:
            memcpy(dest, &col->v.text.len, sizeof(u32));
            memcpy(dest + sizeof(u32), col->v.text.ptr, col->v.text.len);
            return dest + sizeof(u32) + col->v.text.len;
        default:
            return dest; /* should not happen */
    }
}

/* ============================================================
 * Serialization helpers (unchanged from previous design)
 * ============================================================ */

static usize compute_col_size(const TupleVal* v)
{
    switch (v->type)
    {
        case TUPLE_COL_BOOL:
            return 1;
        case TUPLE_COL_I32:
            return 4;
        case TUPLE_COL_I64:
            return 8;
        case TUPLE_COL_F32:
            return 4;
        case TUPLE_COL_F64:
            return 8;
        case TUPLE_COL_TEXT:
            return sizeof(u32) + v->v.text.len;
        default:
            return 0;
    }
}

static usize compute_tuple_size(const HeapTuple* tuple)
{
    usize size = sizeof(TupleHdr) + tuple->ncols * sizeof(TupleVal);
    for (usize i = 0; i < tuple->ncols; i++)
    {
        size += compute_col_size(&tuple->cols[i]);
    }
    return size;
}

/** Serialize tuple directly into dest (must have compute_tuple_size bytes available). */
static void serialize_tuple_into(u8* dest, const HeapTuple* tuple)
{
    u8* p = dest;
    memcpy(p, &tuple->hdr, sizeof(TupleHdr));
    p += sizeof(TupleHdr);

    for (u16 i = 0; i < tuple->ncols; i++)
    {
        if (tuple->hdr.null_bits & (1u << i)) continue;
        p = write_col(p, &tuple->cols[i]);
    }
}

/* ============================================================
 * rs_page_compact — in-place page defragmentation (PG PageRepairFragmentation)
 *
 * Moves all live tuples to the top of the page (high byte addresses),
 * closing gaps left by LP_UNUSED (vacuumed) slots.
 * Updates each live slot's lp_off; resets pd_upper.
 * Dead slots (lp_len == 0) are left unchanged in the slot array.
 *
 * Move direction: always upward (src ≤ dest), so memmove is correct even
 * when adjacent blocks overlap.  O(page_size).
 * ============================================================ */
static void hs_page_compact(u8* page)
{
    u16 pd_lower = *hs_pd_lower(page);
    usize n_slots = (pd_lower - HS_BLOCK_HDR_SIZE) / HS_SLOT_SIZE;
    u16 new_upper = (u16)BLOCK_SIZE;

    for (usize i = 0; i < n_slots; i++)
    {
        TupleSlotId* sl = &hs_slots(page)[i];
        if (sl->lp_len == 0) continue;  /* LP_UNUSED: skip */

        u16 tup_len = sl->lp_len;
        new_upper -= tup_len;
        if (sl->lp_off != new_upper)
        {
            /* Move tuple upward (src ≤ dest always — see header comment). */
            memmove(page + new_upper, page + sl->lp_off, tup_len);
            sl->lp_off = new_upper;
        }
    }

    *hs_pd_upper(page) = new_upper;
}

static ItemPtr hs_reuse_slot(HeapStore* store, ItemPtr ctid, HeapTuple* tuple)
{
    BlockSegment* seg = rs_find_seg_by_block(store, item_ptr_block_id(ctid));
    if (!seg) return INVALID_ITEM_PTR;

    u16 slot_idx = item_ptr_slot(ctid);
    u8* page = (u8*)segment_get_data(seg);
    if ((usize)slot_idx >= seg->base.count) return INVALID_ITEM_PTR;

    TupleSlotId* sl = &hs_slots(page)[slot_idx];
    if (sl->lp_len != 0) return INVALID_ITEM_PTR;  /* not LP_UNUSED: safety check */

    usize ser_size = compute_tuple_size(tuple);
    assert(ser_size <= HS_MAX_TUPLE_SIZE);

    /* No new slot entry needed (the slot already exists in the array).
     * We only need ser_size bytes of tuple space from free space. */
    if ((usize)hs_free_space(page) < ser_size)
    {
        hs_page_compact(page);  /* reclaim dead bytes above pd_upper */
        if ((usize)hs_free_space(page) < ser_size)
            return INVALID_ITEM_PTR;  /* page is genuinely full even after compaction */
    }

    /* Stamp physical ctid: (block_id, slot_idx) — unchanged within the same page. */
    tuple->hdr.t_ctid = make_item_ptr(seg->block_id, slot_idx);

    /* Carve tuple space from pd_upper downward. */
    *hs_pd_upper(page) -= (u16)ser_size;
    u16 tup_off = *hs_pd_upper(page);

    /* Serialize directly into the page — zero-copy. */
    serialize_tuple_into(page + tup_off, tuple);

    /* Reactivate the slot entry. */
    sl->lp_off = tup_off;
    sl->lp_len = (u16)ser_size;

    return ctid;
}

/**
 * Reserve space for a tuple in the page and record its slot.
 * Returns a pointer directly into the page's tuple area for the caller to write into
 * (zero-copy: no intermediate buffer needed).
 * Caller must have already verified rs_free_space(page) >= RS_SLOT_SIZE + len.
 * out_slot_idx receives the assigned 0-based slot index.
 */
static u8* hs_page_reserve_slot(u8* page, u16 len, u16* out_slot_idx)
{
    u16 pd_upper = *hs_pd_upper(page);
    u16 tup_off = (u16)(pd_upper - len);

    u16 slot_idx = (u16)((*hs_pd_lower(page) - HS_BLOCK_HDR_SIZE) / HS_SLOT_SIZE);
    TupleSlotId* slot = hs_slots(page) + slot_idx;
    slot->lp_off = tup_off;
    slot->lp_len = len;

    *hs_pd_lower(page) = (u16)(*hs_pd_lower(page) + HS_SLOT_SIZE);
    *hs_pd_upper(page) = tup_off;

    if (out_slot_idx) *out_slot_idx = slot_idx;
    return page + tup_off;
}

static ItemPtr heapStore_append_tuple(HeapStore* store, HeapTuple* tuple)
{
    /* 1. Compute serialized size first (t_ctid not yet stamped, but TupleHdr
     *    size is fixed — ctid doesn't affect total tuple size). */
    usize ser_size = compute_tuple_size(tuple);
    assert(ser_size <= HS_MAX_TUPLE_SIZE && "tuple too large for one page");

    /* 2. Find current page; create a new one if it won't fit. */
    BlockSegment* seg = (BlockSegment*)segmentTree_get_last_segment(&store->tree);
    u8* page = (u8*)segment_get_data(seg);

    if (rs_free_space(page) < (u16)(HS_SLOT_SIZE + ser_size))
    {
        /* Compute start for new segment from last segment. */
        usize new_start = seg->base.start + seg->base.count;
        BlockSegment* ns = BlockSegment_create2(new_start);

        /* Assign real block_id at segment creation time (true physical ctid). */
        block_id_t bid;
        if (store->block_manager != NULL)
        {
            bid = VCALL(store->block_manager, get_free_block_id);
            ns->block_manager = store->block_manager;
        }
        else
        {
            bid = (block_id_t)store->page_count;  /* sequential local fallback */
        }
        ns->block_id = bid;
        ns->block = Block_create(bid);

        page = (u8*)segment_get_data(ns);
        rs_page_init(page);
        segmentTree_append_segment(&store->tree, (SegmentBase*)ns);
        seg = ns;
        store->page_count++;
    }

    /* 3. Compute slot index from current pd_lower (= number of existing slots). */
    u16 slot_idx = (u16)((*hs_pd_lower(page) - HS_BLOCK_HDR_SIZE) / HS_SLOT_SIZE);

    /* 4. Stamp ctid using the segment's real disk block_id. */
    block_id_t block_id = seg->block_id;
    tuple->hdr.t_ctid = make_item_ptr(block_id, slot_idx);

    /* 5. Reserve page slot and serialize directly into it (zero intermediate buffer). */
    u16 actual_slot;
    u8* dest = hs_page_reserve_slot(page, (u16)ser_size, &actual_slot);
    assert(actual_slot == slot_idx);  /* must match pre-computed slot */
    serialize_tuple_into(dest, tuple);

    seg->base.count++;
    return tuple->hdr.t_ctid;
}

//   地址 0
//   ┌──────────────────────────────────────────┐
//   │ pd_lower = 26  (6 + 5×4 = 5个slot)      │ offset=0
//   │ pd_upper = 7900                          │ offset=2
//   │ pd_flags = 0x0001 (PD_HAS_FREE_LINES)   │ offset=4
//   ├──────────────────────────────────────────┤ offset=6
//   │ slot[0]: lp_off=8100, lp_len=48  ✓alive │
//   │ slot[1]: lp_off=8052, lp_len=48  ✓alive │
//   │ slot[2]: lp_off=0,    lp_len=0   ★LP_UNUSED← 目标 │
//   │ slot[3]: lp_off=7956, lp_len=48  ✓alive │
//   │ slot[4]: lp_off=7900, lp_len=56  ✓alive │
//   ├──────────────────────────────────────────┤ offset=26 (=pd_lower)
//   │                                          │
//   │           空 闲 空 间                    │ ← rs_free_space = 7900-26 = 7874B
//   │                                          │
//   ├──────────────────────────────────────────┤ offset=7900 (=pd_upper)
//   │ tuple[4] data  56B                       │
//   │ tuple[3] data  48B                       │
//   │ (gap: tuple[2] 已被vacuum清零)           │ ← 死区（lp_off/lp_len=0,不占逻辑空间）
//   │ tuple[1] data  48B                       │
//   │ tuple[0] data  48B                       │
//   └──────────────────────────────────────────┘ offset=8192

//   ---
ItemPtr heapStore_insert(HeapStore* store, HeapTuple* tuple)
{
    TxnId txn_id = store->next_txn_id++;
    tuple->hdr.t_xmin = txn_id;
    tuple->hdr.t_xmax = INVALID_TXN_ID;
   // tuple->row_id = row_id;
    ItemPtr ctid = INVALID_ITEM_PTR;

    if (store->hint_free_seg != NULL)
    {
        BlockSegment* scan = store->hint_free_seg;
        while (scan != NULL)
        {
            data_ptr_t page = segment_get_data(scan);
            // Find a page with free space
            if (*hs_pd_flags(page) & PD_HAS_FREE_LINES)
            {
                usize n_slots = (*hs_pd_lower(page) - HS_BLOCK_HDR_SIZE) / HS_SLOT_SIZE;
                for (usize i = 0; i < n_slots; i++)
                {
                    if (hs_slots(page)[i].lp_len != 0) continue;  /* not LP_UNUSED */

                    ItemPtr free_ctid = make_item_ptr(scan->block_id, (u16)i);
                    ItemPtr ctid = hs_reuse_slot(store, free_ctid, tuple);
                    if (item_ptr_is_valid(ctid))
                    {
                        break;
                    }
                }

                /* No usable LP_UNUSED slot on this page — clear its hint bit. */
                *hs_pd_flags(page) &= (u16)~PD_HAS_FREE_LINES;
            }
            scan = (BlockSegment*)scan->base.next;
        }
        if (!item_ptr_is_valid(ctid))
            store->hint_free_seg = NULL;  /* no free pages remain; reset hint */
    }
    if (!item_ptr_is_valid(ctid))
    {
        ctid = heapStore_append_tuple(store, tuple);
    }
}

static BlockSegment* hs_find_seg_by_block(const HeapStore* store, block_id_t block_id)
{
    for (SegmentBase* s = segmentTree_get_root_segment(&store->tree); s != NULL; s = s->next)
    {
        BlockSegment* seg = (BlockSegment*)s;
        if (seg->block_id == block_id) return seg;
    }
    return NULL;
}

/** Return a pointer to the tuple at slot_idx within page. */
static inline u8* hs_page_get_tuple(u8* page, u16 slot_idx)
{
    return page + hs_slots(page)[slot_idx].lp_off;
}

static const u8* read_col_ptr(const u8* src, TupleVal* out)
{
    switch (out->type)
    {
        case TUPLE_COL_BOOL:
            out->v.b = *src != 0;
            return src + 1;
        case TUPLE_COL_I32:
            memcpy(&out->v.i32, src, sizeof(i32));
            return src + sizeof(i32);
        case TUPLE_COL_I64:
            memcpy(&out->v.i64, src, sizeof(i64));
            return src + sizeof(i64);
        case TUPLE_COL_F32:
            memcpy(&out->v.f32, src, sizeof(f32));
            return src + sizeof(f32);
        case TUPLE_COL_F64:
            memcpy(&out->v.f64, src, sizeof(f64));
            return src + sizeof(f64);
        case TUPLE_COL_TEXT:
        {
            u32 len;
            memcpy(&len, src, sizeof(u32));
            out->v.text.len = len;
            out->v.text.ptr = (char*)malloc(len + 1);
            if (!out->v.text.ptr) return NULL;
            out->v.text.ptr[len] = '\0';
            memcpy(out->v.text.ptr, src + sizeof(u32), len);
            return src + sizeof(u32) + len;
        }
        default:
            return NULL; /* should not happen */
    }
}

/**
 * Deserialize one tuple from a contiguous memory pointer.
 * out->cols is heap-allocated; call row_tuple_free(out, schema) when done.
 */
static bool deserialize_tuple_from_ptr(const u8* p, TableSchema* schema, HeapTuple* out)
{
    memcpy(&out->hdr, p, sizeof(TupleHdr));
    p += sizeof(TupleHdr);

    out->ncols = schema->ncols;
    out->cols = (TupleVal*)calloc(schema->ncols, sizeof(TupleVal));
    if (!out->cols) return false;

    for (u16 i = 0; i < schema->ncols; i++)
    {
        if (out->hdr.null_bits & (1u << i))
        {
            out->cols[i].type = TUPLE_COL_NULL;
            continue;
        }
        p = read_col_ptr(p, &out->cols[i]);
        if (!p)
        {
            for (u16 j = 0; j < i; j++)
            {
                TupleVal* v = &out->cols[j];
                if (v->type == TUPLE_COL_TEXT) free(v->v.text.ptr);
            }
            free(out->cols);
            out->cols = NULL;
            out->ncols = 0;
            return false;
        }
    }
    return true;
}

int heapStore_get_by_ctid(HeapStore* store, TableSchema* schema, ItemPtr ctid, HeapTuple* out)
{
    block_id_t block_id = item_ptr_block_id(ctid);
    u16 slot_idx = item_ptr_slot(ctid);
    BlockSegment* seg = hs_find_seg_by_block(store, block_id);
    if (!seg) return -1;

    u8* page = (u8*)segment_get_data(seg);
    if ((usize)slot_idx >= seg->base.count) return -1;

    TupleSlotId* slot = &hs_slots(page)[slot_idx];
    if (slot->lp_len == 0) return -1; /* LP_UNUSED */

    u8* tup_ptr = hs_page_get_tuple(page, slot_idx);

    memset(out, 0, sizeof(HeapTuple));
    out->ncols = schema->ncols;

    if (!deserialize_tuple_from_ptr(tup_ptr, schema, out)) return -1;

    // PostgreSQL 里 t_ctid 有两种状态：

    // 自指（self-pointing）：t_ctid == 自身物理地址
    //        → 要么是最新版本（alive），要么是已删除（xmax 被设置）

    // 前向（forward-pointing）：t_ctid → 另一个物理地址
    //        → UPDATE 的旧版本，指向新版本的位置
    //   ┌───────────────┬────────┬────────┬──────────────────────────────────┬────────────────────────┐
    //   │     状态      │ t_xmax │ t_ctid │               含义                │          处理 │
    //   ├───────────────┼────────┼────────┼──────────────────────────────────┼────────────────────────┤
    //   │ DELETE        │ ≠ 0    │ 自指   │ 已被删除，不可读                   │ 返回 -1 │
    //   ├───────────────┼────────┼────────┼──────────────────────────────────┼────────────────────────┤
    //   │ UPDATE 旧版本  │ ≠ 0    │ 前向   │ 指向新版本，允许 chain traversal  │
    //   正常返回，让调用方追链
    //   └───────────────┴────────┴────────┴──────────────────────────────────┴────────────────────────┘
    if (out->hdr.t_xmax != INVALID_TXN_ID)
    {
        if (item_ptr_block_id(out->hdr.t_ctid) == seg->block_id &&
            item_ptr_slot(out->hdr.t_ctid) == slot_idx)
        {
            /* Self-pointing + t_xmax set → deleted */
            row_tuple_free(out, schema);
            return -1;
        }
        /* Forward pointer → updated old version: return for chain traversal */
    }
    return 0;
}

/* Lookup tuple pointer from ctid. Returns NULL if slot is LP_UNUSED or ctid invalid. */
static u8* hs_lookup_ctid(HeapStore* store, ItemPtr ctid)
{
    block_id_t block_id = item_ptr_block_id(ctid);
    u16 slot_idx = item_ptr_slot(ctid);
    BlockSegment* seg = hs_find_seg_by_block(store, block_id);
    if (!seg) return NULL;
    if ((usize)slot_idx >= seg->base.count) return NULL;
    u8* page = (u8*)segment_get_data(seg);
    if (hs_slots(page)[slot_idx].lp_len == 0) return NULL;
    return hs_page_get_tuple(page, slot_idx);
}

TxnId heapstore_update_by_ctid(HeapStore* store, ItemPtr old_ctid, HeapTuple* new_tuple)
{
    u8* old_tup = hs_lookup_ctid(store, old_ctid);
    if (!old_tup) return INVALID_TXN_ID;

    /* Verify old version is alive */
    TupleHdr old_hdr;
    memcpy(&old_hdr, old_tup, sizeof(TupleHdr));
    if (old_hdr.t_xmax != INVALID_TXN_ID) return INVALID_TXN_ID;

    TxnId txn_id = store->next_txn_id++;

    /* Stamp new-version header (t_ctid set by rs_append_to_store). */
    new_tuple->hdr.t_xmin = txn_id;
    new_tuple->hdr.t_xmax = INVALID_TXN_ID;

    ItemPtr new_ctid = heapStore_append_tuple(store, new_tuple);
    /* new_tuple->hdr.t_ctid now holds the physical ctid of the new version. */
    ItemPtr new_ptr = new_tuple->hdr.t_ctid; /* (new_block_id, new_slot+1) */

    /* Re-lookup defensively: rs_append_to_store may realloc the SegmentTree
     * node array, but block->data buffers are stable (separate allocations).
     * The re-lookup is technically unnecessary but guards against future
     * layout changes that inline block data into the node array. */
    old_tup = hs_lookup_ctid(store, old_ctid);
    if (old_tup)
    {
        memcpy(&old_hdr, old_tup, sizeof(TupleHdr));
        old_hdr.t_xmax = txn_id;
        old_hdr.t_ctid = new_ptr; /* forward pointer to new physical location */
        memcpy(old_tup, &old_hdr, sizeof(TupleHdr));
    }

    (void)new_ctid; /* ctid is encoded in new_tuple->hdr.t_ctid for caller */
    return txn_id;
}

TxnId heapstore_delete_by_ctid(HeapStore* store, ItemPtr ctid)
{
    u8* tup = hs_lookup_ctid(store, ctid);
    if (!tup) return INVALID_TXN_ID;

    TupleHdr hdr;
    memcpy(&hdr, tup, sizeof(TupleHdr));
    if (hdr.t_xmax != INVALID_TXN_ID) return INVALID_TXN_ID; /* already dead */

    TxnId txn_id = store->next_txn_id++;

    /* Mutate TupleHdr in-place: t_xmax = deleter XID; t_ctid stays self-pointing. */
    hdr.t_xmax = txn_id;
    memcpy(tup, &hdr, sizeof(TupleHdr));

    return txn_id;
}
