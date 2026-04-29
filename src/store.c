#include <string.h>
#include <stdlib.h>
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
    LWLockInit(&store->lock, "EmbeddingStore.lock");
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

const f32* embedding_store_get_ptr_ctid(EmbeddingStore* store, ItemPtr emb_ctid)
{
    LWLockAcquire(&store->lock, LW_SHARED);
    usize seg_idx = (usize)item_ptr_block_id(emb_ctid);
    usize local_idx = (usize)item_ptr_slot(emb_ctid);
    SegmentNode* sn = (SegmentNode*)vector_get(store->tree.nodes, seg_idx);
    const f32* ptr = NULL;
    if (sn)
    {
        BlockSegment* seg = (BlockSegment*)sn->node;
        ptr = (const f32*)((u8*)segment_get_data(seg) + local_idx * store->elem_size);
    }
    LWLockRelease(&store->lock);
    return ptr;
}

/* ============================================================
 * heap store
 * ============================================================ */

static inline u16* heapStore_pd_lower(u8* page)
{
    return (u16*)page;
}

static inline u16* heapStore_pd_upper(u8* page)
{
    return (u16*)(page + 2);
}

static inline u16* heapStore_pd_flags(u8* page)
{
    return (u16*)(page + 4);
}

static inline TupleSlotId* heapStore_slots(u8* page)
{
    return (TupleSlotId*)(page + HS_BLOCK_HDR_SIZE);
}

static inline u16 heapStore_free_space(u8* page)
{
    return *heapStore_pd_upper(page) - *heapStore_pd_lower(page);
}

/*
 * datum_deform_copy — read one column from page into Datum (copy path).
 *
 * Fixed-size types: by-value Datum, no malloc.
 * Varlena types (TEXT/BYTEA/JSONB): malloc'd copy — caller must free.
 *
 * Returns pointer past the column bytes on success, NULL on malloc failure.
 */
static const u8* datum_deform_copy(const u8* p, TupleColType type, Datum* out)
{
    switch (type)
    {
        case TUPLE_COL_BOOL:
            *out = BoolGetDatum(*p != 0);
            p += 1;
            break;
        case TUPLE_COL_I32:
        {
            i32 v;
            memcpy(&v, p, 4);
            *out = Int32GetDatum(v);
            p += 4;
            break;
        }
        case TUPLE_COL_I64:
        {
            i64 v;
            memcpy(&v, p, 8);
            *out = Int64GetDatum(v);
            p += 8;
            break;
        }
        case TUPLE_COL_F32:
        {
            f32 v;
            memcpy(&v, p, 4);
            *out = Float32GetDatum(v);
            p += 4;
            break;
        }
        case TUPLE_COL_F64:
        {
            f64 v;
            memcpy(&v, p, 8);
            *out = Float64GetDatum(v);
            p += 8;
            break;
        }
        default:
            *out = (Datum)0;
            break;
    }
    return p;
}

/*
 * rs_deform_all — deserialize all non-null columns from page into Datum[].
 *
 * Returns 0 on success, -1 on malloc failure (partial datums freed on error).
 */
static int heapStore_deform_all(const u8* col_data, const TableSchema* schema, u64 null_bits,
                                Datum* out)
{
    const u8* p = col_data;
    for (u16 i = 0; i < schema->ncols; i++)
    {
        if (null_bits & ((u64)1 << i))
        {
            out[i] = (Datum)0;
            continue;
        }
        p = datum_deform_copy(p, schema->cols[i], &out[i]);
        if (!p)
        {
            /* malloc failure: free already-allocated varlena Datums */
            u64 done = 0;
            for (u16 j = 0; j < i; j++)
            {
                if (null_bits & ((u64)1 << j)) continue;
                TupleColType ct = schema->cols[j];
                if (ct == TUPLE_COL_TEXT || ct == TUPLE_COL_JSONB)
                {
                    void* ptr = DatumGetPointer(out[j]);
                    if (ptr) free(ptr);
                }
                done |= ((u64)1 << j);
            }
            (void)done;
            return -1;
        }
    }
    return 0;
}

/** Initialise a fresh page (pd_lower=6, pd_upper=BLOCK_SIZE, pd_flags=0). */
static void heapStore_page_init(u8* page)
{
    *heapStore_pd_lower(page) = (u16)HS_BLOCK_HDR_SIZE;
    *heapStore_pd_upper(page) = (u16)BLOCK_SIZE;
    *heapStore_pd_flags(page) = 0;
}

void HeapStore_init(HeapStore* store, const TableSchema* schema, BlockManager* bm)
{
    store->block_manager = bm;
    store->schema = schema;
    store->hint_free_seg = NULL;
    LWLockInit(&store->lock, "HeapStore.content_lock");

    SegmentTree_init(&store->tree);

    BlockSegment* seg = BlockSegment_create2(0);
    block_id_t bid;
    if (bm != NULL)
    {
        bid = VCALL(bm, get_free_block_id);
        seg->block_manager = bm;
    }
    else
    {
        bid = 0;
    }
    seg->block_id = bid;
    seg->block = Block_create(bid);
    heapStore_page_init((u8*)segment_get_data(seg));
    segmentTree_append_segment(&store->tree, (SegmentBase*)seg);
}

void HeapStore_deinit(HeapStore* store)
{
    if (!store) return;
    SegmentTree_deinit(&store->tree, BlockSegment_destroy);
    store->page_count = 0;
}

/*
 * heap_deform_tuple — populate Datum[] from a HeapTupleRef.
 *
 * Fixed-size types: by-value Datum (inline, no malloc).
 * Varlena types:    malloc'd copy — caller owns; free with datum_array_free.
 * Null columns:     bit set in ref->hdr->null_bits — Datum = 0 (unused).
 *
 * Returns 0 on success, -1 on malloc failure.
 */
int heapStore_deform_tuple(const HeapTupleRef* ref, Datum* out, usize ncols)
{
    if (!ref->col_data || !out) return -1;
    usize use_ncols = ncols < (usize)ref->schema->ncols ? ncols : (usize)ref->schema->ncols;
    return heapStore_deform_all(ref->col_data, ref->schema, ref->hdr->null_bits, out);
    (void)use_ncols;
}

u64 heapStore_slot_count(HeapStore* store)
{
    SegmentBase* last = segmentTree_get_last_segment(&store->tree);
    if (!last) return 0;
    return (u64)(last->start + last->count);
}

static u8* wirte_col(u8* p, TupleColType type, Datum d)
{
    switch (type)
    {
        case TUPLE_COL_BOOL:
            *p = DatumGetBool(d) ? 1u : 0u;
            return p + 1;
        case TUPLE_COL_I32:
            i32 v1 = DatumGetInt32(d);
            memcpy(p, &v1, sizeof(i32));
            return p += sizeof(i32);
        case TUPLE_COL_I64:
            i64 v2 = DatumGetInt64(d);
            memcpy(p, &v2, sizeof(i64));
            return p + sizeof(i64);
        case TUPLE_COL_F32:
            f32 v3 = DatumGetFloat32(d);
            memcpy(p, &v3, sizeof(f32));
            return p + sizeof(f32);
        case TUPLE_COL_F64:
            f64 v4 = DatumGetFloat64(d);
            memcpy(p, &v4, sizeof(f64));
            return p + sizeof(f64);
        case TUPLE_COL_TEXT:
        {
            /* In-memory: [u32 content_len][data...] — same as on-disk. */
            u8* ptr = (u8*)DatumGetPointer(d);
            u32 len = 0;
            memcpy(&len, ptr, 4);
            memcpy(p, ptr, (usize)4 + len);
            p += (usize)4 + len;
            return p;
        }
        default:
            return p; /* should not happen */
    }
}

/* ============================================================
 * Serialization helpers (unchanged from previous design)
 * ============================================================ */

static usize compute_col_size(TupleColType type, Datum d)
{
    switch (type)
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
        {
            /* In-memory layout: [u32 content_len][data...] — disk format is identical. */
            u32 len = 0;
            memcpy(&len, DatumGetPointer(d), 4);
            return (usize)4 + len;
        }
        default:
            return 0;
    }
}

static usize compute_tuple_size(const TableSchema* schema, const Datum* vals)
{
    usize size = sizeof(TupleHdr);
    for (u16 i = 0; i < schema->ncols; i++)
    {
        // if (null_bits & ((u64)1 << i)) continue;
        if (!vals) continue;
        size += compute_col_size(schema->cols[i], vals[i]);
    }
    return size;
}

/** Serialize tuple directly into dest (must have compute_tuple_size bytes available). */
static void serialize_tuple_into(u8* dst, const TupleHdr* hdr, const TableSchema* schema,
                                 const Datum* vals)
{
    memcpy(dst, hdr, sizeof(TupleHdr));
    u8* p = dst + sizeof(TupleHdr);
    for (u16 i = 0; i < schema->ncols; i++)
    {
        if (hdr->null_bits & ((u64)1 << i)) continue;
        if (!vals) continue;
        p = wirte_col(p, schema->cols[i], vals[i]);
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
static void heapStore_page_compact(u8* page)
{
    u16 pd_lower = *heapStore_pd_lower(page);
    usize n_slots = (pd_lower - HS_BLOCK_HDR_SIZE) / HS_SLOT_SIZE;
    u16 new_upper = (u16)BLOCK_SIZE;

    for (usize i = 0; i < n_slots; i++)
    {
        TupleSlotId* sl = &heapStore_slots(page)[i];
        if (sl->lp_len == 0) continue; /* LP_UNUSED: skip */

        u16 tup_len = sl->lp_len;
        new_upper -= tup_len;
        if (sl->lp_off != new_upper)
        {
            /* Move tuple upward (src ≤ dest always — see header comment). */
            memmove(page + new_upper, page + sl->lp_off, tup_len);
            sl->lp_off = new_upper;
        }
    }

    *heapStore_pd_upper(page) = new_upper;
}

/* Forward declaration — defined later in this file. */
static BlockSegment* heapStore_find_seg_by_block(const HeapStore* store, block_id_t block_id);

static TxnId rs_alloc_xid(HeapStore* store)
{
    return ++store->next_txn_id;
}

static ItemPtr heapStore_reuse_slot(HeapStore* store, ItemPtr ctid, TupleHdr* hdr,
                                    const Datum* vals)
{
    BlockSegment* seg = heapStore_find_seg_by_block(store, item_ptr_block_id(ctid));
    if (!seg) return INVALID_ITEM_PTR;

    u16 slot_idx = item_ptr_slot(ctid);
    u8* page = (u8*)segment_get_data(seg);
    if ((usize)slot_idx >= seg->base.count) return INVALID_ITEM_PTR;

    TupleSlotId* sl = &heapStore_slots(page)[slot_idx];
    if (sl->lp_len != 0) return INVALID_ITEM_PTR; /* not LP_UNUSED: safety check */

    usize ser_size = compute_tuple_size(store->schema, vals);
    assert(ser_size <= HS_MAX_TUPLE_SIZE);

    /* No new slot entry needed (the slot already exists in the array).
     * We only need ser_size bytes of tuple space from free space. */
    if ((usize)heapStore_free_space(page) < ser_size)
    {
        heapStore_page_compact(page); /* reclaim dead bytes above pd_upper */
        if ((usize)heapStore_free_space(page) < ser_size)
            return INVALID_ITEM_PTR; /* page is genuinely full even after compaction */
    }

    /* Stamp physical ctid: (block_id, slot_idx) — unchanged within the same page. */
    hdr->t_ctid = make_item_ptr(seg->block_id, slot_idx);

    /* Carve tuple space from pd_upper downward. */
    *heapStore_pd_upper(page) -= (u16)ser_size;
    u16 tup_off = *heapStore_pd_upper(page);

    /* Serialize directly into the page — zero-copy. */
    serialize_tuple_into(page + tup_off, hdr, store->schema, vals);

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
static u8* heapStore_page_reserve_slot(u8* page, u16 len, u16* out_slot_idx)
{
    u16 pd_upper = *heapStore_pd_upper(page);
    u16 tup_off = (u16)(pd_upper - len);

    u16 slot_idx = (u16)((*heapStore_pd_lower(page) - HS_BLOCK_HDR_SIZE) / HS_SLOT_SIZE);
    TupleSlotId* slot = heapStore_slots(page) + slot_idx;
    slot->lp_off = tup_off;
    slot->lp_len = len;

    *heapStore_pd_lower(page) = (u16)(*heapStore_pd_lower(page) + HS_SLOT_SIZE);
    *heapStore_pd_upper(page) = tup_off;

    if (out_slot_idx) *out_slot_idx = slot_idx;
    return page + tup_off;
}

static ItemPtr heapStore_append_tuple(HeapStore* store, TupleHdr* hdr, const Datum* vals)
{
    /* 1. Compute serialized size first (t_ctid not yet stamped, but TupleHdr
     *    size is fixed — ctid doesn't affect total tuple size). */
    usize ser_size = compute_tuple_size(store->schema, vals);
    assert(ser_size <= HS_MAX_TUPLE_SIZE && "tuple too large for one page");

    /* 2. Find current page; create a new one if it won't fit. */
    BlockSegment* seg = (BlockSegment*)segmentTree_get_last_segment(&store->tree);
    u8* page = (u8*)segment_get_data(seg);

    if (heapStore_free_space(page) < (u16)(HS_SLOT_SIZE + ser_size))
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
            bid = (block_id_t)store->page_count; /* sequential local fallback */
        }
        ns->block_id = bid;
        ns->block = Block_create(bid);

        page = (u8*)segment_get_data(ns);
        heapStore_page_init(page);
        segmentTree_append_segment(&store->tree, (SegmentBase*)ns);
        seg = ns;
        store->page_count++;
    }

    /* 3. Compute slot index from current pd_lower (= number of existing slots). */
    u16 slot_idx = (u16)((*heapStore_pd_lower(page) - HS_BLOCK_HDR_SIZE) / HS_SLOT_SIZE);

    /* 4. Stamp ctid using the segment's real disk block_id. */
    block_id_t block_id = seg->block_id;
    hdr->t_ctid = make_item_ptr(block_id, slot_idx);

    /* 5. Reserve page slot and serialize directly into it (zero intermediate buffer). */
    u16 actual_slot;
    u8* dest = heapStore_page_reserve_slot(page, (u16)ser_size, &actual_slot);
    assert(actual_slot == slot_idx); /* must match pre-computed slot */
    serialize_tuple_into(dest, hdr, store->schema, vals);

    seg->base.count++;
    return hdr->t_ctid;
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
ItemPtr heapStore_insert(HeapStore* store, TxnId xid, ItemPtr emb_ctid, const Datum* values,
                         u64 null_bits)
{
    TupleHdr hdr;
    memset(&hdr, 0, sizeof(hdr));
    hdr.t_xmin = xid ? xid : rs_alloc_xid(store);
    hdr.t_xmax = INVALID_TXN_ID;
    hdr.t_emb_ctid = emb_ctid;
    hdr.null_bits = null_bits;
    // tuple->row_id = row_id;
    ItemPtr ctid = INVALID_ITEM_PTR;

    if (store->hint_free_seg != NULL)
    {
        BlockSegment* scan = store->hint_free_seg;
        while (scan != NULL)
        {
            data_ptr_t page = segment_get_data(scan);
            // Find a page with free space
            if (*heapStore_pd_flags(page) & PD_HAS_FREE_LINES)
            {
                usize n_slots = (*heapStore_pd_lower(page) - HS_BLOCK_HDR_SIZE) / HS_SLOT_SIZE;
                for (usize i = 0; i < n_slots; i++)
                {
                    if (heapStore_slots(page)[i].lp_len != 0) continue; /* not LP_UNUSED */

                    ItemPtr free_ctid = make_item_ptr(scan->block_id, (u16)i);
                    ItemPtr ctid = heapStore_reuse_slot(store, free_ctid, &hdr, values);
                    if (item_ptr_is_valid(ctid))
                    {
                        break;
                    }
                }

                /* No usable LP_UNUSED slot on this page — clear its hint bit. */
                *heapStore_pd_flags(page) &= (u16)~PD_HAS_FREE_LINES;
            }
            scan = (BlockSegment*)scan->base.next;
        }
        if (!item_ptr_is_valid(ctid))
            store->hint_free_seg = NULL; /* no free pages remain; reset hint */
    }
    if (!item_ptr_is_valid(ctid))
    {
        ctid = heapStore_append_tuple(store, &hdr, values);
    }
    return ctid;
}

static BlockSegment* heapStore_find_seg_by_block(const HeapStore* store, block_id_t block_id)
{
    for (SegmentBase* s = segmentTree_get_root_segment(&store->tree); s != NULL; s = s->next)
    {
        BlockSegment* seg = (BlockSegment*)s;
        if (seg->block_id == block_id) return seg;
    }
    return NULL;
}

/** Return a pointer to the tuple at slot_idx within page. */
static inline u8* heapStore_page_get_tuple(u8* page, u16 slot_idx)
{
    return page + heapStore_slots(page)[slot_idx].lp_off;
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
 * out->cols is heap-allocated (Datum[]); call row_tuple_free(out, schema) when done.
 */
static bool deserialize_tuple_from_ptr(const u8* p, const TableSchema* schema, HeapTuple* out)
{
    memcpy(&out->hdr, p, sizeof(TupleHdr));
    p += sizeof(TupleHdr);

    out->ncols = schema->ncols;
    if (schema->ncols == 0)
    {
        out->cols = NULL;
        return true;
    }
    out->cols = (Datum*)calloc(schema->ncols, sizeof(Datum));
    if (!out->cols) return false;

    if (heapStore_deform_all(p, schema, out->hdr.null_bits, out->cols) != 0)
    {
        free(out->cols);
        out->cols = NULL;
        return false;
    }
    return true;
}

/**
 * Free heap-allocated column data in a HeapTuple (varlena Datums + the cols array itself).
 */
static void row_tuple_free(HeapTuple* out, const TableSchema* schema)
{
    if (!out->cols) return;
    for (u16 i = 0; i < out->ncols && i < schema->ncols; i++)
    {
        if (out->hdr.null_bits & ((u64)1 << i)) continue;
        TupleColType ct = schema->cols[i];
        if (ct == TUPLE_COL_TEXT || ct == TUPLE_COL_JSONB)
        {
            void* ptr = DatumGetPointer(out->cols[i]);
            if (ptr) free(ptr);
        }
    }
    free(out->cols);
    out->cols = NULL;
}

int heapStore_get_by_ctid(HeapStore* store, const TableSchema* schema, ItemPtr ctid, HeapTuple* out)
{
    block_id_t block_id = item_ptr_block_id(ctid);
    u16 slot_idx = item_ptr_slot(ctid);
    BlockSegment* seg = heapStore_find_seg_by_block(store, block_id);
    if (!seg) return -1;

    u8* page = (u8*)segment_get_data(seg);
    if ((usize)slot_idx >= seg->base.count) return -1;

    TupleSlotId* slot = &heapStore_slots(page)[slot_idx];
    if (slot->lp_len == 0) return -1; /* LP_UNUSED */

    u8* tup_ptr = heapStore_page_get_tuple(page, slot_idx);

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
static u8* heapStore_lookup_ctid(HeapStore* store, ItemPtr ctid)
{
    block_id_t block_id = item_ptr_block_id(ctid);
    u16 slot_idx = item_ptr_slot(ctid);
    BlockSegment* seg = heapStore_find_seg_by_block(store, block_id);
    if (!seg) return NULL;
    if ((usize)slot_idx >= seg->base.count) return NULL;
    u8* page = (u8*)segment_get_data(seg);
    if (heapStore_slots(page)[slot_idx].lp_len == 0) return NULL;
    return heapStore_page_get_tuple(page, slot_idx);
}

// TxnId heapStore_update_by_ctid(HeapStore* store, ItemPtr old_ctid, HeapTuple* new_tuple)
// {
//     u8* old_tup = heapStore_lookup_ctid(store, old_ctid);
//     if (!old_tup) return INVALID_TXN_ID;

//     /* Verify old version is alive */
//     TupleHdr old_hdr;
//     memcpy(&old_hdr, old_tup, sizeof(TupleHdr));
//     if (old_hdr.t_xmax != INVALID_TXN_ID) return INVALID_TXN_ID;

//     TxnId txn_id = store->next_txn_id++;

//     /* Stamp new-version header (t_ctid set by rs_append_to_store). */
//     new_tuple->hdr.t_xmin = txn_id;
//     new_tuple->hdr.t_xmax = INVALID_TXN_ID;

//     ItemPtr new_ctid = heapStore_append_tuple(store, new_tuple);
//     /* new_tuple->hdr.t_ctid now holds the physical ctid of the new version. */
//     ItemPtr new_ptr = new_tuple->hdr.t_ctid; /* (new_block_id, new_slot+1) */

//     /* Re-lookup defensively: rs_append_to_store may realloc the SegmentTree
//      * node array, but block->data buffers are stable (separate allocations).
//      * The re-lookup is technically unnecessary but guards against future
//      * layout changes that inline block data into the node array. */
//     old_tup = heapStore_lookup_ctid(store, old_ctid);
//     if (old_tup)
//     {
//         memcpy(&old_hdr, old_tup, sizeof(TupleHdr));
//         old_hdr.t_xmax = txn_id;
//         old_hdr.t_ctid = new_ptr; /* forward pointer to new physical location */
//         memcpy(old_tup, &old_hdr, sizeof(TupleHdr));
//     }

//     (void)new_ctid; /* ctid is encoded in new_tuple->hdr.t_ctid for caller */
//     return txn_id;
// }

TxnId heapStore_delete_by_ctid(HeapStore* store, ItemPtr ctid)
{
    u8* tup = heapStore_lookup_ctid(store, ctid);
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

void heapStoreIter_begin(HeapStoreIter* iter, HeapStore* store)
{
    LWLockAcquire(&store->lock, LW_SHARED);
    iter->store = store;
    iter->curr_seg = segmentTree_get_root_segment(&store->tree);
    iter->slot_idx = 0;
}

const TupleHdr* heapStoreIter_next(HeapStoreIter* iter)
{
    while (iter->curr_seg != NULL)
    {
        if ((usize)iter->slot_idx >= iter->curr_seg->count)
        {
            iter->curr_seg = iter->curr_seg->next;
            iter->slot_idx = 0;
            continue;
        }

        BlockSegment* bseg = (BlockSegment*)iter->curr_seg;
        u8* page = (u8*)segment_get_data(bseg);
        u32 slot = iter->slot_idx++;

        TupleSlotId* sl = &heapStore_slots(page)[slot];
        if (sl->lp_len == 0) continue; /* LP_UNUSED */
        const TupleHdr* hdr = (const TupleHdr*)heapStore_page_get_tuple(page, (u16)slot);
        /* Expose tuple length and col_data pointer (PG lp_len style).
         * col_data points directly into the page buffer — valid while LW_SHARED held. */
        iter->curr_tup_len = sl->lp_len;
        iter->curr_col_data = (const u8*)hdr + sizeof(TupleHdr);
        return hdr;
    }
    return NULL;
}

void heapStoreIter_end(HeapStoreIter* iter)
{
    if (iter->store)
    {
        LWLockRelease(&iter->store->lock);
        iter->store = NULL;
    }
}
