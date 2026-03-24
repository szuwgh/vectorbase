#include <string.h>
#include "store.h"
#include "table.h"
#include "segment.h"
#include "vector.h"

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

void heapStore_append(HeapStore* store, u64 row_id, HeapTuple* tuple)
{
    TxnId txn_id = store->next_txn_id++;
    tuple->hdr.t_xmin = txn_id;
    tuple->hdr.t_xmax = INVALID_TXN_ID;
    tuple->row_id = row_id;
}
