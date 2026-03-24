#include <stdlib.h>
#include "segment.h"
#include "storage.h"
#include "vector.h"

void SegmentTree_init(SegmentTree* tree)
{
    tree->root_node = NULL;
    tree->nodes = NEW(Vector, sizeof(SegmentNode), 1);
}

void SegmentTree_deinit(SegmentTree* tree, void (*node_destroy)(SegmentBase*))
{
    if (!tree) return;
    if (node_destroy)
    {
        SegmentBase* cur = tree->root_node;
        while (cur)
        {
            SegmentBase* next = cur->next;
            node_destroy(cur);
            cur = next;
        }
    }
    if (tree->nodes)
    {
        vector_destroy(tree->nodes);
        tree->nodes = NULL;
    }
    tree->root_node = NULL;
}

BlockSegment* BlockSegment_create1(BlockManager* manager, block_id_t block_id, usize offset,
                                   usize count)
{
    BlockSegment* segment = malloc(sizeof(BlockSegment));
    segment->base.start = 0;
    segment->base.count = count;
    segment->base.next = NULL;
    segment->block_id = block_id;
    segment->byte_offset = offset;
    segment->block_manager = manager;
    segment->block = NULL; /* lazy loaded via segment_get_data */
    return segment;
}

BlockSegment* BlockSegment_create2(usize start)
{
    BlockSegment* segment = malloc(sizeof(BlockSegment));
    segment->base.start = start;
    segment->base.count = 0;
    segment->base.next = NULL;
    segment->block_id = INVALID_BLOCK;
    segment->byte_offset = 0;
    segment->block_manager = NULL;
    segment->block = NULL;
    return segment;
}

void BlockSegment_destroy(SegmentBase* segment)
{
    BlockSegment* seg = (BlockSegment*)segment;
    if (!seg) return;
    if (seg->block)
    {
        block_destroy(seg->block);
        seg->block = NULL;
    }
    free(seg);
}

RowSegment* RowSegment_create(DataTable* table, usize start)
{
    RowSegment* segment = malloc(sizeof(RowSegment));
    segment->base.start = start;
    segment->base.count = 0;
    segment->base.next = NULL;
    segment->table = table;
    return segment;
}

void RowSegment_destroy(RowSegment* segment)
{
    if (!segment) return;
    free(segment->columns);
    segment->columns = NULL;
    free(segment);
}

SegmentBase* segmentTree_get_root_segment(SegmentTree* tree)
{
    return tree->root_node;
}

SegmentBase* SegmentTree_get_segment(SegmentTree* tree, usize row_number)
{
    // return vector_get(tree->nodes, index)->node;
    usize lower = 0;
    usize upper = vector_size(tree->nodes) - 1;
        // binary search to find the node
    while (lower <= upper)
    {
        usize index = (lower + upper) / 2;
        SegmentNode* entry = vector_get(tree->nodes, index);
        if (row_number < entry->row_start)
        {
            upper = index - 1;
        }
        else if (row_number >= entry->row_start + entry->node->count)
        {
            lower = index + 1;
        }
        else
        {
            return entry->node;
        }
    }
    return NULL;
}

SegmentBase* segmentTree_get_last_segment(SegmentTree* tree)
{
    return ((SegmentNode*)vector_back(tree->nodes))->node;
}

data_ptr_t segment_get_data(BlockSegment* segment)
{
    if (segment->block == NULL)
    {
        segment->block = Block_create(segment->block_id);
        if (segment->block_id != INVALID_BLOCK)
        {
            VCALL(segment->block_manager, read, segment->block);
        }
    }
    return segment->block->fb->buffer;
}

void segmentTree_append_segment(SegmentTree* tree, SegmentBase* segment)
{
    SegmentNode node = {segment->start, segment};
    vector_push_back(tree->nodes, &node);
    if (vector_size(tree->nodes) > 1)
    {
        SegmentNode* last = vector_get(tree->nodes, vector_size(tree->nodes) - 2);
        last->node->next = segment;
    }
    else
    {
        tree->root_node = segment;
    }
}
