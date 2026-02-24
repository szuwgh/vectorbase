#include <stdlib.h>
#include "segment.h"
#include "storage.h"
#include "vector.h"

void SegmentTree_init(SegmentTree* tree)
{
    tree->root_node = NULL;
    tree->nodes = NEW(Vector, sizeof(SegmentNode), 1);
}

ColumnSegment* ColumnSegment_create2(usize start)
{
    ColumnSegment* segment = malloc(sizeof(ColumnSegment));
    segment->base.start = start;
    segment->base.count = 0;
    segment->block_id = INVALID_BLOCK;
    segment->offset = 0;
    segment->block_manager = NULL;
    segment->block = NULL;
    return segment;
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

data_ptr_t segment_get_data(ColumnSegment* segment)
{
    if (segment->block == NULL)
    {
       // segment->block =
    }
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
