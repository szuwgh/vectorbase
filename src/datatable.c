#include "datatable.h"
#include "segment.h"
#include "vector.h"

static void copy_to_storage() {}

static void datatable_append_row_segment(DataTable* table, usize column_index, SegmentBase* segment)
{
    segmentTree_append_segment(&table->column_storage_tree[column_index], segment);
}

DataTable* Datatable_create(StorageManager* manager, char* schema_name, char* table_name,
                            usize column_count, TypeID* column_types)
{
    Vector columns = VEC(SegmentTree, column_count);
    for (usize i = 0; i < column_count; i++)
    {
        SegmentTree* tree = vector_get(&columns, i);
        SegmentTree_init(tree);
        segmentTree_append_segment(tree, (SegmentBase*)ColumnSegment_create2(0));
    }
    return NULL;
   // return table;
}

usize dataChunk_size(DataChunk* chunk)
{
    if (chunk->column_count == 0) return 0;
    return chunk->columns[0].count;
}

static void datatable_append_vector(DataTable* table, usize column_index, ColumnVector* vector)
{
    ColumnSegment* seg =
        (ColumnSegment*)segmentTree_get_last_segment(&table->column_storage_tree[column_index]);

    usize type_size = get_typeid_size(table->column_types[column_index]);
    usize start_pos = seg->offset;
}

void datatable_append(DataTable* table, DataChunk* chunk)
{
    usize size = dataChunk_size(chunk);
    if (size == 0) return;

    usize remainder = size;
    usize offset = 0;
    RowSegment* last_seg =
        (RowSegment*)((SegmentNode*)vector_back(table->row_storage_tree.nodes))->node;
    while (remainder > 0)
    {
        usize to_copy = MIN(STORAGE_CHUNK_SIZE - GET_PARENT_FIELD(last_seg, count), remainder);
        if (to_copy == 0) break;
    }
}