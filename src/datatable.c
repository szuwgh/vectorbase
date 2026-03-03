#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include "datatable.h"
#include "segment.h"
#include "vb_type.h"
#include "vector.h"
#include "operator.h"

static RowSegment* datatable_append_row_segment(DataTable* table, usize start)
{
    RowSegment* seg = RowSegment_create(table, start);
    seg->columns_count = table->column_count;

    seg->columns = calloc(seg->columns_count, sizeof(ColumnPointer));
    for (usize i = 0; i < table->column_count; i++)
    {
        seg->columns[i].segment =
            (ColumnSegment*)((SegmentNode*)vector_back(table->column_storage_tree[i].nodes))->node;
        seg->columns[i].bytes_offset = seg->columns[i].segment->base.count;
    }
    segmentTree_append_segment(&table->row_storage_tree, (SegmentBase*)seg);
    return seg;
}

DataTable* Datatable_create(StorageManager* manager, char* schema_name, char* table_name,
                            usize column_count, TypeID* column_types)
{
    DataTable* table = malloc(sizeof(DataTable));
    table->manager = manager;
    table->schema_name = schema_name;
    table->table_name = table_name;
    table->column_count = column_count;
    table->column_types = column_types;

    table->column_storage_tree = calloc(column_count, sizeof(SegmentTree));
    for (usize i = 0; i < column_count; i++)
    {
        SegmentTree_init(&table->column_storage_tree[i]);
        segmentTree_append_segment(&table->column_storage_tree[i],
                                   (SegmentBase*)ColumnSegment_create2(0));
    }

    SegmentTree_init(&table->row_storage_tree);
    datatable_append_row_segment(table, 0);

    return table;
}

static void column_segment_destroy_cb(SegmentBase* base)
{
    ColumnSegment_destroy((ColumnSegment*)base);
}

static void row_segment_destroy_cb(SegmentBase* base)
{
    RowSegment_destroy((RowSegment*)base);
}

void Datatable_destroy(DataTable* table)
{
    if (!table) return;

    /* free column storage trees (ColumnSegments + their Blocks) */
    for (usize i = 0; i < table->column_count; i++)
    {
        SegmentTree_deinit(&table->column_storage_tree[i], column_segment_destroy_cb);
    }
    free(table->column_storage_tree);

    /* free row storage tree (RowSegments + their columns arrays) */
    SegmentTree_deinit(&table->row_storage_tree, row_segment_destroy_cb);

    free(table);
}

void ColumnVector_init(ColumnVector* vector, TypeID type)
{
    vector->type = type;
    vector->count = 0;
    vector->data = NULL;
}

void DataChunk_init(DataChunk* chunk, Vector types)
{
    chunk->column_count = types.size;
    chunk->columns = calloc(chunk->column_count, sizeof(ColumnVector));
    for (usize i = 0; i < chunk->column_count; i++)
    {
        ColumnVector_init(&chunk->columns[i], VECTOR_AT(&types, i, TypeID));
    }

    usize size = 0;
    VECTOR_FOREACH(&types, type)
    {
        size += get_typeid_size(*(TypeID*)type) * STANDARD_VECTOR_SIZE;
    }
    assert(size > 0);
    // 连续内存
    data_ptr_t own_data = malloc(size);
    chunk->data = own_data;
    chunk->size = size;
    memset(own_data, 0, size);
    for (usize i = 0; i < chunk->column_count; i++)
    {
        chunk->columns[i].data = own_data;
        own_data += get_typeid_size(VECTOR_AT(&types, i, TypeID)) * STANDARD_VECTOR_SIZE;
    }
}

void dataChunk_deinit(DataChunk* chunk)
{
    if (chunk->data)
    {
        // DataChunk_init 分配了一块连续内存给所有列共享，
        // 只需 free 一次基地址，不能对每列分别 free（否则 double-free）
        free(chunk->data);
        chunk->data = NULL;
    }
    else
    {
        // 各列独立分配的模式（非 DataChunk_init 创建）
        for (usize i = 0; i < chunk->column_count; i++) columnVector_deinit(&chunk->columns[i]);
    }
    free(chunk->columns);
    chunk->columns = NULL;
    chunk->column_count = 0;
}

void dataChunk_clear(DataChunk* chunk)
{
    for (usize i = 0; i < chunk->column_count; i++)
    {
        chunk->columns[i].count = 0;
    }
}

void dataChunk_reset(DataChunk* chunk)
{
    data_ptr_t ptr = chunk->data;
    for (usize i = 0; i < chunk->column_count; i++)
    {
        chunk->columns[i].count = 0;
        chunk->columns[i].data = ptr;
        ptr += get_typeid_size(chunk->columns[i].type) * STANDARD_VECTOR_SIZE;
    }
}

void dataChunk_append(DataChunk* chunk, usize index, ColumnVector src)
{
    assert(index < chunk->column_count);
    chunk->columns[index] = src;
}

usize dataChunk_size(DataChunk* chunk)
{
    if (chunk->column_count == 0) return 0;
    return chunk->columns[0].count;
}

static void datatable_append_vector(DataTable* table, ColumnVector* vector, usize column_index,
                                    usize offset, usize count)
{
    ColumnSegment* seg =
        (ColumnSegment*)segmentTree_get_last_segment(&table->column_storage_tree[column_index]);

    usize type_size = get_typeid_size(table->column_types[column_index]);
    usize start_pos = seg->byte_offset;
    data_ptr_t data = segment_get_data(seg) + start_pos;
    usize elements_to_copy = MIN((BLOCK_SIZE - start_pos) / type_size, count);
    if (elements_to_copy > 0)
    {
        copy_to_storage(vector, data, offset, elements_to_copy);
        offset += elements_to_copy;
        seg->base.count += elements_to_copy;
        seg->byte_offset += elements_to_copy * type_size;
    }
    if (elements_to_copy < count)
    {
        ColumnSegment* new_seg = ColumnSegment_create2(seg->base.start + seg->base.count);
        segmentTree_append_segment(&table->column_storage_tree[column_index],
                                   (SegmentBase*)new_seg);
        datatable_append_vector(table, vector, column_index, offset, count - elements_to_copy);
    }
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
        if (to_copy > 0)
        {
            for (usize i = 0; i < table->column_count; i++)
            {
                datatable_append_vector(table, &chunk->columns[i], i, offset, to_copy);
            }
            last_seg->base.count += to_copy;
            offset += to_copy;
            remainder -= to_copy;
        }
        if (remainder > 0)
        {
            last_seg =
                datatable_append_row_segment(table, last_seg->base.start + last_seg->base.count);
        }
    }
}

/* ===========================================================================
 *  Scan Implementation
 *  Modeled after DuckDB DataTable::Scan() / RetrieveColumnData()
 *  - No transaction / MVCC — all rows are visible
 *  - Each call returns at most one RowSegment's worth of rows
 * =========================================================================== */

/* retrieve_column_data --------------------------------------------------
 *  Reads `count` elements from the column segment chain starting at
 *  `pointer`, writing them into `result->data`.  Advances `pointer`
 *  past the data that was read (handles cross-segment boundaries).
 *
 *  Corresponds to DuckDB src/storage/data_table.cpp RetrieveColumnData()
 * ---------------------------------------------------------------------- */
static void retrieve_column_data(ColumnVector* result, TypeID type, ColumnPointer* pointer,
                                 usize count)
{
    usize type_size = get_typeid_size(type);
    usize written = 0;

    while (count > 0)
    {
        usize avail = (BLOCK_SIZE - pointer->bytes_offset) / type_size;
        usize to_copy = MIN(count, avail);

        if (to_copy > 0)
        {
            data_ptr_t src = segment_get_data(pointer->segment) + pointer->bytes_offset;
            memcpy(result->data + written * type_size, src, to_copy * type_size);
            pointer->bytes_offset += to_copy * type_size;
            written += to_copy;
            count -= to_copy;
        }

        if (count > 0)
        {
            assert(pointer->segment->base.next);
            pointer->segment = (ColumnSegment*)pointer->segment->base.next;
            pointer->bytes_offset = 0;
        }
    }

    result->count = written;
}

/* datatable_init_scan ---------------------------------------------------
 *  Initializes a ScanState for a full sequential scan of `table`.
 *  The caller must use the same `column_ids` across all subsequent
 *  datatable_scan() calls for this state.
 *
 *  Corresponds to DuckDB DataTable::InitializeScan()
 * ---------------------------------------------------------------------- */
void datatable_init_scan(DataTable* table, ScanState* state)
{
    state->current_chunk = (RowSegment*)segmentTree_get_root_segment(&table->row_storage_tree);
    state->last_chunk = (RowSegment*)segmentTree_get_last_segment(&table->row_storage_tree);
    state->last_chunk_count = state->last_chunk->base.count;
    state->offset = 0;

    state->columns = malloc(sizeof(ColumnPointer) * table->column_count);
    for (usize i = 0; i < table->column_count; i++)
    {
        ColumnPointer* src_col = &state->current_chunk->columns[i];
        state->columns[i].segment = src_col->segment;
        /* RowSegment stores element offset in bytes_offset;
         * convert to actual byte offset for scan reads. */
        state->columns[i].bytes_offset = 0;
    }
}

void scanstate_deinit(ScanState* state)
{
    if (!state) return;
    free(state->columns);
    state->columns = NULL;
    state->current_chunk = NULL;
    state->last_chunk = NULL;
}

/* datatable_scan --------------------------------------------------------
 *  Reads the next batch of rows into `output`.  Returns true if data
 *  was produced, false when the scan is exhausted.
 *
 *  `column_ids` / `col_count` specify which table columns to project.
 *  `output` must have `col_count` pre-allocated ColumnVectors whose
 *  `data` buffers are large enough for STORAGE_CHUNK_SIZE elements.
 *
 *  Corresponds to DuckDB DataTable::Scan() (simplified, no MVCC).
 * ---------------------------------------------------------------------- */
bool datatable_scan(DataTable* table, ScanState* state, DataChunk* output, usize* column_ids,
                    usize col_count)
{
    while (state->current_chunk)
    {
        RowSegment* current = state->current_chunk;

        usize end = (current == state->last_chunk) ? state->last_chunk_count : current->base.count;

        usize scan_count = MIN(end - state->offset, STANDARD_VECTOR_SIZE);

        if (scan_count > 0)
        {
            for (usize i = 0; i < col_count; i++)
            {
                usize col_id = column_ids[i];
                output->columns[i].type = table->column_types[col_id];
                retrieve_column_data(&output->columns[i], table->column_types[col_id],
                                     &state->columns[col_id], scan_count);
            }

            state->offset += scan_count;

            if (state->offset >= end)
            {
                if (current == state->last_chunk)
                    state->current_chunk = NULL;
                else
                {
                    state->current_chunk = (RowSegment*)current->base.next;
                    state->offset = 0;
                }
            }

            return true;
        }

        /* Empty chunk — skip to next */
        if (current == state->last_chunk)
        {
            state->current_chunk = NULL;
        }
        else
        {
            state->current_chunk = (RowSegment*)current->base.next;
            state->offset = 0;
        }
    }

    return false;
}
