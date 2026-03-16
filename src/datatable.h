#ifndef DATATABLE_H
#define DATATABLE_H

#include "store.h"
#include "vb_type.h"
#include "segment.h"
#include "vector.h"
#include "types.h"

#define STANDARD_VECTOR_SIZE  50
#define STORAGE_CHUNK_VECTORS 5
#define STORAGE_CHUNK_SIZE    (STANDARD_VECTOR_SIZE * STORAGE_CHUNK_VECTORS)

typedef struct
{
} HeapTupleData;

typedef enum
{
    CHUNK_COLUMN = 0, // 列式
    CHUNK_EMBED = 1, // 嵌入式
} ChunkMode;

/**
 * DataChunk — analytical column-store batch (COLUMN mode).
 *
 *   count    = number of columns (ncols)
 *   arrays   = arrays[i] holds all values for column i (TypeID-typed)
 *
 * For the EMBED (embedding + payload) mode, see VbChunk in tmp/src/table_am.h.
 */
typedef struct DataChunk
{
    ChunkMode mode;
    usize count;  /* number of columns (ncols)                         */
    VectorBase* arrays; /* arrays[i] = typed array of all row values for col i */
    data_ptr_t data;   /* raw storage buffer (owned, optional)              */
    usize size;   /* bytes allocated in data                           */
    usize n_payloads; /*有多少payload 例如一个向量可能有多个payload 每个payload是一个RowVal
                     有可能是JsonB, 也可能是int ，但是Vbchunk 里面 每向量有多少个 payload
                     是相同的*/
    const TupleVal** payloads; /* per-row payload (EMBED only), may be NULL        */
} DataChunk;

void DataChunk_init(DataChunk* chunk, Vector types);

void dataChunk_deinit(DataChunk* chunk);

void dataChunk_clear(DataChunk* chunk);

void dataChunk_reset(DataChunk* chunk);

void dataChunk_append(DataChunk* chunk, usize index, VectorBase src);

usize dataChunk_size(DataChunk* chunk);
// StorageChunk 和 ColumnSegment 的分段边界不一定对齐。一个 INT32 列的 ColumnSegment（256KB）能装
// 262144/4 = 65536 行，而一个 StorageChunk 只管 10240
// 行。所以一个 ColumnSegment 可能跨多个 StorageChunk。
struct DataTable
{
    char* schema_name;
    char* table_name;
    StorageManager* manager;
    SegmentTree row_storage_tree;
    SegmentTree* column_storage_tree; // 列式存储
    TypeID* column_types;
    usize column_count;
};

DataTable* Datatable_create(StorageManager* manager, char* schema_name, char* table_name,
                            usize column_count, TypeID* column_types);

void Datatable_destroy(DataTable* table);

void datatable_append_column(DataTable* table, DataChunk* chunk);

void datatable_append_row(DataTable* table, DataChunk* chunk);

typedef struct
{
    RowSegment* current_chunk;     // 当前正在扫描的 RowSegment
    ColumnPointer* columns;        // 每列的读取位置（segment + byte offset）
    usize offset;                  // current_chunk 内已扫描的行偏移
    RowSegment* last_chunk;        // 表中最后一个 RowSegment（用于判断结束）
    usize last_chunk_count;        // last_chunk 的行数（扫描开始时快照）
} ScanState;

void datatable_init_scan(DataTable* table, ScanState* state);
void scanstate_deinit(ScanState* state);
bool datatable_scan(DataTable* table, ScanState* state, DataChunk* output, usize* column_ids,
                    usize col_count);

#endif