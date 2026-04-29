#ifndef DATATABLE_H
#define DATATABLE_H

#include "store.h"
#include "vb_type.h"
#include "segment.h"
#include "vector.h"
#include "types.h"
#include "operator.h"

typedef struct DataChunk DataChunk;
typedef struct TableQueryResult TableQueryResult;
#define STANDARD_VECTOR_SIZE  50
#define STORAGE_CHUNK_VECTORS 5
#define STORAGE_CHUNK_SIZE    (STANDARD_VECTOR_SIZE * STORAGE_CHUNK_VECTORS)

#define TAM_CTX_STACK_MAX     64

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
struct DataChunk
{
    ChunkMode mode;
    usize count;  /* number of columns (ncols or nrows)                         */
    VectorBase* arrays; /* arrays[i] = typed array of all row values for col i */
    data_ptr_t data;   /* raw storage buffer (owned, optional)              */
    usize size;   /* bytes allocated in data                           */
    usize n_payloads; /*有多少payload 例如一个向量可能有多个payload 每个payload是一个RowVal
                     有可能是JsonB, 也可能是int ，但是Vbchunk 里面 每向量有多少个 payload
                     是相同的*/
    const Datum** payloads; /* per-row payload (EMBED only), may be NULL        */
};

void DataChunk_init(DataChunk* chunk, Vector types);

void dataChunk_deinit(DataChunk* chunk);

void dataChunk_clear(DataChunk* chunk);

void dataChunk_reset(DataChunk* chunk);

void dataChunk_append(DataChunk* chunk, usize index, VectorBase src);

usize dataChunk_size(DataChunk* chunk);
// StorageChunk 和 BlockSegment 的分段边界不一定对齐。一个 INT32 列的 BlockSegment（256KB）能装
// 262144/4 = 65536 行，而一个 StorageChunk 只管 10240
// 行。所以一个 BlockSegment 可能跨多个 StorageChunk。
// struct DataTable
// {
//     char* schema_name;
//     char* table_name;
//     StorageManager* manager;
//     SegmentTree row_storage_tree;
//     SegmentTree* column_storage_tree; // 列式存储
//     TypeID* column_types;
//     usize column_count;
// };

// void Datatable_destroy(DataTable* table);

// void datatable_append_column(DataTable* table, DataChunk* chunk);

// void datatable_append_row(DataTable* table, DataChunk* chunk);

typedef struct
{
    RowSegment* current_chunk;     // 当前正在扫描的 RowSegment
    ColumnPointer* columns;        // 每列的读取位置（segment + byte offset）
    usize offset;                  // current_chunk 内已扫描的行偏移
    RowSegment* last_chunk;        // 表中最后一个 RowSegment（用于判断结束）
    usize last_chunk_count;        // last_chunk 的行数（扫描开始时快照）
} ScanState;

// void datatable_init_scan(DataTable* table, ScanState* state);
// void scanstate_deinit(ScanState* state);
// bool datatable_scan(DataTable* table, ScanState* state, DataChunk* output, usize* column_ids,
//                     usize col_count);

typedef enum TableAmRoutineType
{
    TABLE_AM_ROUTINE_EMBEDDING = 0,
    TABLE_AM_ROUTINE_ROW = 1,
    TABLE_AM_ROUTINE_COLUMN = 2,
} TableAmRoutineType;

typedef struct
{
   // u64* out_row_ids; /* [count] — filled by heap; read by ANN insert when row_ids==NULL */
    ItemPtr* emb_ctids;   /* [count] — filled by phase-0 (emb) engines; read by heap */
    ItemPtr* heap_ctids;
    usize count;
    usize current_index;
    TxnId xid;
} TamInsertCtx;

typedef struct
{
    VectorBase query;
    DistanceType metric;
} VectorCondition;

typedef struct
{
    VectorCondition* vec_cond;
    usize k;         /* max rows to return */
} TamScanCtx;

typedef struct
{
    ItemPtr heap_ctid;    /* physical ctid of the heap slot */
    ItemPtr emb_ctid;     /* emb position (for TamEmbTable) */
} TamUpdateCtx;

typedef struct
{
    ItemPtr heap_ctid;    /* physical ctid of the heap slot */
    ItemPtr emb_ctid;     /* emb position (for TamEmbTable) */
} TamDeleteCtx;

// clang-format off
DEFINE_CLASS(TableAmRoutine,
    VMETHOD(TableAmRoutine, append, void, VectorBase* v, TupleVal* payloads, usize n_payloads)
    VMETHOD(TableAmRoutine, append_chunk, void, const DataChunk* chunk, TamInsertCtx* ctx)
    VMETHOD(TableAmRoutine, scan, int,  TamScanCtx* ctx, TableQueryResult* results, usize max_results)
    VMETHOD(TableAmRoutine, update, int, VectorBase* v, TupleVal* payloads, TamUpdateCtx* ctx)
    VMETHOD(TableAmRoutine, delete, int, TamDeleteCtx* ctx)
    VMETHOD(TableAmRoutine,write_blocks, void, BlockManager* bm, MetaBlockWriter* w)
    VMETHOD(TableAmRoutine,load_blocks, void, BlockManager* bm, MetaBlockReader* r)
    VMETHOD(TableAmRoutine,destroy, void)
    ,
    FIELD(type, TableAmRoutineType)
)
// clang-format on

// typedef struct
// {
//     EmbeddingStore store;  /* pointer — complete type from tmp/src/embedding_store.h */
// } EmbeddingTable;

// void EmbeddingTable_init(EmbeddingTable* table);
// void EmbeddingTable_deinit(EmbeddingTable* table);

/** Ordered list of column definitions.  Borrowed by RowStore. */
struct TableSchema
{
    const TupleColType* cols;
    u16 ncols;
};

typedef struct
{
    EXTENDS(TableAmRoutine);
    HeapStore store;
    TableSchema schema; /* column types for heap tuples (ncols = schema.ncols) */
} HeapTable;

void HeapTable_init(HeapTable* table, const TableSchema* schema, BlockManager* bm);
void HeapTable_deinit(HeapTable* table);

typedef struct
{
    EXTENDS(TableAmRoutine);
    ColumnStore store;  /* pointer — complete type from datatable.h */
    TableSchema schema;
} ColumnTable;

void ColumnTable_init(ColumnTable* table);
void ColumnTable_deinit(ColumnTable* table);

typedef struct
{
    EXTENDS(TableAmRoutine);
    EmbeddingStore embed_store;
    HeapTable heap_table;

} EmbeddingHeapTable;

void EmbeddingHeapTable_init(EmbeddingHeapTable* table, i16 dimension, const TableSchema* schema,
                             BlockManager* bm);
void EmbeddingHeapTable_deinit(EmbeddingHeapTable* table);

struct TableQueryResult
{
    ItemPtr heap_ctid;
    ItemPtr emb_ctid;
    VectorBase vector;
    Datum* payloads; /* optional, heap user cols (NULL if not requested) */
    f32 distance;
};

struct DataTable
{
    /* data */
    char* schema_name;   /* owned */
    char* table_name;    /* owned */
    TableAmRoutine* table;/* owned */
    usize ncols;
};

DataTable* Datatable_create(StorageManager* manager, char* schema_name, char* table_name,
                            usize column_count, TypeID* column_types);

void DataTable_init(DataTable* datatable);
void DataTable_deinit(DataTable* datatable);

void dataTable_insert_datachunk(DataTable* datatable, DataChunk* chunk);
void dataTable_insert(DataTable* datatable, VectorBase* v, TupleVal* payloads, usize n_payloads);
void dataTable_update(DataTable* datatable, ItemPtr old_heap_ctid, VectorBase* v,
                      TupleVal* payloads, usize n_payloads);
void dataTable_delete(DataTable* datatable, ItemPtr old_heap_ctid);
int dataTable_scan(DataTable* datatable, const VectorCondition* vec_cond, TableQueryResult* results,
                   usize max_results);

#endif