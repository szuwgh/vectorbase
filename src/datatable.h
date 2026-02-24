#ifndef DATATABLE_H
#define DATATABLE_H

#include "vb_type.h"
#include "segment.h"

#define STORAGE_CHUNK_SIZE 250

// 列向量
typedef struct
{
    TypeID type;
    usize count;
    data_ptr_t data;
} ColumnVector;

// from pgvector
typedef struct
{
    i32 vl_len_;  /* varlena header (do not touch directly!) */
    i16 dim;   /* number of dimensions */
    i16 unused;   /* reserved for future use, always zero */
    float x[FLEXIBLE_ARRAY_MEMBER];
} EmbeddingVector;

typedef struct
{
    usize column_count;
    ColumnVector* columns;
} DataChunk;

typedef struct DataTable DataTable;
// StorageChunk 和 ColumnSegment 的分段边界不一定对齐。一个 INT32 列的 ColumnSegment（256KB）能装
// 262144/4 = 65536 行，而一个 StorageChunk 只管 10240
//    行。所以一个 ColumnSegment 可能跨多个 StorageChunk。
struct DataTable
{
    SegmentTree row_storage_tree;
    SegmentTree* column_storage_tree;
    TypeID* column_types;
    usize column_count;
};

DataTable* Datatable_create(StorageManager* manager, char* schema_name, char* table_name,
                            usize column_count, TypeID* column_types);

void datatable_append(DataTable* table, DataChunk* chunk);

#endif