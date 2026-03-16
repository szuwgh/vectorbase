#ifndef STORE_H
#define STORE_H

#include "interface.h"
#include "segment.h"
#include "types.h"
#include "vb_type.h"

/* Forward declaration — full definition is in datatable.h.
 * Pointer-only use here; datatable.h provides the struct body.
 * Avoids the circular: datatable.h → store.h → datatable.h. */
typedef struct DataChunk DataChunk;

typedef enum
{
    TUPLE_COL_B = 0,
    TUPLE_COL_I32 = 1,
    TUPLE_COL_I64 = 2,
    TUPLE_COL_F32 = 3,
    TUPLE_COL_F64 = 4,
    TUPLE_COL_TEXT = 5,
    TUPLE_COL_JSONB = 6,
} TupleColType;

typedef struct
{
    TupleColType type;
    union
    {
        bool b;
        i32 i32;
        i64 i64;
        f32 f32;
        f64 f64;
        struct
        {
            char* ptr;
            u32 len;
        } text;    /* heap-allocated when from row_store_get */
       // JsonB* jsonb;   /* heap-allocated when from row_store_get */
    } v;
} TupleVal;

typedef enum TableAmRoutineType
{
    TABLE_AM_ROUTINE_EMBEDDING = 0,
    TABLE_AM_ROUTINE_ROW = 1,
    TABLE_AM_ROUTINE_COLUMN = 2,
} TableAmRoutineType;

// clang-format off
DEFINE_CLASS(TableAmRoutine,
    VMETHOD(TableAmRoutine, append, void, VectorBase* v, TupleVal* payloads, usize n_payloads)
    VMETHOD(TableAmRoutine, append_chunk, void, const DataChunk* chunk)
    VMETHOD(TableAmRoutine, get,void, u64 idx, DataChunk* out)
    VMETHOD(TableAmRoutine,write_blocks, void, BlockManager* bm, MetaBlockWriter* w)
    VMETHOD(TableAmRoutine,load_blocks, void, BlockManager* bm, MetaBlockReader* r)
    ,
    FIELD(type, TableAmRoutineType)
)
// clang-format on

typedef struct
{
} EmbeddingTable;

typedef struct
{
} HeapTable;

typedef struct
{
} ColumnTable;

#endif
