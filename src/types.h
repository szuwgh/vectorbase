#ifndef TYPES_H
#define TYPES_H
#include "vb_type.h"
#include "vector.h"
// internal types
typedef enum
{
    TYPE_INVALID = 0,
    TYPE_INT8 = 1,
    TYPE_INT16 = 2,
    TYPE_INT32 = 3,
    TYPE_INT64 = 4,
    TYPE_FLOAT32 = 5,
    TYPE_FLOAT64 = 6,
} TypeID;

usize get_typeid_size(TypeID type);

// 列向量
typedef struct
{
    TypeID type;
    usize count;
    data_ptr_t data;
} ColumnVector;

void ColumnVector_init(ColumnVector* vector, TypeID type);

void ColumnVector_from_vector(ColumnVector* vector, Vector src, TypeID type);

void columnVector_deinit(ColumnVector* vector);

// from pgvector
typedef struct
{
    i32 vl_len_;  /* varlena header (do not touch directly!) */
    i16 dim;   /* number of dimensions */
    i16 unused;   /* reserved for future use, always zero */
    float x[FLEXIBLE_ARRAY_MEMBER];
} EmbeddingVector;

#endif