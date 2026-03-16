#ifndef TYPES_H
#define TYPES_H
#include "vb_type.h"
#include "vector.h"
#include "interface.h"

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
} VectorBase;

void VectorBase_init(VectorBase* vector, TypeID type);

void VectorBase_from_vector(VectorBase* vector, Vector src, TypeID type);

void VectorBase_deinit(VectorBase* vector);

usize VectorBase_size(VectorBase* vector);

data_ptr_t VectorBase_get_data(VectorBase* vector);

#endif