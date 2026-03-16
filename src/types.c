#include <stdlib.h>
#include <string.h>
#include "types.h"

usize get_typeid_size(TypeID type)
{
    switch (type)
    {
        case TYPE_INT8:
            return sizeof(u8);
        case TYPE_INT16:
            return sizeof(i16);
        case TYPE_INT32:
            return sizeof(i32);
        case TYPE_INT64:
            return sizeof(i64);
        case TYPE_FLOAT32:
            return sizeof(f32);
        case TYPE_FLOAT64:
            return sizeof(f64);
        default:
            return 0;
    }
}

void VectorBase_from_vector(VectorBase* vector, Vector src, TypeID type)
{
    vector->type = type;
    vector->count = src.size;
    vector->data = src.data;
}

void VectorBase_deinit(VectorBase* vector)
{
    vector->count = 0;
    free(vector->data);
}

usize VectorBase_size(VectorBase* vector)
{
    return vector->count;
}

data_ptr_t VectorBase_get_data(VectorBase* vector)
{
    return vector->data;
}
