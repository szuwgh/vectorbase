#include "vb_type.h"

usize get_typeid_size(TypeID type)
{
    switch (type)
    {
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