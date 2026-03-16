#include "operator.h"
#include <stdio.h>
#include <string.h>
#include <math.h>
/* ---- generic_copy_loop: VectorBase → flat buffer copy ----
 *
 * C 版本不需要按类型分派——memcpy 本身就是泛型的，
 * 只需要 elem_size 就能处理所有定宽类型。
 *
 * sel_vector: 选择向量，NULL 表示恒等映射 (identity)
 * nullmask:   每字节一个标记，非零表示 NULL；NULL 指针表示无空值
 */

void copy_fn(data_ptr_t src, data_ptr_t dst, usize offset, usize count, usize elem_size,
             usize* sel_vector)
{
    for (usize k = offset; k < offset + count; k++)
    {
        usize i = sel_vector ? sel_vector[k] : k;
        memcpy(dst + (k - offset) * elem_size, src + i * elem_size, elem_size);
    }
}

void copy_fn_set_null(data_ptr_t src, data_ptr_t dst, usize offset, usize count, usize elem_size,
                      usize* sel_vector, u8* nullmask)
{
    if (!nullmask)
    {
        copy_fn(src, dst, offset, count, elem_size, sel_vector);
        return;
    }
    for (usize k = offset; k < offset + count; k++)
    {
        usize i = sel_vector ? sel_vector[k] : k;
        if (nullmask[i])
            memset(dst + (k - offset) * elem_size, 0, elem_size);
        else
            memcpy(dst + (k - offset) * elem_size, src + i * elem_size, elem_size);
    }
}

void generic_copy_loop(VectorBase* source, void* target, usize offset, usize element_count,
                       bool set_null, usize* sel_vector, u8* nullmask)
{
    if (source->count == 0) return;
    if (element_count == 0) element_count = source->count;
    assert(offset + element_count <= source->count);

    usize elem_size = get_typeid_size(source->type);
    if (set_null)
        copy_fn_set_null(source->data, (data_ptr_t)target, offset, element_count, elem_size,
                         sel_vector, nullmask);
    else
        copy_fn(source->data, (data_ptr_t)target, offset, element_count, elem_size, sel_vector);
}

void copy_to_storage(VectorBase* source, void* target, usize offset, usize element_count)
{
    generic_copy_loop(source, target, offset, element_count, false, NULL, NULL);
}

static inline bool check_dims(const VectorBase* a, const VectorBase* b)
{
    if (a->count != b->count)
    {
        fprintf(stderr, "Dimension mismatch: a->count = %d, b->count = %d\n", a->count, b->count);
        return false;
    }
    return true;
}

static f32 l2_squared_distance(usize count, const float* ax, const float* bx)
{
    float distance = 0.0;

        /* Auto-vectorized */
    for (usize i = 0; i < count; i++)
    {
        float diff = ax[i] - bx[i];

        distance += diff * diff;
    }

    return distance;
}

f32 vec_l2_squared_distance(const VectorBase* a, const VectorBase* b)
{
    if (!check_dims(a, b)) return NAN;
    return l2_squared_distance(a->count, (const f32*)a->data, (const f32*)b->data);
}

f32 vec_l2_distance(const VectorBase* a, const VectorBase* b)
{
    if (!check_dims(a, b)) return NAN;
    return sqrt(l2_squared_distance(a->count, (const f32*)a->data, (const f32*)b->data));
}

static f32 inner_product(usize count, const f32* ax, const f32* bx)
{
    float distance = 0.0;
    /* Auto-vectorized */
    for (usize i = 0; i < count; i++) distance += ax[i] * bx[i];
    return distance;
}

f32 vec_inner_product(const VectorBase* a, const VectorBase* b)
{
    if (!check_dims(a, b)) return NAN;
    return inner_product(a->count, (const f32*)a->data, (const f32*)b->data);
}

static f32 cosine_distance(usize count, const f32* ax, const f32* bx)
{
    float similarity = 0.0;
    float norma = 0.0;
    float normb = 0.0;

    /* Auto-vectorized */
    for (usize i = 0; i < count; i++)
    {
        similarity += ax[i] * bx[i];
        norma += ax[i] * ax[i];
        normb += bx[i] * bx[i];
    }

        /* Use sqrt(a * b) over sqrt(a) * sqrt(b) */
    return (double)similarity / sqrt((double)norma * (double)normb);
}

f32 vec_cosine_distance(const VectorBase* a, const VectorBase* b)
{
    if (!check_dims(a, b)) return NAN;
    f32 similarity = cosine_distance(a->count, (const f32*)a->data, (const f32*)b->data);
    if (isnan(similarity)) return NAN;
    // 边界裁剪（浮点精度可能导致值超出[-1,1]）
    if (similarity > 1.0f) similarity = 1.0f;
    if (similarity < -1.0f) similarity = -1.0f;
    // 余弦距离 = 1 - 余弦相似度
    return 1.0f - similarity;
}

static f32 l1_distance(usize count, const f32* ax, const f32* bx)
{
    float distance = 0.0;
        /* Auto-vectorized */
    for (usize i = 0; i < count; i++) distance += fabsf(ax[i] - bx[i]);
    return distance;
}

f32 vec_l1_distance(const VectorBase* a, const VectorBase* b)
{
    if (!check_dims(a, b)) return NAN;
    return l1_distance(a->count, (const f32*)a->data, (const f32*)b->data);
}

f32 vec_compute_distance(const VectorBase* a, const VectorBase* b, DistanceType type)
{
    switch (type)
    {
        case L2_SQUARED:
            return vec_l2_squared_distance(a, b);
        case L2:
            return vec_l2_distance(a, b);
        case COSINE:
            return vec_cosine_distance(a, b);
        case INNER_PRODUCT:
            return -vec_inner_product(a, b);  // 负内积用于 max-heap 排序
        case L1:
            return vec_l1_distance(a, b);
        default:
            return NAN;
    }
}
