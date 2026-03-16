#ifndef OPERATOR_H
#define OPERATOR_H

#include <assert.h>
#include <string.h>
#include "vb_type.h"
#include "types.h"

// 函数声明

void copy_fn(data_ptr_t src, data_ptr_t dst, usize offset, usize count, usize elem_size,
             usize* sel_vector);

void copy_fn_set_null(data_ptr_t src, data_ptr_t dst, usize offset, usize count, usize elem_size,
                      usize* sel_vector, u8* nullmask);

void generic_copy_loop(VectorBase* source, void* target, usize offset, usize element_count,
                       bool set_null, usize* sel_vector, u8* nullmask);

void copy_to_storage(VectorBase* source, void* target, usize offset, usize element_count);

typedef enum
{
    L2_SQUARED = 0,
    L2 = 1,
    COSINE = 2,
    INNER_PRODUCT = 3,
    L1 = 4,
} DistanceType;

/**
 * L2 距离的平方 (省去 sqrt，比较时等价)
 * sum((a[i] - b[i])^2)
 */
f32 vec_l2_squared_distance(const VectorBase* a, const VectorBase* b);

/**
 * L2 距离 (欧氏距离)
 * sqrt(sum((a[i] - b[i])^2))
 */
f32 vec_l2_distance(const VectorBase* a, const VectorBase* b);

/**
 * 内积
 * sum(a[i] * b[i])
 */
f32 vec_inner_product(const VectorBase* a, const VectorBase* b);

/**
 * 余弦距离 = 1 - cosine_similarity
 * 范围 [0, 2]，0 表示方向完全相同
 */
f32 vec_cosine_distance(const VectorBase* a, const VectorBase* b);

/**
 * L1 距离 (曼哈顿距离)
 * sum(|a[i] - b[i]|)
 */
f32 vec_l1_distance(const VectorBase* a, const VectorBase* b);
/**
 * 向量 L2 范数
 * sqrt(sum(v[i]^2))
 */
f32 vec_norm(const VectorBase* a);

/**
 * 原地归一化为单位向量
 * v[i] /= ||v||
 */
f32 vec_normalize(const VectorBase* a);

/**
 * 根据指定度量计算距离 (统一分发)
 *
 * DISTANCE_L2:             返回 L2 距离 (sqrt)
 * DISTANCE_COSINE:         返回余弦距离
 * DISTANCE_INNER_PRODUCT:  返回负内积 (用于 max-heap 统一排序：越小越相似)
 */
f32 vec_compute_distance(const VectorBase* a, const VectorBase* b, DistanceType type);

#endif  // OPERATOR_H