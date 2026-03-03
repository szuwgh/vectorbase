#ifndef OPERATOR_H
#define OPERATOR_H

#include <assert.h>
#include <string.h>
#include "vb_type.h"
#include "types.h"

/* ---- generic_copy_loop: ColumnVector → flat buffer copy ----
 *
 * C 版本不需要按类型分派——memcpy 本身就是泛型的，
 * 只需要 elem_size 就能处理所有定宽类型。
 *
 * sel_vector: 选择向量，NULL 表示恒等映射 (identity)
 * nullmask:   每字节一个标记，非零表示 NULL；NULL 指针表示无空值
 */

static void copy_fn(data_ptr_t src, data_ptr_t dst, usize offset, usize count, usize elem_size,
                    usize* sel_vector)
{
    for (usize k = offset; k < offset + count; k++)
    {
        usize i = sel_vector ? sel_vector[k] : k;
        memcpy(dst + (k - offset) * elem_size, src + i * elem_size, elem_size);
    }
}

static void copy_fn_set_null(data_ptr_t src, data_ptr_t dst, usize offset, usize count,
                             usize elem_size, usize* sel_vector, u8* nullmask)
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

static void generic_copy_loop(ColumnVector* source, void* target, usize offset, usize element_count,
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

static void copy_to_storage(ColumnVector* source, void* target, usize offset, usize element_count)
{
    generic_copy_loop(source, target, offset, element_count, false, NULL, NULL);
}

#endif  // OPERATOR_H