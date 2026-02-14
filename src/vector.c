#include "vector.h"
#include <stdlib.h>
#include <string.h>

#define VECTOR_DEFAULT_CAPACITY 16
#define VECTOR_GROWTH_FACTOR    2

// 获取指定索引元素的内部指针（辅助函数）
static inline void* vector_get_ptr(const Vector* vec, usize index)
{
    return (char*)vec->data + (index * vec->element_size);
}

int Vector_init(Vector* vec, usize element_size, usize initial_capacity)
{
    if (initial_capacity == 0)
    {
        initial_capacity = VECTOR_DEFAULT_CAPACITY;
    }

    vec->data = malloc(element_size * initial_capacity);
    if (!vec->data)
    {
        return -1;
    }

    vec->size = 0;
    vec->capacity = initial_capacity;
    vec->element_size = element_size;

    return 0;
}

Vector* vector_create(usize element_size, usize initial_capacity)
{
    if (element_size == 0)
    {
        return NULL;  // 元素大小不能为0
    }

    Vector* vec = (Vector*)malloc(sizeof(Vector));
    if (!vec)
    {
        return NULL;
    }

    if (Vector_init(vec, element_size, initial_capacity) != 0)
    {
        free(vec);
        return NULL;
    }

    return vec;
}

void vector_destroy(Vector* vec)
{
    if (vec)
    {
        if (vec->data)
        {
            free(vec->data);
        }
        free(vec);
    }
}

void vector_deinit(Vector* vec)
{
    if (vec)
    {
        if (vec->data)
        {
            free(vec->data);
            vec->data = NULL;
        }
        vec->size = 0;
        vec->capacity = 0;
        vec->element_size = 0;
    }
}

static int vector_grow(Vector* vec)
{
    usize new_capacity =
        vec->capacity > 0 ? vec->capacity * VECTOR_GROWTH_FACTOR : VECTOR_DEFAULT_CAPACITY;
    void* new_data = realloc(vec->data, vec->element_size * new_capacity);
    if (!new_data)
    {
        return -1;
    }

    vec->data = new_data;
    vec->capacity = new_capacity;
    return 0;
}

int vector_push_back(Vector* vec, const void* element)
{
    if (!vec || !element)
    {
        return -1;
    }

    if (vec->size >= vec->capacity)
    {
        if (vector_grow(vec) != 0)
        {
            return -1;
        }
    }

    // 复制元素到缓冲区末尾
    void* dest = vector_get_ptr(vec, vec->size);
    memcpy(dest, element, vec->element_size);
    vec->size++;
    return 0;
}

int vector_pop_back(Vector* vec, void* out)
{
    if (!vec || vec->size == 0)
    {
        return -1;
    }

    vec->size--;

    // 如果需要输出，复制元素
    if (out)
    {
        void* src = vector_get_ptr(vec, vec->size);
        memcpy(out, src, vec->element_size);
    }

    return 0;
}

void* vector_get(const Vector* vec, usize index)
{
    if (!vec || index >= vec->size)
    {
        return NULL;
    }

    return vector_get_ptr(vec, index);
}

int vector_get_copy(const Vector* vec, usize index, void* out)
{
    if (!vec || !out || index >= vec->size)
    {
        return -1;
    }

    void* src = vector_get_ptr(vec, index);
    memcpy(out, src, vec->element_size);
    return 0;
}

int vector_set(Vector* vec, usize index, const void* element)
{
    if (!vec || !element || index >= vec->size)
    {
        return -1;
    }

    void* dest = vector_get_ptr(vec, index);
    memcpy(dest, element, vec->element_size);
    return 0;
}

usize vector_size(const Vector* vec)
{
    if (!vec)
    {
        return 0;
    }
    return vec->size;
}

usize vector_capacity(const Vector* vec)
{
    if (!vec)
    {
        return 0;
    }
    return vec->capacity;
}

usize vector_element_size(const Vector* vec)
{
    if (!vec)
    {
        return 0;
    }
    return vec->element_size;
}

bool vector_empty(const Vector* vec)
{
    return vec == NULL || vec->size == 0;
}

void vector_clear(Vector* vec)
{
    if (vec)
    {
        vec->size = 0;
    }
}

int vector_reserve(Vector* vec, usize new_capacity)
{
    if (!vec)
    {
        return -1;
    }

    if (new_capacity <= vec->capacity)
    {
        return 0;  // 已经有足够容量
    }

    void* new_data = realloc(vec->data, vec->element_size * new_capacity);
    if (!new_data)
    {
        return -1;
    }

    vec->data = new_data;
    vec->capacity = new_capacity;
    return 0;
}

int vector_resize(Vector* vec, usize new_size, const void* default_value)
{
    if (!vec)
    {
        return -1;
    }

    if (new_size > vec->capacity)
    {
        if (vector_reserve(vec, new_size) != 0)
        {
            return -1;
        }
    }

    if (new_size > vec->size)
    {
        // 扩大：填充默认值或零
        for (usize i = vec->size; i < new_size; i++)
        {
            void* dest = vector_get_ptr(vec, i);
            if (default_value)
            {
                memcpy(dest, default_value, vec->element_size);
            }
            else
            {
                memset(dest, 0, vec->element_size);
            }
        }
    }

    vec->size = new_size;
    return 0;
}

int vector_insert(Vector* vec, usize index, const void* element)
{
    if (!vec || !element || index > vec->size)
    {
        return -1;
    }

    if (vec->size >= vec->capacity)
    {
        if (vector_grow(vec) != 0)
        {
            return -1;
        }
    }

    // 移动元素为新元素腾出空间
    if (index < vec->size)
    {
        void* src = vector_get_ptr(vec, index);
        void* dest = vector_get_ptr(vec, index + 1);
        usize bytes_to_move = (vec->size - index) * vec->element_size;
        memmove(dest, src, bytes_to_move);
    }

    // 复制新元素
    void* dest = vector_get_ptr(vec, index);
    memcpy(dest, element, vec->element_size);
    vec->size++;
    return 0;
}

int vector_erase(Vector* vec, usize index, void* out)
{
    if (!vec || index >= vec->size)
    {
        return -1;
    }

    // 如果需要输出，复制元素
    if (out)
    {
        void* src = vector_get_ptr(vec, index);
        memcpy(out, src, vec->element_size);
    }

    // 向前移动元素
    if (index < vec->size - 1)
    {
        void* dest = vector_get_ptr(vec, index);
        void* src = vector_get_ptr(vec, index + 1);
        usize bytes_to_move = (vec->size - index - 1) * vec->element_size;
        memmove(dest, src, bytes_to_move);
    }

    vec->size--;
    return 0;
}

ssize_t vector_find(const Vector* vec, const void* element,
                    int (*compare)(const void*, const void*))
{
    if (!vec || !element || !compare)
    {
        return -1;
    }

    for (usize i = 0; i < vec->size; i++)
    {
        void* current = vector_get_ptr(vec, i);
        if (compare(current, element) == 0)
        {
            return (ssize_t)i;
        }
    }

    return -1;
}

void* vector_front(const Vector* vec)
{
    if (!vec || vec->size == 0)
    {
        return NULL;
    }
    return vec->data;
}

void* vector_back(const Vector* vec)
{
    if (!vec || vec->size == 0)
    {
        return NULL;
    }
    return vector_get_ptr(vec, vec->size - 1);
}

void* vector_data(const Vector* vec)
{
    if (!vec)
    {
        return NULL;
    }
    return vec->data;
}

void vector_iter_init(VectorIterator* iter, Vector* vec)
{
    if (!iter || !vec)
    {
        return;
    }
    iter->vec = vec;
    iter->index = (usize)-1;  // "before-first" position
}

bool vector_iter_next(VectorIterator* iter)
{
    if (!iter || !iter->vec)
    {
        return false;
    }
    iter->index++;
    return iter->index < iter->vec->size;
}

void* vector_iter_get(VectorIterator* iter)
{
    if (!iter || !iter->vec || iter->index >= iter->vec->size)
    {
        return NULL;
    }
    return vector_get_ptr(iter->vec, iter->index);
}