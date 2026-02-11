#ifndef VECTOR_H
#define VECTOR_H

#include "vb_type.h"
#include <stdbool.h>
#include <sys/types.h>

/**
 * Vector - 泛型动态数组（类似 C++ std::vector）
 *
 * 特性：
 * - 值拷贝语义：存储数据的副本，而非引用
 * - 自动扩容：容量不足时自动增长（2倍）
 * - 类型安全：通过 element_size 指定类型大小
 * - 支持任意类型：基本类型、结构体、指针等
 *
 * 使用示例：
 *
 * 1. 存储基本类型：
 *    Vector* vec = Vector_create(sizeof(int), 0);
 *    int val = 42;
 *    Vector_push_back(vec, &val);  // 复制值
 *    int* ptr = (int*)Vector_get(vec, 0);
 *
 * 2. 存储结构体：
 *    Vector* vec = Vector_create(sizeof(MyStruct), 0);
 *    MyStruct s = {...};
 *    Vector_push_back(vec, &s);  // 复制整个结构体
 *
 * 3. 存储指针（需要手动管理内存）：
 *    Vector* vec = Vector_create(sizeof(int*), 0);
 *    int* ptr = malloc(sizeof(int));
 *    *ptr = 42;
 *    Vector_push_back(vec, &ptr);  // 复制指针的值（地址）
 *
 *    // 使用指针
 *    int** ptr_ptr = (int**)Vector_get(vec, 0);
 *    int* retrieved = *ptr_ptr;
 *    printf("%d", *retrieved);  // 42
 *
 *    // 清理：必须手动释放指针指向的内存
 *    for (size_t i = 0; i < Vector_size(vec); i++) {
 *        int** p = (int**)Vector_get(vec, i);
 *        free(*p);
 *    }
 *    Vector_destroy(vec);
 *
 * 4. 存储 void* 通用指针：
 *    Vector* vec = Vector_create(sizeof(void*), 0);
 *    void* ptr = some_data;
 *    Vector_push_back(vec, &ptr);
 */

// 动态数组结构体 - 泛型实现
typedef struct
{
    void* data;          // 数据缓冲区（存储实际数据，非指针）
    usize size;          // 当前元素数量
    usize capacity;      // 当前容量
    usize element_size;  // 每个元素的字节大小
} Vector;

/**
 * @brief 初始化Vector
 *
 * @param vec Vector指针
 * @param element_size 每个元素的字节大小（例如 sizeof(int)）
 * @param initial_capacity 初始容量，如果为0则使用默认容量
 */
int Vector_init(Vector* vec, usize element_size, usize initial_capacity);

/**
 * @brief 创建一个新的Vector
 *
 * @param element_size 每个元素的字节大小（例如 sizeof(int)）
 * @param initial_capacity 初始容量，如果为0则使用默认容量
 * @return Vector* 新创建的Vector指针，失败返回NULL
 */
Vector* Vector_create(usize element_size, usize initial_capacity);

/**
 * @brief 销毁Vector并释放所有资源
 *
 * @param vec 要销毁的Vector指针
 */
void Vector_destroy(Vector* vec);

/**
 * @brief 向Vector末尾添加元素（复制）
 *
 * @param vec Vector指针
 * @param element 要添加的元素指针（会被复制）
 * @return int 成功返回0，失败返回-1
 */
int Vector_push_back(Vector* vec, const void* element);

/**
 * @brief 移除Vector末尾的元素
 *
 * @param vec Vector指针
 * @param out 输出参数，存储被移除的元素（可以为NULL）
 * @return int 成功返回0，失败返回-1
 */
int Vector_pop_back(Vector* vec, void* out);

/**
 * @brief 获取指定索引的元素
 *
 * @param vec Vector指针
 * @param index 元素索引
 * @return void* 元素指针（指向内部数据），如果索引越界返回NULL
 */
void* Vector_get(const Vector* vec, usize index);

/**
 * @brief 设置指定索引的元素（复制）
 *
 * @param vec Vector指针
 * @param index 元素索引
 * @param element 新元素指针（会被复制）
 * @return int 成功返回0，失败返回-1
 */
int Vector_set(Vector* vec, usize index, const void* element);

/**
 * @brief 获取Vector的大小
 *
 * @param vec Vector指针
 * @return usize 元素数量
 */
usize Vector_size(const Vector* vec);

/**
 * @brief 获取Vector的容量
 *
 * @param vec Vector指针
 * @return usize 当前容量
 */
usize Vector_capacity(const Vector* vec);

/**
 * @brief 获取元素大小
 *
 * @param vec Vector指针
 * @return usize 每个元素的字节大小
 */
usize Vector_element_size(const Vector* vec);

/**
 * @brief 检查Vector是否为空
 *
 * @param vec Vector指针
 * @return bool true表示为空，false表示非空
 */
bool Vector_empty(const Vector* vec);

/**
 * @brief 清空Vector中的所有元素
 *
 * @param vec Vector指针
 */
void Vector_clear(Vector* vec);

/**
 * @brief 预留容量
 *
 * @param vec Vector指针
 * @param new_capacity 新容量
 * @return int 成功返回0，失败返回-1
 */
int Vector_reserve(Vector* vec, usize new_capacity);

/**
 * @brief 调整Vector大小
 *
 * @param vec Vector指针
 * @param new_size 新大小
 * @param default_value 新元素的默认值（如果扩大，可以为NULL表示零初始化）
 * @return int 成功返回0，失败返回-1
 */
int Vector_resize(Vector* vec, usize new_size, const void* default_value);

/**
 * @brief 在指定位置插入元素（复制）
 *
 * @param vec Vector指针
 * @param index 插入位置
 * @param element 要插入的元素指针（会被复制）
 * @return int 成功返回0，失败返回-1
 */
int Vector_insert(Vector* vec, usize index, const void* element);

/**
 * @brief 移除指定位置的元素
 *
 * @param vec Vector指针
 * @param index 要移除的位置
 * @param out 输出参数，存储被移除的元素（可以为NULL）
 * @return int 成功返回0，失败返回-1
 */
int Vector_erase(Vector* vec, usize index, void* out);

/**
 * @brief 查找元素
 *
 * @param vec Vector指针
 * @param element 要查找的元素指针
 * @param compare 比较函数，返回0表示相等
 * @return ssize_t 元素索引，未找到返回-1
 */
ssize_t Vector_find(const Vector* vec, const void* element,
                    int (*compare)(const void*, const void*));

/**
 * @brief 获取Vector的前端元素
 *
 * @param vec Vector指针
 * @return void* 前端元素指针（指向内部数据），如果为空返回NULL
 */
void* Vector_front(const Vector* vec);

/**
 * @brief 获取Vector的后端元素
 *
 * @param vec Vector指针
 * @return void* 后端元素指针（指向内部数据），如果为空返回NULL
 */
void* Vector_back(const Vector* vec);

/**
 * @brief 获取指定索引的元素（带边界检查，复制到输出）
 *
 * @param vec Vector指针
 * @param index 元素索引
 * @param out 输出缓冲区
 * @return int 成功返回0，失败返回-1
 */
int Vector_get_copy(const Vector* vec, usize index, void* out);

/**
 * @brief 获取内部数据指针（用于直接访问，小心使用）
 *
 * @param vec Vector指针
 * @return void* 内部数据缓冲区指针
 */
void* Vector_data(const Vector* vec);

// 类型安全的宏辅助（可选）
#define VECTOR_GET(vec, index, type) ((type*)Vector_get(vec, index))

#define VECTOR_FRONT(vec, type)      ((type*)Vector_front(vec))

#define VECTOR_BACK(vec, type)       ((type*)Vector_back(vec))

#endif  // VECTOR_H
