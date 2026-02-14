#ifndef VB_TYPE_H
#define VB_TYPE_H
#include <stdint.h>
#include <stdbool.h>

typedef uint8_t u8;
typedef uint64_t usize;
typedef int64_t i64;
typedef uint64_t u64;
typedef uint32_t u32;
typedef u8* data_ptr_t;
typedef u64 block_id_t;
typedef u64 idx_t;

typedef struct
{
} String;

/**
 * MAKE(type, init_fn, ...) - 零初始化 + 调用 init 函数，返回结构体值
 *
 * 用法：
 *   Vector v      = MAKE(Vector, sizeof(int), 0);
 *   hmap m        = MAKE(hmap);
 *   CatalogSet s  = MAKE(CatalogSet);
 */
#define MAKE(type, ...)                    \
    ({                                     \
        type _obj = {0};                   \
        type##_init(&_obj, ##__VA_ARGS__); \
        _obj;                              \
    })

#define VEC(elem, ...)   MAKE(Vector, sizeof(elem), ##__VA_ARGS__)

// _Generic dispatch: auto-select key_size, hash_func, key_compare by key type
// Supported key types: int, char*, const char*
// Other types: use MAKE(hmap, ...) directly
#define _MAP_KEY_SIZE(K) _Generic((K){0}, char*: (usize)0, const char*: (usize)0, u32: sizeof(K))

#define _MAP_HASH(K) \
    _Generic((K){0}, char*: hmap_str_hash, const char*: hmap_str_hash, u32: hmap_int_hash)

#define _MAP_CMP(K) \
    _Generic((K){0}, char*: hmap_str_cmp, const char*: hmap_str_cmp, u32: hmap_int_cmp)

#define MAP(key, value, ...) _MAP_INIT(key, value, ##__VA_ARGS__)

#define _MAP_INIT(key, value, nbuckets, ...) \
    MAKE(hmap, _MAP_KEY_SIZE(key), sizeof(value), nbuckets, _MAP_HASH(key), _MAP_CMP(key))

#define NEW(type, ...)                     \
    ({                                     \
        type* _ptr = malloc(sizeof(type)); \
        if (!_ptr)                         \
        {                                  \
            perror("malloc");              \
            exit(EXIT_FAILURE);            \
        }                                  \
        type##_init(_ptr, ##__VA_ARGS__);  \
        _ptr;                              \
    })

#endif // VB_TYPE_H