#ifndef HASH_H
#define HASH_H
#include "vb_type.h"

#define HMAP_DEFAULT_NBUCKETS 16

u64 checksum(u8* buffer, usize size);

// Built-in hash/compare functions (used by MAP macro via _Generic)
u32 hmap_int_hash(const void* key);
int hmap_int_cmp(const void* a, const void* b);
u32 hmap_str_hash(const void* key);
int hmap_str_cmp(const void* a, const void* b);

typedef struct hmap_node hmap_node;

struct hmap_node
{
    hmap_node* next;
    void* key;
    void* value;
};

typedef struct
{
    hmap_node** buckets;
    usize nbuckets;
    usize len; // 当前节点数
    usize key_size;   // 0 表示字符串 key（变长，用 strlen+1）
    usize value_size;
    uint32_t (*hash_func)(const void* key);
    int (*key_compare)(const void* key1, const void* key2);
} hmap;

void hmap_init(hmap* hm, usize key_size, usize value_size, usize nbuckets,
               u32 (*hash_func)(const void* key),
               int (*key_compare)(const void* key1, const void* key2));

void hmap_deinit(hmap* hm);

void hmap_init_str(hmap* hm, usize value_size);

hmap* hmap_create(usize key_size, usize value_size, usize nbuckets,
                  uint32_t (*hash_func)(const void* key),
                  int (*key_compare)(const void* key1, const void* key2));

hmap_node* hmap_insert(hmap* hmap, const void* key, const void* value);

hmap_node* hmap_get(hmap* hmap, const void* key);

int hmap_delete(hmap* hmap, const void* key, void* out);

bool hmap_contains(hmap* hmap, const void* key);

void hmap_destroy(hmap* hmap);

usize hmap_size(hmap* hmap);

#define HMAP_VALUE(node, type) (*(type*)((node)->value))

typedef struct hmap_iterator hmap_iterator;

struct hmap_iterator
{
    hmap* hmap;
    usize bucket_idx;
    hmap_node* node;
};

void hmap_iter_init(hmap_iterator* iter, hmap* hmap);

bool hmap_iter_next(hmap_iterator* iter);

const void* hmap_iter_key(hmap_iterator* iter);

void* hmap_iter_value(hmap_iterator* iter);

#define HMAP_FOREACH(hmap_ptr, entry_var)           \
    hmap_iterator _iter_##entry_var;                \
    hmap_iter_init(&_iter_##entry_var, (hmap_ptr)); \
    void* entry_var;                                \
    while (hmap_iter_next(&_iter_##entry_var) &&    \
           ((entry_var = hmap_iter_value(&_iter_##entry_var)), 1))

#endif
