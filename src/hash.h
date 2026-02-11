#ifndef HASH_H
#define HASH_H
#include <stdbool.h>
#include "vb_type.h"

u64 checksum(u8* buffer, usize size);

struct hmap_node
{
    struct hmap_node* next;
    const void* key;
    void* value;
};

struct hmap
{
    struct hmap_node** buckets;
    usize nbuckets;
    usize len; // 当前节点数
    uint32_t (*hash_func)(const void* key);
    int (*key_compare)(const void* key1, const void* key2);
};

struct hmap* hmap_init(usize nbuckets, uint32_t (*hash_func)(const void* key),
                       int (*key_compare)(const void* key1, const void* key2));

void hmap_insert(struct hmap* hmap, const void* key, void* value);

void* hmap_get(struct hmap* hmap, const void* key);

void* hmap_delete(struct hmap* hmap, const void* key);

bool hmap_contains(struct hmap* hmap, const void* key);

void hmap_destroy(struct hmap* hmap);

usize hmap_size(struct hmap* hmap);

#endif