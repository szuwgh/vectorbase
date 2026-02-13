
#include <stdlib.h>
#include <stdbool.h>
#include <string.h>
#include "hash.h"

// https://nullprogram.com/blog/2018/07/31/
inline static u64 murmurhash32(u32 x)
{
    x ^= x >> 16;
    x *= UINT32_C(0x85ebca6b);
    x ^= x >> 13;
    x *= UINT32_C(0xc2b2ae35);
    x ^= x >> 16;
    return (u64)x;
}

inline static u64 murmurhash64(u64 x)
{
    x ^= x >> 30;
    x *= UINT64_C(0xbf58476d1ce4e5b9);
    x ^= x >> 27;
    x *= UINT64_C(0x94d049bb133111eb);
    x ^= x >> 31;
    return x;
}

static u64 Hash(u64 val)
{
    return murmurhash32((u32)val);
}

static u64 Hash1(const char* val, usize size)
{
    u64 hash = 5381;

    for (usize i = 0; i < size; i++)
    {
        hash = ((hash << 5) + hash) + val[i];
    }
    return hash;
}

static u64 Hash_char(char* val, usize size)
{
    return Hash1((const char*)val, size);
}

static u64 Hash_u8(u8* val, usize size)
{
    return Hash1((const char*)val, size);
}

u64 checksum(u8* buffer, usize size)
{
    u64 result = 5381;
    u64* ptr = (u64*)buffer;
    usize i;
        // for efficiency, we first hash uint64_t values
    for (i = 0; i < size / 8; i++)
    {
        result ^= Hash(ptr[i]);
    }
    if (size - i * 8 > 0)
    {
                // the remaining 0-7 bytes we hash using a string hash
        result ^= Hash_u8(buffer + i * 8, size - i * 8);
    }
    return result;
}

static u32 int_hash(const void* key)
{
    u32 value = *(const u32*)key;
    return (u32)murmurhash32(value);
}

static u32 str_hash(const void* key)
{
    const char* s = (const char*)key;
    u32 hash = 5381;
    int c;
    while ((c = *s++)) hash = ((hash << 5) + hash) + c;  // djb2 算法
    return hash;
}

static int str_compare(const void* a, const void* b)
{
    return strcmp((const char*)a, (const char*)b);
}

void hmap_init(hmap* hm, usize nbuckets, u32 (*hash_func)(const void* key),
               int (*key_compare)(const void* key1, const void* key2))
{
    if (!hm) return;
    hm->buckets = calloc(nbuckets, sizeof(hmap_node*));
    hm->nbuckets = nbuckets;
    hm->len = 0;
    hm->hash_func = hash_func;
    hm->key_compare = key_compare;
}

void hmap_deinit(hmap* hm)
{
    if (!hm) return;
    for (usize i = 0; i < hm->nbuckets; i++)
    {
        hmap_node* node = hm->buckets[i];
        while (node)
        {
            hmap_node* next = node->next;
            free(node);
            node = next;
        }
    }
    free(hm->buckets);
    hm->buckets = NULL;
    hm->nbuckets = 0;
    hm->len = 0;
}

void hmap_init_str(hmap* hm)
{
    hmap_init(hm, HMAP_DEFAULT_NBUCKETS, str_hash, str_compare);
}

hmap* hmap_create(usize nbuckets, uint32_t (*hash_func)(const void* key),
                  int (*key_compare)(const void* key1, const void* key2))
{
    hmap* hmap = malloc(sizeof(*hmap));
    if (!hmap) return NULL;
    hmap->buckets = calloc(nbuckets, sizeof(hmap_node*));
    hmap->nbuckets = nbuckets;
    hmap->len = 0;
    hmap->hash_func = hash_func;
    hmap->key_compare = key_compare;
    return hmap;
}

static void hmap_grow(hmap* hmap)
{
    usize new_nbuckets = hmap->nbuckets * 2;
    hmap_node** new_buckets = calloc(new_nbuckets, sizeof(hmap_node*));
    if (!new_buckets) return;
    for (usize i = 0; i < hmap->nbuckets; i++)
    {
        hmap_node* node = hmap->buckets[i];
        while (node)
        {
            hmap_node* next = node->next;
            uint32_t hash = hmap->hash_func(node->key);
            usize new_bucket_index = hash % new_nbuckets;
            node->next = new_buckets[new_bucket_index];
            new_buckets[new_bucket_index] = node;
            node = next;
        }
    }
    free(hmap->buckets);
    hmap->buckets = new_buckets;
    hmap->nbuckets = new_nbuckets;
}

hmap_node* hmap_insert(hmap* hmap, const void* key, void* value)
{
    // Implementation of insertion logic goes here
    uint32_t hash = hmap->hash_func(key);
    if (hmap->len >= hmap->nbuckets - (hmap->nbuckets >> 2)) // 75% load factor
    {
        hmap_grow(hmap);
    }
    usize bucket_index = hash % hmap->nbuckets;

    // 检查键是否已存在，如果存在则更新值
    hmap_node* existing = hmap->buckets[bucket_index];
    while (existing)
    {
        if (hmap->key_compare(existing->key, key) == 0)
        {
            existing->value = value;
            return existing;
        }
        existing = existing->next;
    }

    // 键不存在，创建新节点
    hmap_node* node = malloc(sizeof(hmap_node));
    if (!node) return NULL;
    node->key = key;
    node->value = value;
    node->next = hmap->buckets[bucket_index];
    hmap->buckets[bucket_index] = node;
    hmap->len++;
    return node;
}

hmap_node* hmap_get(hmap* hmap, const void* key)
{
    uint32_t hash = hmap->hash_func(key);
    usize bucket_index = hash % hmap->nbuckets;
    hmap_node* node = hmap->buckets[bucket_index];
    while (node)
    {
        if (hmap->key_compare(node->key, key) == 0) return node;
        node = node->next;
    }
    return NULL;
}

void* hmap_delete(hmap* hmap, const void* key)
{
    uint32_t hash = hmap->hash_func(key);
    size_t bucket_index = hash % hmap->nbuckets;
    hmap_node* node = hmap->buckets[bucket_index];
    hmap_node* prev = NULL;

    while (node)
    {
        if (hmap->key_compare(node->key, key) == 0)
        {
            // 找到节点，从链表移除
            void* value = node->value;
            if (prev)
            {
                prev->next = node->next;
            }
            else
            {
                hmap->buckets[bucket_index] = node->next;
            }
            free(node);
            hmap->len--;
            return value; // 返回被删除节点的值
        }
        prev = node;
        node = node->next;
    }
    return NULL; // 未找到
}

bool hmap_contains(hmap* hmap, const void* key)
{
    uint32_t hash = hmap->hash_func(key);
    size_t bucket_index = hash % hmap->nbuckets;
    hmap_node* node = hmap->buckets[bucket_index];
    while (node)
    {
        if (hmap->key_compare(node->key, key) == 0) return true;
        node = node->next;
    }
    return false;
}

void hmap_destroy(hmap* hmap)
{
    if (!hmap) return;
    hmap_deinit(hmap);
    // 释放哈希表结构体
    free(hmap);
}

size_t hmap_size(hmap* hmap)
{
    if (!hmap) return 0;
    return hmap->len;
}

void hmap_iter_init(hmap_iterator* iter, hmap* hmap)
{
    iter->hmap = hmap;
    iter->bucket_idx = 0;
    iter->node = NULL;
}

bool hmap_iter_next(hmap_iterator* iter)
{
    if (!iter->hmap) return false;
    if (iter->node)
    {
        iter->node = iter->node->next;
        if (iter->node) return true;
    }
    while (iter->bucket_idx < iter->hmap->nbuckets)
    {
        if (iter->hmap->buckets[iter->bucket_idx])
        {
            iter->node = iter->hmap->buckets[iter->bucket_idx];
            iter->bucket_idx++;
            return true;
        }
        iter->bucket_idx++;
    }
    return false;
}

const void* hmap_iter_key(hmap_iterator* iter)
{
    if (!iter->node) return NULL;
    return iter->node->key;
}

void* hmap_iter_value(hmap_iterator* iter)
{
    if (!iter->node) return NULL;
    return iter->node->value;
}
