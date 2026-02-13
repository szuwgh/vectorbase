#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "../src/hash.h"

static int pass_count = 0;
static int fail_count = 0;

#define PASS(msg, ...)                             \
    do {                                           \
        pass_count++;                              \
        printf("[PASS] " msg "\n", ##__VA_ARGS__); \
    } while (0)

#define FAIL(msg, ...)                             \
    do {                                           \
        fail_count++;                              \
        printf("[FAIL] " msg "\n", ##__VA_ARGS__); \
    } while (0)

static uint32_t int_hash(const void* key)
{
    return (uint32_t)(*(int*)key);
}

static int int_compare(const void* key1, const void* key2)
{
    return *(int*)key1 - *(int*)key2;
}

static uint32_t string_hash(const void* key)
{
    const char* str = (const char*)key;
    uint32_t hash = 5381;
    int c;
    while ((c = *str++))
    {
        hash = ((hash << 5) + hash) + c;
    }
    return hash;
}

static int string_compare(const void* key1, const void* key2)
{
    return strcmp((const char*)key1, (const char*)key2);
}

// Hash that always returns 0, forces all keys into bucket 0
static uint32_t always_zero_hash(const void* key)
{
    (void)key;
    return 0;
}

// ========== Test: checksum function ==========
void test_checksum(void)
{
    printf("\n=== Test checksum ===\n");

    u8 data1[] = "Hello, checksum!";
    u64 cs1 = checksum(data1, sizeof(data1));
    u64 cs2 = checksum(data1, sizeof(data1));

    if (cs1 == cs2)
        PASS("Checksum is deterministic (same input -> same output)");
    else
        FAIL("Checksum not deterministic");

    // checksum() uses murmurhash32 on lower 32 bits of each u64 chunk,
    // difference must be in the lower 4 bytes of a chunk
    u8 data2[] = "Xello, checksum!";
    u64 cs3 = checksum(data2, sizeof(data2));
    if (cs1 != cs3)
        PASS("Different data produces different checksum");
    else
        FAIL("Different data should produce different checksum");

    u8 empty_data[] = "";
    u64 cs_empty = checksum(empty_data, 0);
    (void)cs_empty;
    PASS("Checksum on empty data does not crash");

    u8 single = 0xAB;
    u64 cs_single = checksum(&single, 1);
    (void)cs_single;
    PASS("Checksum on single byte works");

    u8* large_buf = (u8*)malloc(65536);
    memset(large_buf, 0xCC, 65536);
    u64 cs_large1 = checksum(large_buf, 65536);
    u64 cs_large2 = checksum(large_buf, 65536);
    if (cs_large1 == cs_large2)
        PASS("Checksum on large buffer (64KB) is consistent");
    else
        FAIL("Checksum on large buffer inconsistent");

    large_buf[32768] ^= 0xFF;
    u64 cs_large3 = checksum(large_buf, 65536);
    if (cs_large1 != cs_large3)
        PASS("Single byte change in large buffer changes checksum");
    else
        FAIL("Checksum should change with single byte flip");

    free(large_buf);
}

// ========== Test: hmap_create / hmap_destroy ==========
void test_hmap_create_destroy(void)
{
    printf("\n=== Test hmap_create_destroy ===\n");

    hmap* map = hmap_create(16, int_hash, int_compare);
    if (!map)
    {
        FAIL("hmap_create failed");
        return;
    }

    if (hmap_size(map) == 0 && map->nbuckets == 16)
        PASS("hmap_create correct (size=0, buckets=16)");
    else
        FAIL("hmap_create initialization incorrect");

    hmap_destroy(map);
    PASS("hmap_destroy completed");
}

// ========== Test: hmap_init (in-place) ==========
void test_hmap_init(void)
{
    printf("\n=== Test hmap_init (in-place) ===\n");

    hmap* map = (hmap*)malloc(sizeof(hmap));
    hmap_init(map, 8, int_hash, int_compare);

    if (map->nbuckets == 8 && map->len == 0 && map->buckets != NULL)
        PASS("hmap_init in-place initialization correct");
    else
        FAIL("hmap_init initialization incorrect");

    int key = 42;
    int value = 100;
    hmap_node* node = hmap_insert(map, &key, &value);
    if (node && node->value == &value)
        PASS("Insert into hmap_init'd map works");
    else
        FAIL("Insert into hmap_init'd map failed");

    hmap_destroy(map);
}

// ========== Test: hmap_init_str (string convenience) ==========
void test_hmap_init_str(void)
{
    printf("\n=== Test hmap_init_str ===\n");

    hmap* map = (hmap*)malloc(sizeof(hmap));
    hmap_init_str(map);

    if (map->nbuckets == HMAP_DEFAULT_NBUCKETS && map->len == 0)
        PASS("hmap_init_str uses default buckets (%d)", HMAP_DEFAULT_NBUCKETS);
    else
        FAIL("hmap_init_str initialization incorrect");

    // Insert string keys using the built-in str_hash/str_compare
    const char* k1 = "hello";
    const char* k2 = "world";
    int v1 = 1, v2 = 2;
    hmap_insert(map, k1, &v1);
    hmap_insert(map, k2, &v2);

    hmap_node* n1 = hmap_get(map, "hello");
    hmap_node* n2 = hmap_get(map, "world");
    if (n1 && *(int*)n1->value == 1 && n2 && *(int*)n2->value == 2)
        PASS("hmap_init_str string key lookup works");
    else
        FAIL("hmap_init_str string key lookup failed");

    if (hmap_size(map) == 2)
        PASS("hmap_init_str size correct");
    else
        FAIL("hmap_init_str size incorrect");

    hmap_destroy(map);
}

// ========== Test: insert and get ==========
void test_hmap_insert_get(void)
{
    printf("\n=== Test hmap_insert_get ===\n");

    hmap* map = hmap_create(8, int_hash, int_compare);
    if (!map) { FAIL("hmap_create failed"); return; }

    int keys[] = {1, 2, 3, 4, 5};
    int values[] = {10, 20, 30, 40, 50};
    int num = 5;

    for (int i = 0; i < num; i++)
    {
        hmap_node* node = hmap_insert(map, &keys[i], &values[i]);
        if (!node)
        {
            FAIL("hmap_insert returned NULL for key %d", keys[i]);
            hmap_destroy(map);
            return;
        }
    }

    if (hmap_size(map) == 5)
        PASS("Inserted 5 pairs (size=%zu)", hmap_size(map));
    else
        FAIL("Size incorrect after insert");

    // hmap_insert returns hmap_node*
    hmap_node* first_node = hmap_insert(map, &keys[0], &values[0]);
    if (first_node != NULL)
        PASS("hmap_insert returns non-NULL node");
    else
        FAIL("hmap_insert should return non-NULL node");

    // hmap_get returns hmap_node*
    bool all_correct = true;
    for (int i = 0; i < num; i++)
    {
        hmap_node* node = hmap_get(map, &keys[i]);
        if (!node || *(int*)node->value != values[i])
        {
            all_correct = false;
            break;
        }
    }

    if (all_correct)
        PASS("All get operations correct (via node->value)");
    else
        FAIL("Some get operations failed");

    // Get non-existent key
    int non_key = 999;
    if (hmap_get(map, &non_key) == NULL)
        PASS("Get non-existent key returns NULL");
    else
        FAIL("Get non-existent key should return NULL");

    hmap_destroy(map);
}

// ========== Test: contains ==========
void test_hmap_contains(void)
{
    printf("\n=== Test hmap_contains ===\n");

    hmap* map = hmap_create(8, int_hash, int_compare);
    if (!map) { FAIL("hmap_create failed"); return; }

    int keys[] = {10, 20, 30};
    int values[] = {100, 200, 300};
    for (int i = 0; i < 3; i++) hmap_insert(map, &keys[i], &values[i]);

    if (hmap_contains(map, &keys[0]))
        PASS("Contains found existing key %d", keys[0]);
    else
        FAIL("Contains should find existing key");

    int non_existent = 999;
    if (!hmap_contains(map, &non_existent))
        PASS("Contains returns false for non-existent key");
    else
        FAIL("Contains should not find non-existent key");

    hmap_destroy(map);
}

// ========== Test: delete ==========
void test_hmap_delete(void)
{
    printf("\n=== Test hmap_delete ===\n");

    hmap* map = hmap_create(8, int_hash, int_compare);
    if (!map) { FAIL("hmap_create failed"); return; }

    int keys[] = {1, 2, 3, 4, 5};
    int values[] = {10, 20, 30, 40, 50};
    for (int i = 0; i < 5; i++) hmap_insert(map, &keys[i], &values[i]);

    int key_to_delete = 3;
    int* deleted_value = (int*)hmap_delete(map, &key_to_delete);

    if (deleted_value && *deleted_value == 30)
        PASS("Deleted key 3, value: %d", *deleted_value);
    else
        FAIL("Delete failed");

    if (hmap_size(map) == 4)
        PASS("Size correct after delete");
    else
        FAIL("Size incorrect after delete");

    if (!hmap_contains(map, &key_to_delete))
        PASS("Deleted key no longer exists");
    else
        FAIL("Key should not exist after deletion");

    if (hmap_contains(map, &keys[0]) && hmap_contains(map, &keys[1]))
        PASS("Other keys still exist");
    else
        FAIL("Other keys should still exist");

    int non_existent = 999;
    if (hmap_delete(map, &non_existent) == NULL)
        PASS("Delete non-existent key returns NULL");
    else
        FAIL("Delete should return NULL for non-existent key");

    hmap_destroy(map);
}

// ========== Test: update existing key ==========
void test_hmap_update(void)
{
    printf("\n=== Test hmap_update ===\n");

    hmap* map = hmap_create(8, int_hash, int_compare);
    if (!map) { FAIL("hmap_create failed"); return; }

    int key = 42;
    int value1 = 100;
    int value2 = 200;

    hmap_node* n1 = hmap_insert(map, &key, &value1);
    hmap_node* n2 = hmap_insert(map, &key, &value2);

    // Update should return the same node
    if (n1 == n2)
        PASS("Update returns same node");
    else
        FAIL("Update should return same node");

    hmap_node* node = hmap_get(map, &key);
    if (node && *(int*)node->value == value2)
        PASS("Update existing key works (value=%d)", *(int*)node->value);
    else
        FAIL("Update failed");

    if (hmap_size(map) == 1)
        PASS("Size remains 1 after update");
    else
        FAIL("Size should not increase on update");

    hmap_destroy(map);
}

// ========== Test: string keys ==========
void test_hmap_string_keys(void)
{
    printf("\n=== Test hmap_string_keys ===\n");

    hmap* map = hmap_create(16, string_hash, string_compare);
    if (!map) { FAIL("hmap_create failed"); return; }

    const char* keys[] = {"apple", "banana", "cherry", "date"};
    int values[] = {1, 2, 3, 4};
    int num = 4;

    for (int i = 0; i < num; i++) hmap_insert(map, keys[i], &values[i]);

    if (hmap_size(map) == 4)
        PASS("Inserted 4 string key-value pairs");
    else
        FAIL("Size incorrect after insert");

    bool all_correct = true;
    for (int i = 0; i < num; i++)
    {
        hmap_node* node = hmap_get(map, keys[i]);
        if (!node || *(int*)node->value != values[i])
        {
            all_correct = false;
            break;
        }
    }

    if (all_correct)
        PASS("All string key operations correct");
    else
        FAIL("Some string key operations failed");

    hmap_destroy(map);
}

// ========== Test: collision handling ==========
void test_hmap_collisions(void)
{
    printf("\n=== Test hmap_collisions ===\n");

    hmap* map = hmap_create(2, int_hash, int_compare);
    if (!map) { FAIL("hmap_create failed"); return; }

    int keys[] = {0, 2, 4, 6, 8, 10};
    int values[] = {10, 20, 30, 40, 50, 60};
    int num = 6;

    for (int i = 0; i < num; i++) hmap_insert(map, &keys[i], &values[i]);

    bool all_found = true;
    for (int i = 0; i < num; i++)
    {
        hmap_node* node = hmap_get(map, &keys[i]);
        if (!node || *(int*)node->value != values[i])
        {
            all_found = false;
            break;
        }
    }

    if (all_found)
        PASS("All keys retrievable despite collisions (2 buckets, %d items)", num);
    else
        FAIL("Some keys lost due to collision handling issues");

    hmap_destroy(map);
}

// ========== Test: automatic grow ==========
void test_hmap_grow(void)
{
    printf("\n=== Test hmap_grow ===\n");

    hmap* map = hmap_create(4, int_hash, int_compare);
    if (!map) { FAIL("hmap_create failed"); return; }

    usize initial_buckets = map->nbuckets;

    int* keys = malloc(sizeof(int) * 20);
    int* values = malloc(sizeof(int) * 20);
    for (int i = 0; i < 20; i++)
    {
        keys[i] = i;
        values[i] = i * 10;
        hmap_insert(map, &keys[i], &values[i]);
    }

    if (map->nbuckets > initial_buckets)
        PASS("HashMap grew automatically (%zu -> %zu buckets)", initial_buckets, map->nbuckets);
    else
        FAIL("HashMap should have grown");

    bool all_correct = true;
    for (int i = 0; i < 20; i++)
    {
        hmap_node* node = hmap_get(map, &keys[i]);
        if (!node || *(int*)node->value != values[i])
        {
            all_correct = false;
            break;
        }
    }

    if (all_correct)
        PASS("All data intact after growth");
    else
        FAIL("Some data lost after growth");

    free(keys);
    free(values);
    hmap_destroy(map);
}

// ========== Test: stress ==========
void test_hmap_stress(void)
{
    printf("\n=== Test hmap_stress ===\n");

    hmap* map = hmap_create(16, int_hash, int_compare);
    if (!map) { FAIL("hmap_create failed"); return; }

    const int num = 1000;
    int* keys = malloc(sizeof(int) * num);
    int* values = malloc(sizeof(int) * num);

    for (int i = 0; i < num; i++)
    {
        keys[i] = i;
        values[i] = i * 2;
        hmap_insert(map, &keys[i], &values[i]);
    }

    if (hmap_size(map) == (usize)num)
        PASS("Inserted %d items (buckets=%zu)", num, map->nbuckets);
    else
        FAIL("Size incorrect");

    bool all_correct = true;
    for (int i = 0; i < num; i++)
    {
        hmap_node* node = hmap_get(map, &keys[i]);
        if (!node || *(int*)node->value != values[i])
        {
            all_correct = false;
            break;
        }
    }

    if (all_correct)
        PASS("All %d items verified", num);
    else
        FAIL("Some items incorrect");

    // Delete half
    for (int i = 0; i < num / 2; i++) hmap_delete(map, &keys[i]);

    if (hmap_size(map) == (usize)(num / 2))
        PASS("Size correct after deleting half (%zu)", hmap_size(map));
    else
        FAIL("Size incorrect after delete");

    // Verify remaining
    all_correct = true;
    for (int i = num / 2; i < num; i++)
    {
        hmap_node* node = hmap_get(map, &keys[i]);
        if (!node || *(int*)node->value != values[i])
        {
            all_correct = false;
            break;
        }
    }

    if (all_correct)
        PASS("Remaining items still correct");
    else
        FAIL("Some remaining items incorrect");

    // Verify deleted items are gone
    bool deleted_ok = true;
    for (int i = 0; i < num / 2; i++)
    {
        if (hmap_contains(map, &keys[i]))
        {
            deleted_ok = false;
            break;
        }
    }

    if (deleted_ok)
        PASS("Deleted items no longer accessible");
    else
        FAIL("Some deleted items still accessible");

    free(keys);
    free(values);
    hmap_destroy(map);
}

// ========== Test: hmap_node access pattern ==========
void test_hmap_node_access(void)
{
    printf("\n=== Test hmap_node_access ===\n");

    hmap* map = hmap_create(8, int_hash, int_compare);
    if (!map) { FAIL("hmap_create failed"); return; }

    int key = 10;
    int value = 42;
    hmap_node* inserted = hmap_insert(map, &key, &value);

    // Verify node fields
    if (inserted && inserted->key == &key)
        PASS("hmap_insert returns node with correct key pointer");
    else
        FAIL("Inserted node key incorrect");

    if (inserted && inserted->value == &value)
        PASS("hmap_insert returns node with correct value pointer");
    else
        FAIL("Inserted node value incorrect");

    // hmap_get returns same node
    hmap_node* found = hmap_get(map, &key);
    if (found == inserted)
        PASS("hmap_get returns same node as hmap_insert");
    else
        FAIL("hmap_get should return same node");

    // Modify value through node
    int new_value = 999;
    found->value = &new_value;
    hmap_node* refetch = hmap_get(map, &key);
    if (refetch && *(int*)refetch->value == 999)
        PASS("Value modification through node pointer works");
    else
        FAIL("Value modification through node failed");

    hmap_destroy(map);
}

// ========== Test: hmap_deinit ==========
void test_hmap_deinit(void)
{
    printf("\n=== Test hmap_deinit ===\n");

    // Stack-allocated hmap
    hmap map;
    hmap_init(&map, 8, int_hash, int_compare);

    int keys[] = {1, 2, 3};
    int values[] = {10, 20, 30};
    for (int i = 0; i < 3; i++) hmap_insert(&map, &keys[i], &values[i]);

    if (hmap_size(&map) == 3)
        PASS("Stack hmap: inserted 3 items");
    else
        FAIL("Stack hmap: size incorrect");

    hmap_deinit(&map);

    if (map.buckets == NULL && map.nbuckets == 0 && map.len == 0)
        PASS("hmap_deinit zeroes all fields");
    else
        FAIL("hmap_deinit should zero buckets/nbuckets/len");

    // Double deinit should be safe (buckets is NULL, nbuckets is 0)
    hmap_deinit(&map);
    PASS("Double hmap_deinit is safe");

    // NULL deinit
    hmap_deinit(NULL);
    PASS("hmap_deinit(NULL) is safe");
}

// ========== Test: stack-allocated hmap lifecycle ==========
void test_hmap_stack_lifecycle(void)
{
    printf("\n=== Test hmap_stack_lifecycle ===\n");

    // Full lifecycle: init -> insert -> get -> delete -> deinit
    hmap map;
    hmap_init(&map, 4, int_hash, int_compare);

    int keys[] = {10, 20, 30, 40, 50};
    int values[] = {100, 200, 300, 400, 500};
    for (int i = 0; i < 5; i++) hmap_insert(&map, &keys[i], &values[i]);

    // Verify grow works on stack hmap (5 items, 4 initial buckets, 75% threshold = 3)
    if (map.nbuckets > 4)
        PASS("Stack hmap grew (4 -> %zu buckets)", map.nbuckets);
    else
        FAIL("Stack hmap should have grown");

    // Get
    hmap_node* node = hmap_get(&map, &keys[2]);
    if (node && *(int*)node->value == 300)
        PASS("Stack hmap get works");
    else
        FAIL("Stack hmap get failed");

    // Delete
    int* del_val = (int*)hmap_delete(&map, &keys[0]);
    if (del_val && *del_val == 100 && hmap_size(&map) == 4)
        PASS("Stack hmap delete works");
    else
        FAIL("Stack hmap delete failed");

    // Contains after delete
    if (!hmap_contains(&map, &keys[0]) && hmap_contains(&map, &keys[1]))
        PASS("Stack hmap contains correct after delete");
    else
        FAIL("Stack hmap contains incorrect");

    hmap_deinit(&map);
    PASS("Stack hmap deinit after operations");
}

// ========== Test: hmap_init_str on stack ==========
void test_hmap_init_str_stack(void)
{
    printf("\n=== Test hmap_init_str_stack ===\n");

    hmap map;
    hmap_init_str(&map);

    const char* fruits[] = {"apple", "banana", "cherry", "date", "elderberry"};
    int prices[] = {3, 2, 5, 4, 8};
    for (int i = 0; i < 5; i++) hmap_insert(&map, fruits[i], &prices[i]);

    if (hmap_size(&map) == 5)
        PASS("Stack str-hmap: inserted 5 items");
    else
        FAIL("Stack str-hmap: size incorrect");

    // Lookup by string literal (different pointer, same content)
    hmap_node* n = hmap_get(&map, "cherry");
    if (n && *(int*)n->value == 5)
        PASS("Stack str-hmap: lookup by string literal works");
    else
        FAIL("Stack str-hmap: lookup failed");

    // Delete by string literal
    int* del = (int*)hmap_delete(&map, "banana");
    if (del && *del == 2 && hmap_size(&map) == 4)
        PASS("Stack str-hmap: delete by string literal works");
    else
        FAIL("Stack str-hmap: delete failed");

    hmap_deinit(&map);
    PASS("Stack str-hmap: deinit completed");
}

// ========== Test: NULL safety ==========
void test_hmap_null_safety(void)
{
    printf("\n=== Test hmap_null_safety ===\n");

    // hmap_init(NULL, ...) should not crash
    hmap_init(NULL, 8, int_hash, int_compare);
    PASS("hmap_init(NULL) is safe");

    // hmap_deinit(NULL) should not crash
    hmap_deinit(NULL);
    PASS("hmap_deinit(NULL) is safe");

    // hmap_destroy(NULL) should not crash
    hmap_destroy(NULL);
    PASS("hmap_destroy(NULL) is safe");

    // hmap_size(NULL) should return 0
    if (hmap_size(NULL) == 0)
        PASS("hmap_size(NULL) returns 0");
    else
        FAIL("hmap_size(NULL) should return 0");
}

// ========== Test: delete all then reinsert ==========
void test_hmap_delete_all_reinsert(void)
{
    printf("\n=== Test hmap_delete_all_reinsert ===\n");

    hmap* map = hmap_create(8, int_hash, int_compare);
    if (!map) { FAIL("hmap_create failed"); return; }

    int keys[] = {1, 2, 3, 4, 5};
    int values[] = {10, 20, 30, 40, 50};
    for (int i = 0; i < 5; i++) hmap_insert(map, &keys[i], &values[i]);

    // Delete all
    for (int i = 0; i < 5; i++) hmap_delete(map, &keys[i]);

    if (hmap_size(map) == 0)
        PASS("All entries deleted (size=0)");
    else
        FAIL("Size should be 0 after deleting all");

    // Verify none accessible
    bool none_found = true;
    for (int i = 0; i < 5; i++)
    {
        if (hmap_get(map, &keys[i]) != NULL)
        {
            none_found = false;
            break;
        }
    }
    if (none_found)
        PASS("No entries accessible after delete all");
    else
        FAIL("Some entries still accessible");

    // Reinsert
    int new_values[] = {100, 200, 300, 400, 500};
    for (int i = 0; i < 5; i++) hmap_insert(map, &keys[i], &new_values[i]);

    if (hmap_size(map) == 5)
        PASS("Reinserted 5 items after delete all");
    else
        FAIL("Reinsert size incorrect");

    bool all_correct = true;
    for (int i = 0; i < 5; i++)
    {
        hmap_node* node = hmap_get(map, &keys[i]);
        if (!node || *(int*)node->value != new_values[i])
        {
            all_correct = false;
            break;
        }
    }
    if (all_correct)
        PASS("Reinserted values correct");
    else
        FAIL("Reinserted values incorrect");

    hmap_destroy(map);
}

// ========== Test: checksum boundary sizes ==========
void test_checksum_boundary(void)
{
    printf("\n=== Test checksum_boundary ===\n");

    // Allocate aligned buffer large enough for all tests
    u8 buf[32];

    // Test sizes around the 8-byte boundary
    usize test_sizes[] = {1, 2, 3, 4, 5, 6, 7, 8, 9, 15, 16, 17, 24, 25};
    int num_sizes = sizeof(test_sizes) / sizeof(test_sizes[0]);

    bool all_deterministic = true;
    for (int i = 0; i < num_sizes; i++)
    {
        memset(buf, 0xAA, sizeof(buf));
        u64 c1 = checksum(buf, test_sizes[i]);
        u64 c2 = checksum(buf, test_sizes[i]);
        if (c1 != c2)
        {
            all_deterministic = false;
            FAIL("Checksum not deterministic for size %zu", test_sizes[i]);
            break;
        }
    }
    if (all_deterministic)
        PASS("Checksum deterministic for all boundary sizes (1-%zu)", test_sizes[num_sizes - 1]);

    // Different sizes should (generally) produce different checksums
    memset(buf, 0xBB, sizeof(buf));
    u64 cs8 = checksum(buf, 8);
    u64 cs9 = checksum(buf, 9);
    u64 cs16 = checksum(buf, 16);
    if (cs8 != cs9 && cs8 != cs16)
        PASS("Different sizes produce different checksums");
    else
        FAIL("Different sizes should produce different checksums");

    // Exact 8-byte multiple (only u64 path, no remainder)
    memset(buf, 0xCC, 16);
    u64 cs_exact = checksum(buf, 16);
    buf[0] ^= 0xFF;  // flip byte in lower 4 of first chunk
    u64 cs_flipped = checksum(buf, 16);
    if (cs_exact != cs_flipped)
        PASS("Exact 8-byte multiple: detects change in first chunk");
    else
        FAIL("Exact 8-byte multiple: should detect change");

    // Remainder-only (size < 8, uses Hash_u8 path)
    u8 small1[4] = {0x01, 0x02, 0x03, 0x04};
    u8 small2[4] = {0x01, 0x02, 0x03, 0x05};
    u64 cs_s1 = checksum(small1, 4);
    u64 cs_s2 = checksum(small2, 4);
    if (cs_s1 != cs_s2)
        PASS("Remainder path (size<8): detects 1-byte difference");
    else
        FAIL("Remainder path: should detect difference");
}

// ========== Test: delete head/middle/tail of chain ==========
void test_hmap_delete_chain_positions(void)
{
    printf("\n=== Test hmap_delete_chain_positions ===\n");

    // Use 1 bucket to force all into same chain
    hmap* map = hmap_create(1, int_hash, int_compare);
    if (!map) { FAIL("hmap_create failed"); return; }

    int keys[] = {1, 2, 3, 4, 5};
    int values[] = {10, 20, 30, 40, 50};
    for (int i = 0; i < 5; i++) hmap_insert(map, &keys[i], &values[i]);

    // Delete from head of chain (most recently inserted = key 5)
    int* d5 = (int*)hmap_delete(map, &keys[4]);
    if (d5 && *d5 == 50 && hmap_size(map) == 4)
        PASS("Delete chain head (key=5) works");
    else
        FAIL("Delete chain head failed");

    // Delete from middle of chain (key 3)
    int* d3 = (int*)hmap_delete(map, &keys[2]);
    if (d3 && *d3 == 30 && hmap_size(map) == 3)
        PASS("Delete chain middle (key=3) works");
    else
        FAIL("Delete chain middle failed");

    // Delete from tail of chain (first inserted = key 1)
    int* d1 = (int*)hmap_delete(map, &keys[0]);
    if (d1 && *d1 == 10 && hmap_size(map) == 2)
        PASS("Delete chain tail (key=1) works");
    else
        FAIL("Delete chain tail failed");

    // Remaining keys still accessible
    hmap_node* n2 = hmap_get(map, &keys[1]);
    hmap_node* n4 = hmap_get(map, &keys[3]);
    if (n2 && *(int*)n2->value == 20 && n4 && *(int*)n4->value == 40)
        PASS("Remaining chain entries intact");
    else
        FAIL("Remaining chain entries corrupted");

    hmap_destroy(map);
}

// ========== Test: insert returns existing node on update ==========
void test_hmap_insert_return_semantics(void)
{
    printf("\n=== Test hmap_insert_return_semantics ===\n");

    hmap* map = hmap_create(8, int_hash, int_compare);
    if (!map) { FAIL("hmap_create failed"); return; }

    int key = 42;
    int val1 = 100, val2 = 200, val3 = 300;

    hmap_node* n1 = hmap_insert(map, &key, &val1);
    hmap_node* n2 = hmap_insert(map, &key, &val2);
    hmap_node* n3 = hmap_insert(map, &key, &val3);

    if (n1 == n2 && n2 == n3)
        PASS("Repeated insert returns same node");
    else
        FAIL("Repeated insert should return same node");

    if (n3->value == &val3)
        PASS("Node value updated to latest (%d)", *(int*)n3->value);
    else
        FAIL("Node value should be latest");

    if (hmap_size(map) == 1)
        PASS("Size stays 1 after 3 inserts of same key");
    else
        FAIL("Size should be 1");

    hmap_destroy(map);
}

// ========== Test: large key count with delete-reinsert churn ==========
void test_hmap_churn(void)
{
    printf("\n=== Test hmap_churn ===\n");

    hmap* map = hmap_create(4, int_hash, int_compare);
    if (!map) { FAIL("hmap_create failed"); return; }

    const int n = 200;
    int* keys = malloc(sizeof(int) * n);
    int* values = malloc(sizeof(int) * n);

    // Insert n items
    for (int i = 0; i < n; i++)
    {
        keys[i] = i;
        values[i] = i * 10;
        hmap_insert(map, &keys[i], &values[i]);
    }

    // Delete even keys
    for (int i = 0; i < n; i += 2) hmap_delete(map, &keys[i]);

    if (hmap_size(map) == (usize)(n / 2))
        PASS("Deleted %d even keys (size=%zu)", n / 2, hmap_size(map));
    else
        FAIL("Size incorrect after deleting evens");

    // Reinsert even keys with new values
    int* new_values = malloc(sizeof(int) * n);
    for (int i = 0; i < n; i += 2)
    {
        new_values[i] = i * 100;
        hmap_insert(map, &keys[i], &new_values[i]);
    }

    if (hmap_size(map) == (usize)n)
        PASS("Reinserted even keys (size=%zu)", hmap_size(map));
    else
        FAIL("Size incorrect after reinsert");

    // Verify all: odd keys have original values, even keys have new values
    bool all_ok = true;
    for (int i = 0; i < n; i++)
    {
        hmap_node* node = hmap_get(map, &keys[i]);
        int expected = (i % 2 == 0) ? i * 100 : i * 10;
        if (!node || *(int*)node->value != expected)
        {
            all_ok = false;
            break;
        }
    }
    if (all_ok)
        PASS("All %d entries correct after churn", n);
    else
        FAIL("Some entries incorrect after churn");

    free(keys);
    free(values);
    free(new_values);
    hmap_destroy(map);
}

// ========== Test: iterator init state ==========
void test_hmap_iter_init_state(void)
{
    printf("\n=== Test hmap_iter_init_state ===\n");

    hmap map;
    hmap_init(&map, 8, int_hash, int_compare);

    hmap_iterator iter;
    hmap_iter_init(&iter, &map);

    if (iter.hmap == &map)
        PASS("iter.hmap points to map");
    else
        FAIL("iter.hmap incorrect");

    if (iter.bucket_idx == 0)
        PASS("iter.bucket_idx initialized to 0");
    else
        FAIL("iter.bucket_idx should be 0");

    if (iter.node == NULL)
        PASS("iter.node initialized to NULL");
    else
        FAIL("iter.node should be NULL");

    // key/value before first next() should be NULL
    if (hmap_iter_key(&iter) == NULL)
        PASS("Key before first next() is NULL");
    else
        FAIL("Key before first next() should be NULL");

    if (hmap_iter_value(&iter) == NULL)
        PASS("Value before first next() is NULL");
    else
        FAIL("Value before first next() should be NULL");

    hmap_deinit(&map);
}

// ========== Test: iterator on empty map ==========
void test_hmap_iter_empty(void)
{
    printf("\n=== Test hmap_iter_empty ===\n");

    hmap* map = hmap_create(8, int_hash, int_compare);
    if (!map) { FAIL("hmap_create failed"); return; }

    hmap_iterator iter;
    hmap_iter_init(&iter, map);

    int count = 0;
    while (hmap_iter_next(&iter)) count++;

    if (count == 0)
        PASS("Empty map: iterator yields 0 items");
    else
        FAIL("Empty map: expected 0, got %d", count);

    // key/value on exhausted iterator
    if (hmap_iter_key(&iter) == NULL)
        PASS("Exhausted iterator: key returns NULL");
    else
        FAIL("Exhausted iterator: key should return NULL");

    if (hmap_iter_value(&iter) == NULL)
        PASS("Exhausted iterator: value returns NULL");
    else
        FAIL("Exhausted iterator: value should return NULL");

    hmap_destroy(map);
}

// ========== Test: iterator single element ==========
void test_hmap_iter_single(void)
{
    printf("\n=== Test hmap_iter_single ===\n");

    hmap* map = hmap_create(8, int_hash, int_compare);
    if (!map) { FAIL("hmap_create failed"); return; }

    int key = 42;
    int value = 100;
    hmap_insert(map, &key, &value);

    hmap_iterator iter;
    hmap_iter_init(&iter, map);

    int count = 0;
    const int* got_key = NULL;
    int* got_value = NULL;
    while (hmap_iter_next(&iter))
    {
        got_key = (const int*)hmap_iter_key(&iter);
        got_value = (int*)hmap_iter_value(&iter);
        count++;
    }

    if (count == 1)
        PASS("Single item: iterator yields 1 item");
    else
        FAIL("Single item: expected 1, got %d", count);

    if (got_key && *got_key == 42)
        PASS("Single item: key is 42");
    else
        FAIL("Single item: key incorrect");

    if (got_value && *got_value == 100)
        PASS("Single item: value is 100");
    else
        FAIL("Single item: value incorrect (hmap_iter_value may return node instead of value)");

    hmap_destroy(map);
}

// ========== Test: iterator visits all entries ==========
void test_hmap_iter_all_entries(void)
{
    printf("\n=== Test hmap_iter_all_entries ===\n");

    hmap* map = hmap_create(16, int_hash, int_compare);
    if (!map) { FAIL("hmap_create failed"); return; }

    const int N = 10;
    int keys[10], values[10];
    for (int i = 0; i < N; i++)
    {
        keys[i] = i;
        values[i] = i * 10;
        hmap_insert(map, &keys[i], &values[i]);
    }

    hmap_iterator iter;
    hmap_iter_init(&iter, map);

    bool seen[10] = {false};
    int count = 0;
    while (hmap_iter_next(&iter))
    {
        const int* k = (const int*)hmap_iter_key(&iter);
        if (k)
        {
            for (int i = 0; i < N; i++)
            {
                if (*k == keys[i])
                {
                    seen[i] = true;
                    break;
                }
            }
        }
        count++;
    }

    if (count == N)
        PASS("Iterator visited exactly %d entries", N);
    else
        FAIL("Iterator visited %d entries, expected %d", count, N);

    bool all_seen = true;
    for (int i = 0; i < N; i++)
    {
        if (!seen[i]) { all_seen = false; break; }
    }
    if (all_seen)
        PASS("All %d keys seen during iteration", N);
    else
        FAIL("Some keys not seen during iteration");

    hmap_destroy(map);
}

// ========== Test: iterator with collisions (all in same bucket) ==========
void test_hmap_iter_collisions(void)
{
    printf("\n=== Test hmap_iter_collisions ===\n");

    // Use always_zero_hash: all items forced into bucket 0
    hmap* map = hmap_create(16, always_zero_hash, int_compare);
    if (!map) { FAIL("hmap_create failed"); return; }

    const int N = 5;
    int keys[5], values[5];
    for (int i = 0; i < N; i++)
    {
        keys[i] = i + 1;
        values[i] = (i + 1) * 10;
        hmap_insert(map, &keys[i], &values[i]);
    }

    // Verify all items in a single bucket chain
    hmap_iterator iter;
    hmap_iter_init(&iter, map);

    bool seen[5] = {false};
    int count = 0;
    while (hmap_iter_next(&iter))
    {
        const int* k = (const int*)hmap_iter_key(&iter);
        if (k)
        {
            for (int i = 0; i < N; i++)
            {
                if (*k == keys[i])
                {
                    seen[i] = true;
                    break;
                }
            }
        }
        count++;
    }

    if (count == N)
        PASS("Collision chain: visited all %d entries", N);
    else
        FAIL("Collision chain: visited %d, expected %d (iter_next may skip chain nodes)", count, N);

    bool all_seen = true;
    for (int i = 0; i < N; i++)
    {
        if (!seen[i]) { all_seen = false; break; }
    }
    if (all_seen)
        PASS("Collision chain: all keys seen");
    else
        FAIL("Collision chain: some keys missed");

    hmap_destroy(map);
}

// ========== Test: iterator key/value with string keys ==========
void test_hmap_iter_string_keys(void)
{
    printf("\n=== Test hmap_iter_string_keys ===\n");

    hmap* map = hmap_create(16, string_hash, string_compare);
    if (!map) { FAIL("hmap_create failed"); return; }

    const char* keys[] = {"apple", "banana", "cherry"};
    int values[] = {1, 2, 3};
    for (int i = 0; i < 3; i++) hmap_insert(map, keys[i], &values[i]);

    hmap_iterator iter;
    hmap_iter_init(&iter, map);

    bool seen[3] = {false};
    int count = 0;
    while (hmap_iter_next(&iter))
    {
        const char* k = (const char*)hmap_iter_key(&iter);
        int* v = (int*)hmap_iter_value(&iter);
        if (!k || !v) { count++; continue; }

        for (int i = 0; i < 3; i++)
        {
            if (strcmp(k, keys[i]) == 0)
            {
                if (*v == values[i]) seen[i] = true;
                break;
            }
        }
        count++;
    }

    if (count == 3)
        PASS("String iter: visited 3 entries");
    else
        FAIL("String iter: visited %d, expected 3", count);

    bool all_ok = true;
    for (int i = 0; i < 3; i++)
    {
        if (!seen[i]) { all_ok = false; break; }
    }
    if (all_ok)
        PASS("String iter: all key-value pairs correct");
    else
        FAIL("String iter: some key-value pairs incorrect (hmap_iter_value may return node)");

    hmap_destroy(map);
}

// ========== Test: iterator after deletions ==========
void test_hmap_iter_after_delete(void)
{
    printf("\n=== Test hmap_iter_after_delete ===\n");

    hmap* map = hmap_create(16, int_hash, int_compare);
    if (!map) { FAIL("hmap_create failed"); return; }

    int keys[10], values[10];
    for (int i = 0; i < 10; i++)
    {
        keys[i] = i;
        values[i] = i * 10;
        hmap_insert(map, &keys[i], &values[i]);
    }

    // Delete even keys
    for (int i = 0; i < 10; i += 2) hmap_delete(map, &keys[i]);

    hmap_iterator iter;
    hmap_iter_init(&iter, map);

    int count = 0;
    bool only_odd = true;
    while (hmap_iter_next(&iter))
    {
        const int* k = (const int*)hmap_iter_key(&iter);
        if (k && (*k % 2 == 0)) only_odd = false;
        count++;
    }

    if (count == 5)
        PASS("After delete: visited 5 remaining entries");
    else
        FAIL("After delete: visited %d, expected 5", count);

    if (only_odd)
        PASS("After delete: only odd keys remain");
    else
        FAIL("After delete: found deleted even key");

    hmap_destroy(map);
}

// ========== Test: iterator re-init ==========
void test_hmap_iter_reinit(void)
{
    printf("\n=== Test hmap_iter_reinit ===\n");

    hmap* map = hmap_create(8, int_hash, int_compare);
    if (!map) { FAIL("hmap_create failed"); return; }

    int keys[] = {1, 2, 3};
    int values[] = {10, 20, 30};
    for (int i = 0; i < 3; i++) hmap_insert(map, &keys[i], &values[i]);

    // First iteration
    hmap_iterator iter;
    hmap_iter_init(&iter, map);
    int count1 = 0;
    while (hmap_iter_next(&iter)) count1++;

    // Re-init and iterate again
    hmap_iter_init(&iter, map);
    int count2 = 0;
    while (hmap_iter_next(&iter)) count2++;

    if (count1 == 3 && count2 == 3)
        PASS("Re-init: both iterations yield 3");
    else
        FAIL("Re-init: first=%d, second=%d, expected 3 each", count1, count2);

    hmap_destroy(map);
}

// ========== Test: iterator large map ==========
void test_hmap_iter_large(void)
{
    printf("\n=== Test hmap_iter_large ===\n");

    hmap* map = hmap_create(8, int_hash, int_compare);
    if (!map) { FAIL("hmap_create failed"); return; }

    const int N = 500;
    int* keys = malloc(sizeof(int) * N);
    int* values = malloc(sizeof(int) * N);
    for (int i = 0; i < N; i++)
    {
        keys[i] = i;
        values[i] = i * 3;
        hmap_insert(map, &keys[i], &values[i]);
    }

    hmap_iterator iter;
    hmap_iter_init(&iter, map);

    int count = 0;
    while (hmap_iter_next(&iter)) count++;

    if ((usize)count == hmap_size(map))
        PASS("Large map: iter count (%d) matches hmap_size", count);
    else
        FAIL("Large map: iter count %d != hmap_size %zu", count, hmap_size(map));

    // Verify all keys visited (use a seen-bitmap)
    bool* seen = calloc(N, sizeof(bool));
    hmap_iter_init(&iter, map);
    while (hmap_iter_next(&iter))
    {
        const int* k = (const int*)hmap_iter_key(&iter);
        if (k && *k >= 0 && *k < N) seen[*k] = true;
    }
    bool all_seen = true;
    for (int i = 0; i < N; i++)
    {
        if (!seen[i]) { all_seen = false; break; }
    }
    if (all_seen)
        PASS("Large map: all %d keys visited", N);
    else
        FAIL("Large map: some keys missed");

    free(seen);
    free(keys);
    free(values);
    hmap_destroy(map);
}

// ========== Test: iterator no duplicate visits ==========
void test_hmap_iter_no_duplicates(void)
{
    printf("\n=== Test hmap_iter_no_duplicates ===\n");

    hmap* map = hmap_create(4, int_hash, int_compare);
    if (!map) { FAIL("hmap_create failed"); return; }

    const int N = 20;
    int keys[20], values[20];
    for (int i = 0; i < N; i++)
    {
        keys[i] = i;
        values[i] = i;
        hmap_insert(map, &keys[i], &values[i]);
    }

    hmap_iterator iter;
    hmap_iter_init(&iter, map);

    int visit_count[20] = {0};
    int count = 0;
    while (hmap_iter_next(&iter))
    {
        const int* k = (const int*)hmap_iter_key(&iter);
        if (k && *k >= 0 && *k < N) visit_count[*k]++;
        count++;
    }

    bool no_dups = true;
    for (int i = 0; i < N; i++)
    {
        if (visit_count[i] > 1) { no_dups = false; break; }
    }
    if (no_dups && count == N)
        PASS("No duplicates: each key visited exactly once (%d items)", N);
    else
        FAIL("Duplicates detected or count wrong (%d)", count);

    hmap_destroy(map);
}

int main(void)
{
    printf("========================================\n");
    printf("   HashMap Test Suite\n");
    printf("========================================\n");

    test_checksum();
    test_checksum_boundary();
    test_hmap_create_destroy();
    test_hmap_init();
    test_hmap_init_str();
    test_hmap_deinit();
    test_hmap_stack_lifecycle();
    test_hmap_init_str_stack();
    test_hmap_null_safety();
    test_hmap_insert_get();
    test_hmap_insert_return_semantics();
    test_hmap_contains();
    test_hmap_delete();
    test_hmap_delete_all_reinsert();
    test_hmap_delete_chain_positions();
    test_hmap_update();
    test_hmap_string_keys();
    test_hmap_collisions();
    test_hmap_grow();
    test_hmap_stress();
    test_hmap_churn();
    test_hmap_node_access();

    // Iterator tests
    test_hmap_iter_init_state();
    test_hmap_iter_empty();
    test_hmap_iter_single();
    test_hmap_iter_all_entries();
    test_hmap_iter_collisions();
    test_hmap_iter_string_keys();
    test_hmap_iter_after_delete();
    test_hmap_iter_reinit();
    test_hmap_iter_large();
    test_hmap_iter_no_duplicates();

    printf("\n========================================\n");
    printf("   Results: %d passed, %d failed\n", pass_count, fail_count);
    printf("========================================\n\n");

    return fail_count > 0 ? 1 : 0;
}
