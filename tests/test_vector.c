#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "../src/vector.h"

static int pass_count = 0;
static int fail_count = 0;

#define PASS(msg, ...)                            \
    do                                            \
    {                                             \
        pass_count++;                             \
        printf("[PASS] " msg "\n", ##__VA_ARGS__); \
    } while (0)

#define FAIL(msg, ...)                            \
    do                                            \
    {                                             \
        fail_count++;                             \
        printf("[FAIL] " msg "\n", ##__VA_ARGS__); \
    } while (0)

int int_compare(const void* a, const void* b)
{
    return *(int*)a - *(int*)b;
}

typedef struct
{
    int id;
    char name[32];
    double value;
} TestStruct;

// ========== Test: vector_create / vector_destroy ==========
void test_vector_create_destroy(void)
{
    printf("\n=== Test vector_create_destroy ===\n");

    Vector* int_vec = vector_create(sizeof(int), 0);
    if (!int_vec)
    {
        FAIL("Vector creation failed");
        return;
    }

    if (vector_element_size(int_vec) == sizeof(int) && vector_size(int_vec) == 0 &&
        vector_capacity(int_vec) > 0 && vector_empty(int_vec))
    {
        PASS("Int Vector initialization correct");
    }
    else
    {
        FAIL("Int Vector initialization incorrect");
    }

    vector_destroy(int_vec);

    Vector* struct_vec = vector_create(sizeof(TestStruct), 8);
    if (struct_vec && vector_element_size(struct_vec) == sizeof(TestStruct) &&
        vector_capacity(struct_vec) == 8)
    {
        PASS("Struct Vector creation with explicit capacity");
    }
    else
    {
        FAIL("Struct Vector creation failed");
    }

    vector_destroy(struct_vec);
}

// ========== Test: vector_create with element_size=0 ==========
void test_vector_create_zero_element(void)
{
    printf("\n=== Test vector_create_zero_element ===\n");

    Vector* vec = vector_create(0, 0);
    if (vec == NULL)
    {
        PASS("vector_create(0, 0) returns NULL");
    }
    else
    {
        FAIL("vector_create(0, 0) should return NULL");
        vector_destroy(vec);
    }
}

// ========== Test: vector_init / vector_deinit (embedded) ==========
void test_vector_init_deinit(void)
{
    printf("\n=== Test vector_init_deinit (embedded) ===\n");

    Vector vec;
    int result = Vector_init(&vec, sizeof(int), 0);
    if (result == 0 && vec.data != NULL && vec.size == 0 && vec.capacity > 0)
    {
        PASS("vector_init succeeded for embedded vector");
    }
    else
    {
        FAIL("vector_init failed");
        return;
    }

    // Push some data
    int val = 42;
    vector_push_back(&vec, &val);
    val = 99;
    vector_push_back(&vec, &val);

    if (vector_size(&vec) == 2)
    {
        PASS("Embedded vector push_back works (size=%zu)", vector_size(&vec));
    }
    else
    {
        FAIL("Embedded vector push_back failed");
    }

    // Verify data
    int* p = (int*)vector_get(&vec, 0);
    if (p && *p == 42)
    {
        PASS("Embedded vector data accessible");
    }
    else
    {
        FAIL("Embedded vector data incorrect");
    }

    // Deinit
    vector_deinit(&vec);
    if (vec.data == NULL && vec.size == 0 && vec.capacity == 0)
    {
        PASS("vector_deinit clears all fields");
    }
    else
    {
        FAIL("vector_deinit did not clear fields properly");
    }

    // Double deinit should be safe
    vector_deinit(&vec);
    PASS("Double vector_deinit is safe");
}

// ========== Test: vector_reserve ==========
void test_vector_reserve(void)
{
    printf("\n=== Test vector_reserve ===\n");

    Vector* vec = vector_create(sizeof(int), 4);
    if (!vec)
    {
        FAIL("Vector creation failed");
        return;
    }

    usize original_cap = vector_capacity(vec);
    if (original_cap == 4)
    {
        PASS("Initial capacity is 4");
    }
    else
    {
        FAIL("Initial capacity expected 4, got %zu", original_cap);
    }

    // Reserve larger
    if (vector_reserve(vec, 100) == 0 && vector_capacity(vec) >= 100)
    {
        PASS("vector_reserve(100) succeeded, capacity=%zu", vector_capacity(vec));
    }
    else
    {
        FAIL("vector_reserve(100) failed");
    }

    // Reserve smaller (should be no-op)
    usize cap_before = vector_capacity(vec);
    if (vector_reserve(vec, 10) == 0 && vector_capacity(vec) == cap_before)
    {
        PASS("vector_reserve with smaller value is no-op");
    }
    else
    {
        FAIL("vector_reserve with smaller value should be no-op");
    }

    // Reserve on NULL
    if (vector_reserve(NULL, 10) == -1)
    {
        PASS("vector_reserve(NULL) returns -1");
    }
    else
    {
        FAIL("vector_reserve(NULL) should return -1");
    }

    // Push data after reserve and verify
    for (int i = 0; i < 50; i++)
    {
        vector_push_back(vec, &i);
    }
    bool data_ok = true;
    for (int i = 0; i < 50; i++)
    {
        int* p = (int*)vector_get(vec, i);
        if (!p || *p != i) { data_ok = false; break; }
    }
    if (data_ok)
    {
        PASS("Data intact after reserve + push_back");
    }
    else
    {
        FAIL("Data corrupted after reserve + push_back");
    }

    vector_destroy(vec);
}

// ========== Test: push_back and pop_back (value copy) ==========
void test_vector_push_pop(void)
{
    printf("\n=== Test vector_push_pop ===\n");

    Vector* vec = vector_create(sizeof(int), 4);
    if (!vec) { FAIL("Vector creation failed"); return; }

    int values[] = {10, 20, 30, 40, 50};
    int num = 5;

    for (int i = 0; i < num; i++)
    {
        if (vector_push_back(vec, &values[i]) != 0)
        {
            FAIL("push_back failed for %d", values[i]);
            vector_destroy(vec);
            return;
        }
    }
    PASS("Pushed %d values (size=%zu, capacity=%zu)", num, vector_size(vec), vector_capacity(vec));

    // Verify value copy semantics
    for (int i = 0; i < num; i++) values[i] = 999;

    bool unchanged = true;
    int expected[] = {10, 20, 30, 40, 50};
    for (usize i = 0; i < vector_size(vec); i++)
    {
        int* val = (int*)vector_get(vec, i);
        if (!val || *val != expected[i]) { unchanged = false; break; }
    }

    if (unchanged)
    {
        PASS("Values are copied (not referenced)");
    }
    else
    {
        FAIL("Values should be independent copies");
    }

    // Pop all
    int popped;
    for (int i = num - 1; i >= 0; i--)
    {
        if (vector_pop_back(vec, &popped) != 0 || popped != expected[i])
        {
            FAIL("pop_back returned wrong value");
            vector_destroy(vec);
            return;
        }
    }

    if (vector_empty(vec) && vector_size(vec) == 0)
    {
        PASS("All values popped correctly");
    }
    else
    {
        FAIL("Vector not empty after popping all");
    }

    // Pop from empty vector
    if (vector_pop_back(vec, &popped) == -1)
    {
        PASS("Pop from empty vector returns -1");
    }
    else
    {
        FAIL("Pop from empty vector should return -1");
    }

    vector_destroy(vec);
}

// ========== Test: get and set ==========
void test_vector_get_set(void)
{
    printf("\n=== Test vector_get_set ===\n");

    Vector* vec = vector_create(sizeof(int), 0);
    if (!vec) { FAIL("Vector creation failed"); return; }

    int data[] = {100, 200, 300, 400, 500};
    for (int i = 0; i < 5; i++) vector_push_back(vec, &data[i]);

    // Test get
    bool all_correct = true;
    for (int i = 0; i < 5; i++)
    {
        int* val = VECTOR_GET(vec, i, int);
        if (!val || *val != data[i]) { all_correct = false; break; }
    }
    if (all_correct) PASS("All get operations correct");
    else FAIL("Some get operations failed");

    // Test out of bounds get
    if (vector_get(vec, 100) == NULL)
    {
        PASS("Out-of-bounds get returns NULL");
    }
    else
    {
        FAIL("Out-of-bounds get should return NULL");
    }

    // Test set
    int new_value = 999;
    if (vector_set(vec, 2, &new_value) == 0)
    {
        int* val = (int*)vector_get(vec, 2);
        if (val && *val == 999) PASS("Set operation works");
        else FAIL("Set verification failed");
    }
    else
    {
        FAIL("Set operation failed");
    }

    // Test out of bounds set
    if (vector_set(vec, 100, &new_value) == -1)
    {
        PASS("Out-of-bounds set returns -1");
    }
    else
    {
        FAIL("Out-of-bounds set should return -1");
    }

    // Test get_copy
    int copied_value;
    if (vector_get_copy(vec, 0, &copied_value) == 0 && copied_value == 100)
    {
        PASS("Get_copy works");
    }
    else
    {
        FAIL("Get_copy failed");
    }

    // Test get_copy out of bounds
    if (vector_get_copy(vec, 100, &copied_value) == -1)
    {
        PASS("Out-of-bounds get_copy returns -1");
    }
    else
    {
        FAIL("Out-of-bounds get_copy should return -1");
    }

    vector_destroy(vec);
}

// ========== Test: struct storage ==========
void test_vector_struct(void)
{
    printf("\n=== Test vector_struct ===\n");

    Vector* vec = vector_create(sizeof(TestStruct), 0);
    if (!vec) { FAIL("Vector creation failed"); return; }

    TestStruct s1 = {1, "Alice", 95.5};
    TestStruct s2 = {2, "Bob", 87.3};
    TestStruct s3 = {3, "Charlie", 92.1};

    vector_push_back(vec, &s1);
    vector_push_back(vec, &s2);
    vector_push_back(vec, &s3);

    if (vector_size(vec) == 3) PASS("Added 3 structs");
    else FAIL("Size incorrect");

    // Modify original
    s1.id = 999;
    strcpy(s1.name, "Modified");

    TestStruct* stored = (TestStruct*)vector_get(vec, 0);
    if (stored && stored->id == 1 && strcmp(stored->name, "Alice") == 0)
    {
        PASS("Struct data is copied (value semantics)");
    }
    else
    {
        FAIL("Struct data should be independent");
    }

    vector_destroy(vec);
}

// ========== Test: insert and erase ==========
void test_vector_insert_erase(void)
{
    printf("\n=== Test vector_insert_erase ===\n");

    Vector* vec = vector_create(sizeof(int), 0);
    if (!vec) { FAIL("Vector creation failed"); return; }

    int data[] = {1, 2, 4, 5};
    for (int i = 0; i < 4; i++) vector_push_back(vec, &data[i]);

    // Insert 3 at index 2
    int three = 3;
    if (vector_insert(vec, 2, &three) == 0)
    {
        int expected[] = {1, 2, 3, 4, 5};
        bool ok = true;
        for (int i = 0; i < 5; i++)
        {
            int* v = (int*)vector_get(vec, i);
            if (!v || *v != expected[i]) { ok = false; break; }
        }
        if (ok) PASS("Insert at middle works");
        else FAIL("Insert at middle produced wrong data");
    }
    else
    {
        FAIL("Insert operation failed");
    }

    // Insert at beginning
    int zero = 0;
    if (vector_insert(vec, 0, &zero) == 0)
    {
        int* v = (int*)vector_get(vec, 0);
        if (v && *v == 0) PASS("Insert at beginning works");
        else FAIL("Insert at beginning failed");
    }

    // Insert at end (append)
    int six = 6;
    if (vector_insert(vec, vector_size(vec), &six) == 0)
    {
        int* v = (int*)vector_back(vec);
        if (v && *v == 6) PASS("Insert at end works");
        else FAIL("Insert at end failed");
    }

    // Insert beyond size
    if (vector_insert(vec, vector_size(vec) + 10, &six) == -1)
    {
        PASS("Insert beyond size returns -1");
    }
    else
    {
        FAIL("Insert beyond size should return -1");
    }

    // Erase
    int erased;
    usize size_before = vector_size(vec);
    if (vector_erase(vec, 0, &erased) == 0 && erased == 0)
    {
        if (vector_size(vec) == size_before - 1)
        {
            PASS("Erase at beginning works");
        }
        else
        {
            FAIL("Erase size mismatch");
        }
    }
    else
    {
        FAIL("Erase operation failed");
    }

    // Erase out of bounds
    if (vector_erase(vec, 999, NULL) == -1)
    {
        PASS("Erase out of bounds returns -1");
    }
    else
    {
        FAIL("Erase out of bounds should return -1");
    }

    vector_destroy(vec);
}

// ========== Test: clear and resize ==========
void test_vector_clear_resize(void)
{
    printf("\n=== Test vector_clear_resize ===\n");

    Vector* vec = vector_create(sizeof(int), 0);
    if (!vec) { FAIL("Vector creation failed"); return; }

    int data[] = {1, 2, 3, 4, 5};
    for (int i = 0; i < 5; i++) vector_push_back(vec, &data[i]);

    // Test clear
    vector_clear(vec);
    if (vector_empty(vec) && vector_size(vec) == 0)
    {
        PASS("Clear works");
    }
    else
    {
        FAIL("Clear failed");
    }

    // Resize with zero-init
    if (vector_resize(vec, 5, NULL) == 0)
    {
        bool all_zero = true;
        for (usize i = 0; i < 5; i++)
        {
            int val = *(int*)vector_get(vec, i);
            if (val != 0) { all_zero = false; break; }
        }
        if (all_zero) PASS("Resize with zero-init works");
        else FAIL("Resize zero-init failed");
    }
    else
    {
        FAIL("Resize failed");
    }

    // Resize with default value
    int default_val = 42;
    if (vector_resize(vec, 8, &default_val) == 0)
    {
        bool ok = true;
        for (usize i = 5; i < 8; i++)
        {
            int val = *(int*)vector_get(vec, i);
            if (val != 42) { ok = false; break; }
        }
        if (ok) PASS("Resize with default value works");
        else FAIL("Resize default value failed");
    }

    // Resize to shrink
    if (vector_resize(vec, 3, NULL) == 0 && vector_size(vec) == 3)
    {
        PASS("Resize to shrink works (size=%zu)", vector_size(vec));
    }
    else
    {
        FAIL("Resize to shrink failed");
    }

    vector_destroy(vec);
}

// ========== Test: find ==========
void test_vector_find(void)
{
    printf("\n=== Test vector_find ===\n");

    Vector* vec = vector_create(sizeof(int), 0);
    if (!vec) { FAIL("Vector creation failed"); return; }

    int data[] = {10, 20, 30, 40, 50};
    for (int i = 0; i < 5; i++) vector_push_back(vec, &data[i]);

    int target = 30;
    if (vector_find(vec, &target, int_compare) == 2)
    {
        PASS("Found 30 at index 2");
    }
    else
    {
        FAIL("Find failed for existing element");
    }

    int not_found = 99;
    if (vector_find(vec, &not_found, int_compare) == -1)
    {
        PASS("Returns -1 for non-existent element");
    }
    else
    {
        FAIL("Find should return -1");
    }

    // Find with NULL args
    if (vector_find(NULL, &target, int_compare) == -1)
    {
        PASS("Find with NULL vec returns -1");
    }
    else
    {
        FAIL("Find with NULL vec should return -1");
    }

    if (vector_find(vec, &target, NULL) == -1)
    {
        PASS("Find with NULL compare returns -1");
    }
    else
    {
        FAIL("Find with NULL compare should return -1");
    }

    vector_destroy(vec);
}

// ========== Test: front, back, data ==========
void test_vector_access(void)
{
    printf("\n=== Test vector_access (front/back/data) ===\n");

    Vector* vec = vector_create(sizeof(int), 0);
    if (!vec) { FAIL("Vector creation failed"); return; }

    // Empty vector
    if (vector_front(vec) == NULL && vector_back(vec) == NULL)
    {
        PASS("Empty vector: front and back return NULL");
    }
    else
    {
        FAIL("Empty vector should return NULL for front/back");
    }

    int data[] = {100, 200, 300};
    for (int i = 0; i < 3; i++) vector_push_back(vec, &data[i]);

    int* front = VECTOR_FRONT(vec, int);
    int* back = VECTOR_BACK(vec, int);

    if (front && *front == 100) PASS("Front element: %d", *front);
    else FAIL("Front element incorrect");

    if (back && *back == 300) PASS("Back element: %d", *back);
    else FAIL("Back element incorrect");

    int* raw_data = (int*)vector_data(vec);
    if (raw_data && raw_data[0] == 100 && raw_data[1] == 200 && raw_data[2] == 300)
    {
        PASS("vector_data returns correct pointer");
    }
    else
    {
        FAIL("vector_data failed");
    }

    // NULL vector
    if (vector_front(NULL) == NULL) PASS("front(NULL) returns NULL");
    else FAIL("front(NULL) should return NULL");

    if (vector_back(NULL) == NULL) PASS("back(NULL) returns NULL");
    else FAIL("back(NULL) should return NULL");

    if (vector_data(NULL) == NULL) PASS("data(NULL) returns NULL");
    else FAIL("data(NULL) should return NULL");

    vector_destroy(vec);
}

// ========== Test: NULL argument handling ==========
void test_vector_null_handling(void)
{
    printf("\n=== Test vector_null_handling ===\n");

    // vector_destroy(NULL) should not crash
    vector_destroy(NULL);
    PASS("vector_destroy(NULL) is safe");

    // vector_deinit(NULL) should not crash
    vector_deinit(NULL);
    PASS("vector_deinit(NULL) is safe");

    // vector_clear(NULL) should not crash
    vector_clear(NULL);
    PASS("vector_clear(NULL) is safe");

    // vector_push_back with NULL vec
    int val = 42;
    if (vector_push_back(NULL, &val) == -1) PASS("push_back(NULL, ...) returns -1");
    else FAIL("push_back(NULL, ...) should return -1");

    // vector_push_back with NULL element
    Vector* vec = vector_create(sizeof(int), 0);
    if (vector_push_back(vec, NULL) == -1) PASS("push_back(vec, NULL) returns -1");
    else FAIL("push_back(vec, NULL) should return -1");

    // vector_pop_back with NULL vec
    if (vector_pop_back(NULL, NULL) == -1) PASS("pop_back(NULL) returns -1");
    else FAIL("pop_back(NULL) should return -1");

    // vector_size/capacity/element_size with NULL
    if (vector_size(NULL) == 0) PASS("size(NULL) returns 0");
    else FAIL("size(NULL) should return 0");

    if (vector_capacity(NULL) == 0) PASS("capacity(NULL) returns 0");
    else FAIL("capacity(NULL) should return 0");

    if (vector_element_size(NULL) == 0) PASS("element_size(NULL) returns 0");
    else FAIL("element_size(NULL) should return 0");

    if (vector_empty(NULL) == true) PASS("empty(NULL) returns true");
    else FAIL("empty(NULL) should return true");

    vector_destroy(vec);
}

// ========== Test: stress test ==========
void test_vector_stress(void)
{
    printf("\n=== Test vector_stress ===\n");

    Vector* vec = vector_create(sizeof(int), 0);
    if (!vec) { FAIL("Vector creation failed"); return; }

    const int num = 10000;

    for (int i = 0; i < num; i++)
    {
        if (vector_push_back(vec, &i) != 0)
        {
            FAIL("Push failed at %d", i);
            vector_destroy(vec);
            return;
        }
    }
    PASS("Pushed %d elements (capacity=%zu)", num, vector_capacity(vec));

    bool all_correct = true;
    for (int i = 0; i < num; i++)
    {
        int* val = (int*)vector_get(vec, i);
        if (!val || *val != i) { all_correct = false; break; }
    }

    if (all_correct) PASS("All %d elements verified", num);
    else FAIL("Some elements incorrect");

    vector_destroy(vec);
}

// ========== Test: pointer storage ==========
void test_vector_pointer_storage(void)
{
    printf("\n=== Test vector_pointer_storage ===\n");

    Vector* ptr_vec = vector_create(sizeof(TestStruct*), 0);
    if (!ptr_vec) { FAIL("Vector creation failed"); return; }

    TestStruct* s1 = (TestStruct*)malloc(sizeof(TestStruct));
    TestStruct* s2 = (TestStruct*)malloc(sizeof(TestStruct));

    *s1 = (TestStruct){1, "Alice", 95.5};
    *s2 = (TestStruct){2, "Bob", 87.3};

    vector_push_back(ptr_vec, &s1);
    vector_push_back(ptr_vec, &s2);

    TestStruct** retrieved = (TestStruct**)vector_get(ptr_vec, 0);
    if (retrieved && *retrieved == s1 && (*retrieved)->id == 1)
    {
        PASS("Pointer storage and retrieval works");
    }
    else
    {
        FAIL("Pointer storage failed");
    }

    // Modify through stored pointer
    TestStruct** ptr = (TestStruct**)vector_get(ptr_vec, 1);
    (*ptr)->id = 999;
    if (s2->id == 999)
    {
        PASS("Modification through stored pointer works");
    }
    else
    {
        FAIL("Pointer modification failed");
    }

    // Pop pointer
    TestStruct* popped;
    if (vector_pop_back(ptr_vec, &popped) == 0 && popped == s2)
    {
        PASS("Pop_back returns correct pointer");
    }
    else
    {
        FAIL("Pop_back failed");
    }

    // Cleanup
    for (usize i = 0; i < vector_size(ptr_vec); i++)
    {
        TestStruct** p = (TestStruct**)vector_get(ptr_vec, i);
        free(*p);
    }
    free(popped);
    vector_destroy(ptr_vec);
}

// ========== Test: iterator init state ==========
void test_vector_iter_init_state(void)
{
    printf("\n=== Test vector_iter_init_state ===\n");

    Vector vec;
    Vector_init(&vec, sizeof(int), 0);

    VectorIterator iter;
    vector_iter_init(&iter, &vec);

    if (iter.vec == &vec)
        PASS("iter.vec points to vector");
    else
        FAIL("iter.vec incorrect");

    // get before first next should return NULL (no current element yet)
    // This matches hmap_iterator pattern: init -> next -> get
    if (vector_iter_get(&iter) == NULL)
        PASS("Get before first next() returns NULL");
    else
        FAIL("Get before first next() should return NULL");

    vector_deinit(&vec);
}

// ========== Test: iterator on empty vector ==========
void test_vector_iter_empty(void)
{
    printf("\n=== Test vector_iter_empty ===\n");

    Vector* vec = vector_create(sizeof(int), 0);
    if (!vec) { FAIL("Vector creation failed"); return; }

    VectorIterator iter;
    vector_iter_init(&iter, vec);

    int count = 0;
    while (vector_iter_next(&iter)) count++;

    if (count == 0)
        PASS("Empty vector: 0 iterations");
    else
        FAIL("Empty vector: expected 0, got %d", count);

    if (vector_iter_get(&iter) == NULL)
        PASS("Exhausted iter: get returns NULL");
    else
        FAIL("Exhausted iter: get should return NULL");

    vector_destroy(vec);
}

// ========== Test: iterator single element ==========
void test_vector_iter_single(void)
{
    printf("\n=== Test vector_iter_single ===\n");

    Vector* vec = vector_create(sizeof(int), 0);
    if (!vec) { FAIL("Vector creation failed"); return; }

    int val = 42;
    vector_push_back(vec, &val);

    VectorIterator iter;
    vector_iter_init(&iter, vec);

    int count = 0;
    int got = -1;
    while (vector_iter_next(&iter))
    {
        int* p = (int*)vector_iter_get(&iter);
        if (p) got = *p;
        count++;
    }

    if (count == 1)
        PASS("Single element: 1 iteration");
    else
        FAIL("Single element: expected 1 iteration, got %d", count);

    if (got == 42)
        PASS("Single element: value is 42");
    else
        FAIL("Single element: value is %d, expected 42", got);

    vector_destroy(vec);
}

// ========== Test: iterator visits all elements in order ==========
void test_vector_iter_order(void)
{
    printf("\n=== Test vector_iter_order ===\n");

    Vector* vec = vector_create(sizeof(int), 0);
    if (!vec) { FAIL("Vector creation failed"); return; }

    int data[] = {10, 20, 30, 40, 50};
    for (int i = 0; i < 5; i++) vector_push_back(vec, &data[i]);

    VectorIterator iter;
    vector_iter_init(&iter, vec);

    int results[5] = {0};
    int count = 0;
    while (vector_iter_next(&iter))
    {
        int* p = (int*)vector_iter_get(&iter);
        if (p && count < 5) results[count] = *p;
        count++;
    }

    if (count == 5)
        PASS("Visited exactly 5 elements");
    else
        FAIL("Visited %d elements, expected 5", count);

    // Verify order preserved (sequential access)
    bool in_order = true;
    for (int i = 0; i < 5 && i < count; i++)
    {
        if (results[i] != data[i]) { in_order = false; break; }
    }
    if (in_order && count == 5)
        PASS("Elements in correct order: 10,20,30,40,50");
    else
        FAIL("Order incorrect (first element: %d, expected 10)", count > 0 ? results[0] : -1);

    vector_destroy(vec);
}

// ========== Test: iterator get returns internal pointer ==========
void test_vector_iter_get_pointer(void)
{
    printf("\n=== Test vector_iter_get_pointer ===\n");

    Vector* vec = vector_create(sizeof(int), 0);
    if (!vec) { FAIL("Vector creation failed"); return; }

    int data[] = {100, 200, 300};
    for (int i = 0; i < 3; i++) vector_push_back(vec, &data[i]);

    VectorIterator iter;
    vector_iter_init(&iter, vec);

    // First element
    vector_iter_next(&iter);
    int* iter_ptr = (int*)vector_iter_get(&iter);
    int* direct_ptr = (int*)vector_get(vec, 0);

    if (iter_ptr == direct_ptr)
        PASS("iter_get returns same pointer as vector_get(0)");
    else
        FAIL("iter_get pointer mismatch with vector_get");

    // Modify through iterator pointer
    if (iter_ptr) *iter_ptr = 999;
    int* check = (int*)vector_get(vec, 0);
    if (check && *check == 999)
        PASS("Modify through iter_get affects vector data");
    else
        FAIL("Modify through iter_get should affect vector");

    vector_destroy(vec);
}

// ========== Test: iterator with structs ==========
void test_vector_iter_struct(void)
{
    printf("\n=== Test vector_iter_struct ===\n");

    Vector* vec = vector_create(sizeof(TestStruct), 0);
    if (!vec) { FAIL("Vector creation failed"); return; }

    TestStruct items[] = {
        {1, "Alice", 95.5},
        {2, "Bob", 87.3},
        {3, "Charlie", 91.0},
    };
    for (int i = 0; i < 3; i++) vector_push_back(vec, &items[i]);

    VectorIterator iter;
    vector_iter_init(&iter, vec);

    int count = 0;
    bool all_match = true;
    while (vector_iter_next(&iter))
    {
        TestStruct* s = (TestStruct*)vector_iter_get(&iter);
        if (!s || s->id != items[count].id ||
            strcmp(s->name, items[count].name) != 0)
        {
            all_match = false;
        }
        count++;
    }

    if (count == 3 && all_match)
        PASS("Struct iteration: all 3 structs match");
    else
        FAIL("Struct iteration failed (count=%d)", count);

    vector_destroy(vec);
}

// ========== Test: iterator after modifications ==========
void test_vector_iter_after_modify(void)
{
    printf("\n=== Test vector_iter_after_modify ===\n");

    Vector* vec = vector_create(sizeof(int), 0);
    if (!vec) { FAIL("Vector creation failed"); return; }

    // Push 5, pop 2, iterate remaining 3
    int data[] = {10, 20, 30, 40, 50};
    for (int i = 0; i < 5; i++) vector_push_back(vec, &data[i]);
    vector_pop_back(vec, NULL);
    vector_pop_back(vec, NULL);

    VectorIterator iter;
    vector_iter_init(&iter, vec);

    int count = 0;
    int sum = 0;
    while (vector_iter_next(&iter))
    {
        int* p = (int*)vector_iter_get(&iter);
        if (p) sum += *p;
        count++;
    }

    if (count == 3)
        PASS("After pop: iterated 3 elements");
    else
        FAIL("After pop: expected 3, got %d", count);

    if (sum == 60) // 10+20+30
        PASS("After pop: sum correct (60)");
    else
        FAIL("After pop: sum %d, expected 60", sum);

    vector_destroy(vec);
}

// ========== Test: iterator re-init ==========
void test_vector_iter_reinit(void)
{
    printf("\n=== Test vector_iter_reinit ===\n");

    Vector* vec = vector_create(sizeof(int), 0);
    if (!vec) { FAIL("Vector creation failed"); return; }

    int data[] = {1, 2, 3, 4};
    for (int i = 0; i < 4; i++) vector_push_back(vec, &data[i]);

    VectorIterator iter;

    // First iteration
    vector_iter_init(&iter, vec);
    int count1 = 0;
    while (vector_iter_next(&iter)) count1++;

    // Second iteration (re-init)
    vector_iter_init(&iter, vec);
    int count2 = 0;
    while (vector_iter_next(&iter)) count2++;

    if (count1 == 4 && count2 == 4)
        PASS("Re-init: both iterations yield 4");
    else
        FAIL("Re-init: first=%d, second=%d, expected 4", count1, count2);

    vector_destroy(vec);
}

// ========== Test: iterator large vector ==========
void test_vector_iter_large(void)
{
    printf("\n=== Test vector_iter_large ===\n");

    Vector* vec = vector_create(sizeof(int), 0);
    if (!vec) { FAIL("Vector creation failed"); return; }

    const int N = 1000;
    for (int i = 0; i < N; i++) vector_push_back(vec, &i);

    VectorIterator iter;
    vector_iter_init(&iter, vec);

    int count = 0;
    long sum = 0;
    while (vector_iter_next(&iter))
    {
        int* p = (int*)vector_iter_get(&iter);
        if (p) sum += *p;
        count++;
    }

    if (count == N)
        PASS("Large vector: iterated %d elements", N);
    else
        FAIL("Large vector: iterated %d, expected %d", count, N);

    long expected = (long)(N - 1) * N / 2; // 0+1+...+999
    if (sum == expected)
        PASS("Large vector: sum correct (%ld)", sum);
    else
        FAIL("Large vector: sum %ld, expected %ld", sum, expected);

    vector_destroy(vec);
}

// ========== Test: iterator next/get consistency ==========
void test_vector_iter_consistency(void)
{
    printf("\n=== Test vector_iter_consistency ===\n");

    Vector* vec = vector_create(sizeof(int), 0);
    if (!vec) { FAIL("Vector creation failed"); return; }

    int data[] = {100, 200, 300};
    for (int i = 0; i < 3; i++) vector_push_back(vec, &data[i]);

    VectorIterator iter;
    vector_iter_init(&iter, vec);

    // Every time next returns true, get must return non-NULL
    bool consistent = true;
    int count = 0;
    while (vector_iter_next(&iter))
    {
        void* val = vector_iter_get(&iter);
        if (!val) { consistent = false; break; }
        count++;
    }

    if (consistent && count == 3)
        PASS("next/get consistent: next=true always means get!=NULL");
    else
        FAIL("next/get inconsistent: next returned true but get returned NULL");

    // After next returns false, get should return NULL
    if (vector_iter_get(&iter) == NULL)
        PASS("After exhaustion: get returns NULL");
    else
        FAIL("After exhaustion: get should return NULL");

    vector_destroy(vec);
}

// ========== Test: VECTOR_FOREACH basic int iteration ==========
void test_vector_foreach_basic(void)
{
    printf("\n=== Test VECTOR_FOREACH basic ===\n");

    Vector* vec = vector_create(sizeof(int), 0);
    if (!vec) { FAIL("Vector creation failed"); return; }

    int data[] = {10, 20, 30, 40, 50};
    for (int i = 0; i < 5; i++) vector_push_back(vec, &data[i]);

    int count = 0;
    int sum = 0;
    VECTOR_FOREACH(vec, elem)
    {
        int* p = (int*)elem;
        if (p) sum += *p;
        count++;
    }

    if (count == 5)
        PASS("FOREACH visited 5 elements");
    else
        FAIL("FOREACH visited %d, expected 5", count);

    if (sum == 150)
        PASS("FOREACH sum correct (150)");
    else
        FAIL("FOREACH sum %d, expected 150", sum);

    vector_destroy(vec);
}

// ========== Test: VECTOR_FOREACH on empty vector ==========
void test_vector_foreach_empty(void)
{
    printf("\n=== Test VECTOR_FOREACH empty ===\n");

    Vector* vec = vector_create(sizeof(int), 0);
    if (!vec) { FAIL("Vector creation failed"); return; }

    int count = 0;
    VECTOR_FOREACH(vec, elem)
    {
        (void)elem;
        count++;
    }

    if (count == 0)
        PASS("FOREACH on empty: 0 iterations");
    else
        FAIL("FOREACH on empty: got %d iterations", count);

    vector_destroy(vec);
}

// ========== Test: VECTOR_FOREACH single element ==========
void test_vector_foreach_single(void)
{
    printf("\n=== Test VECTOR_FOREACH single ===\n");

    Vector* vec = vector_create(sizeof(int), 0);
    if (!vec) { FAIL("Vector creation failed"); return; }

    int val = 99;
    vector_push_back(vec, &val);

    int count = 0;
    int got = -1;
    VECTOR_FOREACH(vec, elem)
    {
        int* p = (int*)elem;
        if (p) got = *p;
        count++;
    }

    if (count == 1)
        PASS("FOREACH single: 1 iteration");
    else
        FAIL("FOREACH single: %d iterations", count);

    if (got == 99)
        PASS("FOREACH single: value 99");
    else
        FAIL("FOREACH single: value %d, expected 99", got);

    vector_destroy(vec);
}

// ========== Test: VECTOR_FOREACH preserves order ==========
void test_vector_foreach_order(void)
{
    printf("\n=== Test VECTOR_FOREACH order ===\n");

    Vector* vec = vector_create(sizeof(int), 0);
    if (!vec) { FAIL("Vector creation failed"); return; }

    int data[] = {5, 4, 3, 2, 1};
    for (int i = 0; i < 5; i++) vector_push_back(vec, &data[i]);

    int results[5];
    int idx = 0;
    VECTOR_FOREACH(vec, elem)
    {
        int* p = (int*)elem;
        if (p && idx < 5) results[idx++] = *p;
    }

    bool in_order = true;
    for (int i = 0; i < 5; i++)
    {
        if (results[i] != data[i]) { in_order = false; break; }
    }

    if (in_order)
        PASS("FOREACH preserves insertion order: 5,4,3,2,1");
    else
        FAIL("FOREACH order incorrect");

    vector_destroy(vec);
}

// ========== Test: VECTOR_FOREACH with structs ==========
void test_vector_foreach_struct(void)
{
    printf("\n=== Test VECTOR_FOREACH struct ===\n");

    Vector* vec = vector_create(sizeof(TestStruct), 0);
    if (!vec) { FAIL("Vector creation failed"); return; }

    TestStruct items[] = {
        {1, "Alice", 95.5},
        {2, "Bob", 87.3},
        {3, "Charlie", 91.0},
    };
    for (int i = 0; i < 3; i++) vector_push_back(vec, &items[i]);

    int count = 0;
    bool all_match = true;
    VECTOR_FOREACH(vec, elem)
    {
        TestStruct* s = (TestStruct*)elem;
        if (!s || s->id != items[count].id ||
            strcmp(s->name, items[count].name) != 0)
        {
            all_match = false;
        }
        count++;
    }

    if (count == 3 && all_match)
        PASS("FOREACH struct: all 3 match");
    else
        FAIL("FOREACH struct: count=%d, match=%d", count, all_match);

    vector_destroy(vec);
}

// ========== Test: VECTOR_FOREACH modify through pointer ==========
void test_vector_foreach_modify(void)
{
    printf("\n=== Test VECTOR_FOREACH modify ===\n");

    Vector* vec = vector_create(sizeof(int), 0);
    if (!vec) { FAIL("Vector creation failed"); return; }

    for (int i = 1; i <= 5; i++) vector_push_back(vec, &i);

    // Double every element through the foreach pointer
    VECTOR_FOREACH(vec, elem)
    {
        int* p = (int*)elem;
        if (p) *p *= 2;
    }

    // Verify via direct access
    int expected[] = {2, 4, 6, 8, 10};
    bool ok = true;
    for (int i = 0; i < 5; i++)
    {
        int* v = (int*)vector_get(vec, i);
        if (!v || *v != expected[i]) { ok = false; break; }
    }

    if (ok)
        PASS("FOREACH modify: all elements doubled");
    else
        FAIL("FOREACH modify: data mismatch");

    vector_destroy(vec);
}

// ========== Test: VECTOR_FOREACH after push/pop ==========
void test_vector_foreach_after_pop(void)
{
    printf("\n=== Test VECTOR_FOREACH after pop ===\n");

    Vector* vec = vector_create(sizeof(int), 0);
    if (!vec) { FAIL("Vector creation failed"); return; }

    int data[] = {10, 20, 30, 40, 50};
    for (int i = 0; i < 5; i++) vector_push_back(vec, &data[i]);
    vector_pop_back(vec, NULL);
    vector_pop_back(vec, NULL);

    int count = 0;
    int sum = 0;
    VECTOR_FOREACH(vec, elem)
    {
        int* p = (int*)elem;
        if (p) sum += *p;
        count++;
    }

    if (count == 3)
        PASS("FOREACH after pop: 3 elements");
    else
        FAIL("FOREACH after pop: %d elements", count);

    if (sum == 60)
        PASS("FOREACH after pop: sum 60");
    else
        FAIL("FOREACH after pop: sum %d", sum);

    vector_destroy(vec);
}

// ========== Test: VECTOR_FOREACH with embedded vector ==========
void test_vector_foreach_embedded(void)
{
    printf("\n=== Test VECTOR_FOREACH embedded ===\n");

    Vector vec;
    Vector_init(&vec, sizeof(int), 0);

    int data[] = {7, 14, 21};
    for (int i = 0; i < 3; i++) vector_push_back(&vec, &data[i]);

    int count = 0;
    int sum = 0;
    VECTOR_FOREACH(&vec, elem)
    {
        int* p = (int*)elem;
        if (p) sum += *p;
        count++;
    }

    if (count == 3 && sum == 42)
        PASS("FOREACH embedded vector: count=3, sum=42");
    else
        FAIL("FOREACH embedded: count=%d, sum=%d", count, sum);

    vector_deinit(&vec);
}

// ========== Test: two VECTOR_FOREACH in same scope ==========
void test_vector_foreach_two_loops(void)
{
    printf("\n=== Test VECTOR_FOREACH two loops ===\n");

    Vector* vec = vector_create(sizeof(int), 0);
    if (!vec) { FAIL("Vector creation failed"); return; }

    for (int i = 1; i <= 4; i++) vector_push_back(vec, &i);

    // First loop: sum
    int sum1 = 0;
    {
        VECTOR_FOREACH(vec, a)
        {
            int* p = (int*)a;
            if (p) sum1 += *p;
        }
    }

    // Second loop: sum again (different variable name)
    int sum2 = 0;
    {
        VECTOR_FOREACH(vec, b)
        {
            int* p = (int*)b;
            if (p) sum2 += *p;
        }
    }

    if (sum1 == 10 && sum2 == 10)
        PASS("Two FOREACH loops: both sum to 10");
    else
        FAIL("Two loops: sum1=%d, sum2=%d", sum1, sum2);

    vector_destroy(vec);
}

// ========== Test: VECTOR_FOREACH large ==========
void test_vector_foreach_large(void)
{
    printf("\n=== Test VECTOR_FOREACH large ===\n");

    Vector* vec = vector_create(sizeof(int), 0);
    if (!vec) { FAIL("Vector creation failed"); return; }

    const int N = 1000;
    for (int i = 0; i < N; i++) vector_push_back(vec, &i);

    int count = 0;
    long sum = 0;
    VECTOR_FOREACH(vec, elem)
    {
        int* p = (int*)elem;
        if (p) sum += *p;
        count++;
    }

    long expected = (long)(N - 1) * N / 2;
    if (count == N)
        PASS("FOREACH large: %d elements", N);
    else
        FAIL("FOREACH large: %d, expected %d", count, N);

    if (sum == expected)
        PASS("FOREACH large: sum %ld", sum);
    else
        FAIL("FOREACH large: sum %ld, expected %ld", sum, expected);

    vector_destroy(vec);
}

// ========== Test: VECTOR_FOREACH elem is non-NULL ==========
void test_vector_foreach_nonnull(void)
{
    printf("\n=== Test VECTOR_FOREACH non-null guarantee ===\n");

    Vector* vec = vector_create(sizeof(int), 0);
    if (!vec) { FAIL("Vector creation failed"); return; }

    int data[] = {1, 2, 3, 4, 5};
    for (int i = 0; i < 5; i++) vector_push_back(vec, &data[i]);

    bool all_nonnull = true;
    int count = 0;
    VECTOR_FOREACH(vec, elem)
    {
        if (!elem) { all_nonnull = false; break; }
        count++;
    }

    if (all_nonnull && count == 5)
        PASS("FOREACH: elem always non-NULL");
    else
        FAIL("FOREACH: elem was NULL at iteration %d", count);

    vector_destroy(vec);
}

int main(void)
{
    printf("========================================\n");
    printf("   Vector Test Suite\n");
    printf("========================================\n");

    test_vector_create_destroy();
    test_vector_create_zero_element();
    test_vector_init_deinit();
    test_vector_reserve();
    test_vector_push_pop();
    test_vector_get_set();
    test_vector_struct();
    test_vector_insert_erase();
    test_vector_clear_resize();
    test_vector_find();
    test_vector_access();
    test_vector_null_handling();
    test_vector_stress();
    test_vector_pointer_storage();

    // Iterator tests
    test_vector_iter_init_state();
    test_vector_iter_empty();
    test_vector_iter_single();
    test_vector_iter_order();
    test_vector_iter_get_pointer();
    test_vector_iter_struct();
    test_vector_iter_after_modify();
    test_vector_iter_reinit();
    test_vector_iter_large();
    test_vector_iter_consistency();

    // VECTOR_FOREACH macro tests
    test_vector_foreach_basic();
    test_vector_foreach_empty();
    test_vector_foreach_single();
    test_vector_foreach_order();
    test_vector_foreach_struct();
    test_vector_foreach_modify();
    test_vector_foreach_after_pop();
    test_vector_foreach_embedded();
    test_vector_foreach_two_loops();
    test_vector_foreach_large();
    test_vector_foreach_nonnull();

    printf("\n========================================\n");
    printf("   Results: %d passed, %d failed\n", pass_count, fail_count);
    printf("========================================\n\n");

    return fail_count > 0 ? 1 : 0;
}
