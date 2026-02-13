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

// ========== Test: Vector_create / Vector_destroy ==========
void test_vector_create_destroy(void)
{
    printf("\n=== Test Vector_create_destroy ===\n");

    Vector* int_vec = Vector_create(sizeof(int), 0);
    if (!int_vec)
    {
        FAIL("Vector creation failed");
        return;
    }

    if (Vector_element_size(int_vec) == sizeof(int) && Vector_size(int_vec) == 0 &&
        Vector_capacity(int_vec) > 0 && Vector_empty(int_vec))
    {
        PASS("Int Vector initialization correct");
    }
    else
    {
        FAIL("Int Vector initialization incorrect");
    }

    Vector_destroy(int_vec);

    Vector* struct_vec = Vector_create(sizeof(TestStruct), 8);
    if (struct_vec && Vector_element_size(struct_vec) == sizeof(TestStruct) &&
        Vector_capacity(struct_vec) == 8)
    {
        PASS("Struct Vector creation with explicit capacity");
    }
    else
    {
        FAIL("Struct Vector creation failed");
    }

    Vector_destroy(struct_vec);
}

// ========== Test: Vector_create with element_size=0 ==========
void test_vector_create_zero_element(void)
{
    printf("\n=== Test Vector_create_zero_element ===\n");

    Vector* vec = Vector_create(0, 0);
    if (vec == NULL)
    {
        PASS("Vector_create(0, 0) returns NULL");
    }
    else
    {
        FAIL("Vector_create(0, 0) should return NULL");
        Vector_destroy(vec);
    }
}

// ========== Test: Vector_init / Vector_deinit (embedded) ==========
void test_vector_init_deinit(void)
{
    printf("\n=== Test Vector_init_deinit (embedded) ===\n");

    Vector vec;
    int result = Vector_init(&vec, sizeof(int), 0);
    if (result == 0 && vec.data != NULL && vec.size == 0 && vec.capacity > 0)
    {
        PASS("Vector_init succeeded for embedded vector");
    }
    else
    {
        FAIL("Vector_init failed");
        return;
    }

    // Push some data
    int val = 42;
    Vector_push_back(&vec, &val);
    val = 99;
    Vector_push_back(&vec, &val);

    if (Vector_size(&vec) == 2)
    {
        PASS("Embedded vector push_back works (size=%zu)", Vector_size(&vec));
    }
    else
    {
        FAIL("Embedded vector push_back failed");
    }

    // Verify data
    int* p = (int*)Vector_get(&vec, 0);
    if (p && *p == 42)
    {
        PASS("Embedded vector data accessible");
    }
    else
    {
        FAIL("Embedded vector data incorrect");
    }

    // Deinit
    Vector_deinit(&vec);
    if (vec.data == NULL && vec.size == 0 && vec.capacity == 0)
    {
        PASS("Vector_deinit clears all fields");
    }
    else
    {
        FAIL("Vector_deinit did not clear fields properly");
    }

    // Double deinit should be safe
    Vector_deinit(&vec);
    PASS("Double Vector_deinit is safe");
}

// ========== Test: Vector_reserve ==========
void test_vector_reserve(void)
{
    printf("\n=== Test Vector_reserve ===\n");

    Vector* vec = Vector_create(sizeof(int), 4);
    if (!vec)
    {
        FAIL("Vector creation failed");
        return;
    }

    usize original_cap = Vector_capacity(vec);
    if (original_cap == 4)
    {
        PASS("Initial capacity is 4");
    }
    else
    {
        FAIL("Initial capacity expected 4, got %zu", original_cap);
    }

    // Reserve larger
    if (Vector_reserve(vec, 100) == 0 && Vector_capacity(vec) >= 100)
    {
        PASS("Vector_reserve(100) succeeded, capacity=%zu", Vector_capacity(vec));
    }
    else
    {
        FAIL("Vector_reserve(100) failed");
    }

    // Reserve smaller (should be no-op)
    usize cap_before = Vector_capacity(vec);
    if (Vector_reserve(vec, 10) == 0 && Vector_capacity(vec) == cap_before)
    {
        PASS("Vector_reserve with smaller value is no-op");
    }
    else
    {
        FAIL("Vector_reserve with smaller value should be no-op");
    }

    // Reserve on NULL
    if (Vector_reserve(NULL, 10) == -1)
    {
        PASS("Vector_reserve(NULL) returns -1");
    }
    else
    {
        FAIL("Vector_reserve(NULL) should return -1");
    }

    // Push data after reserve and verify
    for (int i = 0; i < 50; i++)
    {
        Vector_push_back(vec, &i);
    }
    bool data_ok = true;
    for (int i = 0; i < 50; i++)
    {
        int* p = (int*)Vector_get(vec, i);
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

    Vector_destroy(vec);
}

// ========== Test: push_back and pop_back (value copy) ==========
void test_vector_push_pop(void)
{
    printf("\n=== Test Vector_push_pop ===\n");

    Vector* vec = Vector_create(sizeof(int), 4);
    if (!vec) { FAIL("Vector creation failed"); return; }

    int values[] = {10, 20, 30, 40, 50};
    int num = 5;

    for (int i = 0; i < num; i++)
    {
        if (Vector_push_back(vec, &values[i]) != 0)
        {
            FAIL("push_back failed for %d", values[i]);
            Vector_destroy(vec);
            return;
        }
    }
    PASS("Pushed %d values (size=%zu, capacity=%zu)", num, Vector_size(vec), Vector_capacity(vec));

    // Verify value copy semantics
    for (int i = 0; i < num; i++) values[i] = 999;

    bool unchanged = true;
    int expected[] = {10, 20, 30, 40, 50};
    for (usize i = 0; i < Vector_size(vec); i++)
    {
        int* val = (int*)Vector_get(vec, i);
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
        if (Vector_pop_back(vec, &popped) != 0 || popped != expected[i])
        {
            FAIL("pop_back returned wrong value");
            Vector_destroy(vec);
            return;
        }
    }

    if (Vector_empty(vec) && Vector_size(vec) == 0)
    {
        PASS("All values popped correctly");
    }
    else
    {
        FAIL("Vector not empty after popping all");
    }

    // Pop from empty vector
    if (Vector_pop_back(vec, &popped) == -1)
    {
        PASS("Pop from empty vector returns -1");
    }
    else
    {
        FAIL("Pop from empty vector should return -1");
    }

    Vector_destroy(vec);
}

// ========== Test: get and set ==========
void test_vector_get_set(void)
{
    printf("\n=== Test Vector_get_set ===\n");

    Vector* vec = Vector_create(sizeof(int), 0);
    if (!vec) { FAIL("Vector creation failed"); return; }

    int data[] = {100, 200, 300, 400, 500};
    for (int i = 0; i < 5; i++) Vector_push_back(vec, &data[i]);

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
    if (Vector_get(vec, 100) == NULL)
    {
        PASS("Out-of-bounds get returns NULL");
    }
    else
    {
        FAIL("Out-of-bounds get should return NULL");
    }

    // Test set
    int new_value = 999;
    if (Vector_set(vec, 2, &new_value) == 0)
    {
        int* val = (int*)Vector_get(vec, 2);
        if (val && *val == 999) PASS("Set operation works");
        else FAIL("Set verification failed");
    }
    else
    {
        FAIL("Set operation failed");
    }

    // Test out of bounds set
    if (Vector_set(vec, 100, &new_value) == -1)
    {
        PASS("Out-of-bounds set returns -1");
    }
    else
    {
        FAIL("Out-of-bounds set should return -1");
    }

    // Test get_copy
    int copied_value;
    if (Vector_get_copy(vec, 0, &copied_value) == 0 && copied_value == 100)
    {
        PASS("Get_copy works");
    }
    else
    {
        FAIL("Get_copy failed");
    }

    // Test get_copy out of bounds
    if (Vector_get_copy(vec, 100, &copied_value) == -1)
    {
        PASS("Out-of-bounds get_copy returns -1");
    }
    else
    {
        FAIL("Out-of-bounds get_copy should return -1");
    }

    Vector_destroy(vec);
}

// ========== Test: struct storage ==========
void test_vector_struct(void)
{
    printf("\n=== Test Vector_struct ===\n");

    Vector* vec = Vector_create(sizeof(TestStruct), 0);
    if (!vec) { FAIL("Vector creation failed"); return; }

    TestStruct s1 = {1, "Alice", 95.5};
    TestStruct s2 = {2, "Bob", 87.3};
    TestStruct s3 = {3, "Charlie", 92.1};

    Vector_push_back(vec, &s1);
    Vector_push_back(vec, &s2);
    Vector_push_back(vec, &s3);

    if (Vector_size(vec) == 3) PASS("Added 3 structs");
    else FAIL("Size incorrect");

    // Modify original
    s1.id = 999;
    strcpy(s1.name, "Modified");

    TestStruct* stored = (TestStruct*)Vector_get(vec, 0);
    if (stored && stored->id == 1 && strcmp(stored->name, "Alice") == 0)
    {
        PASS("Struct data is copied (value semantics)");
    }
    else
    {
        FAIL("Struct data should be independent");
    }

    Vector_destroy(vec);
}

// ========== Test: insert and erase ==========
void test_vector_insert_erase(void)
{
    printf("\n=== Test Vector_insert_erase ===\n");

    Vector* vec = Vector_create(sizeof(int), 0);
    if (!vec) { FAIL("Vector creation failed"); return; }

    int data[] = {1, 2, 4, 5};
    for (int i = 0; i < 4; i++) Vector_push_back(vec, &data[i]);

    // Insert 3 at index 2
    int three = 3;
    if (Vector_insert(vec, 2, &three) == 0)
    {
        int expected[] = {1, 2, 3, 4, 5};
        bool ok = true;
        for (int i = 0; i < 5; i++)
        {
            int* v = (int*)Vector_get(vec, i);
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
    if (Vector_insert(vec, 0, &zero) == 0)
    {
        int* v = (int*)Vector_get(vec, 0);
        if (v && *v == 0) PASS("Insert at beginning works");
        else FAIL("Insert at beginning failed");
    }

    // Insert at end (append)
    int six = 6;
    if (Vector_insert(vec, Vector_size(vec), &six) == 0)
    {
        int* v = (int*)Vector_back(vec);
        if (v && *v == 6) PASS("Insert at end works");
        else FAIL("Insert at end failed");
    }

    // Insert beyond size
    if (Vector_insert(vec, Vector_size(vec) + 10, &six) == -1)
    {
        PASS("Insert beyond size returns -1");
    }
    else
    {
        FAIL("Insert beyond size should return -1");
    }

    // Erase
    int erased;
    usize size_before = Vector_size(vec);
    if (Vector_erase(vec, 0, &erased) == 0 && erased == 0)
    {
        if (Vector_size(vec) == size_before - 1)
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
    if (Vector_erase(vec, 999, NULL) == -1)
    {
        PASS("Erase out of bounds returns -1");
    }
    else
    {
        FAIL("Erase out of bounds should return -1");
    }

    Vector_destroy(vec);
}

// ========== Test: clear and resize ==========
void test_vector_clear_resize(void)
{
    printf("\n=== Test Vector_clear_resize ===\n");

    Vector* vec = Vector_create(sizeof(int), 0);
    if (!vec) { FAIL("Vector creation failed"); return; }

    int data[] = {1, 2, 3, 4, 5};
    for (int i = 0; i < 5; i++) Vector_push_back(vec, &data[i]);

    // Test clear
    Vector_clear(vec);
    if (Vector_empty(vec) && Vector_size(vec) == 0)
    {
        PASS("Clear works");
    }
    else
    {
        FAIL("Clear failed");
    }

    // Resize with zero-init
    if (Vector_resize(vec, 5, NULL) == 0)
    {
        bool all_zero = true;
        for (usize i = 0; i < 5; i++)
        {
            int val = *(int*)Vector_get(vec, i);
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
    if (Vector_resize(vec, 8, &default_val) == 0)
    {
        bool ok = true;
        for (usize i = 5; i < 8; i++)
        {
            int val = *(int*)Vector_get(vec, i);
            if (val != 42) { ok = false; break; }
        }
        if (ok) PASS("Resize with default value works");
        else FAIL("Resize default value failed");
    }

    // Resize to shrink
    if (Vector_resize(vec, 3, NULL) == 0 && Vector_size(vec) == 3)
    {
        PASS("Resize to shrink works (size=%zu)", Vector_size(vec));
    }
    else
    {
        FAIL("Resize to shrink failed");
    }

    Vector_destroy(vec);
}

// ========== Test: find ==========
void test_vector_find(void)
{
    printf("\n=== Test Vector_find ===\n");

    Vector* vec = Vector_create(sizeof(int), 0);
    if (!vec) { FAIL("Vector creation failed"); return; }

    int data[] = {10, 20, 30, 40, 50};
    for (int i = 0; i < 5; i++) Vector_push_back(vec, &data[i]);

    int target = 30;
    if (Vector_find(vec, &target, int_compare) == 2)
    {
        PASS("Found 30 at index 2");
    }
    else
    {
        FAIL("Find failed for existing element");
    }

    int not_found = 99;
    if (Vector_find(vec, &not_found, int_compare) == -1)
    {
        PASS("Returns -1 for non-existent element");
    }
    else
    {
        FAIL("Find should return -1");
    }

    // Find with NULL args
    if (Vector_find(NULL, &target, int_compare) == -1)
    {
        PASS("Find with NULL vec returns -1");
    }
    else
    {
        FAIL("Find with NULL vec should return -1");
    }

    if (Vector_find(vec, &target, NULL) == -1)
    {
        PASS("Find with NULL compare returns -1");
    }
    else
    {
        FAIL("Find with NULL compare should return -1");
    }

    Vector_destroy(vec);
}

// ========== Test: front, back, data ==========
void test_vector_access(void)
{
    printf("\n=== Test Vector_access (front/back/data) ===\n");

    Vector* vec = Vector_create(sizeof(int), 0);
    if (!vec) { FAIL("Vector creation failed"); return; }

    // Empty vector
    if (Vector_front(vec) == NULL && Vector_back(vec) == NULL)
    {
        PASS("Empty vector: front and back return NULL");
    }
    else
    {
        FAIL("Empty vector should return NULL for front/back");
    }

    int data[] = {100, 200, 300};
    for (int i = 0; i < 3; i++) Vector_push_back(vec, &data[i]);

    int* front = VECTOR_FRONT(vec, int);
    int* back = VECTOR_BACK(vec, int);

    if (front && *front == 100) PASS("Front element: %d", *front);
    else FAIL("Front element incorrect");

    if (back && *back == 300) PASS("Back element: %d", *back);
    else FAIL("Back element incorrect");

    int* raw_data = (int*)Vector_data(vec);
    if (raw_data && raw_data[0] == 100 && raw_data[1] == 200 && raw_data[2] == 300)
    {
        PASS("Vector_data returns correct pointer");
    }
    else
    {
        FAIL("Vector_data failed");
    }

    // NULL vector
    if (Vector_front(NULL) == NULL) PASS("front(NULL) returns NULL");
    else FAIL("front(NULL) should return NULL");

    if (Vector_back(NULL) == NULL) PASS("back(NULL) returns NULL");
    else FAIL("back(NULL) should return NULL");

    if (Vector_data(NULL) == NULL) PASS("data(NULL) returns NULL");
    else FAIL("data(NULL) should return NULL");

    Vector_destroy(vec);
}

// ========== Test: NULL argument handling ==========
void test_vector_null_handling(void)
{
    printf("\n=== Test Vector_null_handling ===\n");

    // Vector_destroy(NULL) should not crash
    Vector_destroy(NULL);
    PASS("Vector_destroy(NULL) is safe");

    // Vector_deinit(NULL) should not crash
    Vector_deinit(NULL);
    PASS("Vector_deinit(NULL) is safe");

    // Vector_clear(NULL) should not crash
    Vector_clear(NULL);
    PASS("Vector_clear(NULL) is safe");

    // Vector_push_back with NULL vec
    int val = 42;
    if (Vector_push_back(NULL, &val) == -1) PASS("push_back(NULL, ...) returns -1");
    else FAIL("push_back(NULL, ...) should return -1");

    // Vector_push_back with NULL element
    Vector* vec = Vector_create(sizeof(int), 0);
    if (Vector_push_back(vec, NULL) == -1) PASS("push_back(vec, NULL) returns -1");
    else FAIL("push_back(vec, NULL) should return -1");

    // Vector_pop_back with NULL vec
    if (Vector_pop_back(NULL, NULL) == -1) PASS("pop_back(NULL) returns -1");
    else FAIL("pop_back(NULL) should return -1");

    // Vector_size/capacity/element_size with NULL
    if (Vector_size(NULL) == 0) PASS("size(NULL) returns 0");
    else FAIL("size(NULL) should return 0");

    if (Vector_capacity(NULL) == 0) PASS("capacity(NULL) returns 0");
    else FAIL("capacity(NULL) should return 0");

    if (Vector_element_size(NULL) == 0) PASS("element_size(NULL) returns 0");
    else FAIL("element_size(NULL) should return 0");

    if (Vector_empty(NULL) == true) PASS("empty(NULL) returns true");
    else FAIL("empty(NULL) should return true");

    Vector_destroy(vec);
}

// ========== Test: stress test ==========
void test_vector_stress(void)
{
    printf("\n=== Test Vector_stress ===\n");

    Vector* vec = Vector_create(sizeof(int), 0);
    if (!vec) { FAIL("Vector creation failed"); return; }

    const int num = 10000;

    for (int i = 0; i < num; i++)
    {
        if (Vector_push_back(vec, &i) != 0)
        {
            FAIL("Push failed at %d", i);
            Vector_destroy(vec);
            return;
        }
    }
    PASS("Pushed %d elements (capacity=%zu)", num, Vector_capacity(vec));

    bool all_correct = true;
    for (int i = 0; i < num; i++)
    {
        int* val = (int*)Vector_get(vec, i);
        if (!val || *val != i) { all_correct = false; break; }
    }

    if (all_correct) PASS("All %d elements verified", num);
    else FAIL("Some elements incorrect");

    Vector_destroy(vec);
}

// ========== Test: pointer storage ==========
void test_vector_pointer_storage(void)
{
    printf("\n=== Test Vector_pointer_storage ===\n");

    Vector* ptr_vec = Vector_create(sizeof(TestStruct*), 0);
    if (!ptr_vec) { FAIL("Vector creation failed"); return; }

    TestStruct* s1 = (TestStruct*)malloc(sizeof(TestStruct));
    TestStruct* s2 = (TestStruct*)malloc(sizeof(TestStruct));

    *s1 = (TestStruct){1, "Alice", 95.5};
    *s2 = (TestStruct){2, "Bob", 87.3};

    Vector_push_back(ptr_vec, &s1);
    Vector_push_back(ptr_vec, &s2);

    TestStruct** retrieved = (TestStruct**)Vector_get(ptr_vec, 0);
    if (retrieved && *retrieved == s1 && (*retrieved)->id == 1)
    {
        PASS("Pointer storage and retrieval works");
    }
    else
    {
        FAIL("Pointer storage failed");
    }

    // Modify through stored pointer
    TestStruct** ptr = (TestStruct**)Vector_get(ptr_vec, 1);
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
    if (Vector_pop_back(ptr_vec, &popped) == 0 && popped == s2)
    {
        PASS("Pop_back returns correct pointer");
    }
    else
    {
        FAIL("Pop_back failed");
    }

    // Cleanup
    for (usize i = 0; i < Vector_size(ptr_vec); i++)
    {
        TestStruct** p = (TestStruct**)Vector_get(ptr_vec, i);
        free(*p);
    }
    free(popped);
    Vector_destroy(ptr_vec);
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

    printf("\n========================================\n");
    printf("   Results: %d passed, %d failed\n", pass_count, fail_count);
    printf("========================================\n\n");

    return fail_count > 0 ? 1 : 0;
}
