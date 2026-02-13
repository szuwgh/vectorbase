#include <stdio.h>
#include "../src/vectorbase.h"
#include "../src/storage.h"

// Test setup and teardown
void setUp(void)
{
  // Set up test fixtures
}

void tearDown(void)
{
  // Clean up after test
}

// Test cases
void test_VectorBase_initialization(void)
{
    VectorBase vb;
    vb.storage_manager = NULL;
    vb.catalog = NULL;
}

void test_VectorBase_zero_initialization(void)
{
    VectorBase vb = {0};
}

void test_FileBuffer_create(void)
{
    FileBuffer* fb = FileBuffer_create(4096);
    if (!fb)
    {
        printf("FileBuffer creation failed\n");
    }
    printf("FileBuffer created with offset %p\n", fb->internal_buf);
    printf("FileBuffer created with internal_size %zu\n", fb->internal_size);
    printf("FileBuffer created with size %zu\n", fb->size);
    printf("FileBuffer created with buffer offset %p\n", fb->buffer);
}

// Run all tests
int main(void)
{
    test_FileBuffer_create();
    printf("Running VectorBase tests...\n");
}