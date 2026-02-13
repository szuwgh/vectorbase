#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "storage.h"

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

// MemoryFile - in-memory buffer simulating a file
typedef struct
{
    u8* data;
    usize size;
    usize capacity;
} MemoryFile;

// MemFileHandle - extends FileHandle to wrap a MemoryFile
typedef struct
{
    EXTENDS(FileHandle);
    MemoryFile* mf;
} MemFileHandle;

// Memory file read function
static int mem_file_read(FileHandle* handle, void* buffer, usize nr_bytes)
{
    MemFileHandle* mfh = (MemFileHandle*)handle;
    MemoryFile* mf = mfh->mf;
    if (nr_bytes > mf->size) nr_bytes = mf->size;
    memcpy(buffer, mf->data, nr_bytes);
    return nr_bytes;
}

// Memory file write function
static int mem_file_write(FileHandle* handle, const void* buffer, usize nr_bytes)
{
    MemFileHandle* mfh = (MemFileHandle*)handle;
    MemoryFile* mf = mfh->mf;
    if (nr_bytes > mf->capacity - mf->size)
    {
        mf->capacity = mf->size + nr_bytes;
        mf->data = (u8*)realloc(mf->data, mf->capacity);
    }
    memcpy(mf->data + mf->size, buffer, nr_bytes);
    mf->size += nr_bytes;
    return nr_bytes;
}

// Memory file read_at function
static int mem_file_read_at(FileHandle* handle, void* buffer, usize nr_bytes, usize location)
{
    MemFileHandle* mfh = (MemFileHandle*)handle;
    MemoryFile* mf = mfh->mf;
    if (location + nr_bytes > mf->size)
    {
        nr_bytes = (location < mf->size) ? (mf->size - location) : 0;
    }
    if (nr_bytes > 0)
    {
        memcpy(buffer, mf->data + location, nr_bytes);
    }
    return nr_bytes;
}

// Memory file write_at function
static int mem_file_write_at(FileHandle* handle, const void* buffer, usize nr_bytes, usize location)
{
    MemFileHandle* mfh = (MemFileHandle*)handle;
    MemoryFile* mf = mfh->mf;
    if (location + nr_bytes > mf->capacity)
    {
        mf->capacity = location + nr_bytes;
        mf->data = (u8*)realloc(mf->data, mf->capacity);
    }
    memcpy(mf->data + location, buffer, nr_bytes);
    if (location + nr_bytes > mf->size)
    {
        mf->size = location + nr_bytes;
    }
    return nr_bytes;
}

// Memory file free function - only frees the handle, not the MemoryFile
static void mem_file_free(FileHandle* handle)
{
    free(handle);
}

// Memory file sync function (no-op for memory)
static void mem_file_sync(FileHandle* handle)
{
    (void)handle;
}

// VTable for memory file handle
static const FileHandleVTable mem_file_vtable = {.free = mem_file_free,
                                                  .read = mem_file_read,
                                                  .write = mem_file_write,
                                                  .read_at = mem_file_read_at,
                                                  .write_at = mem_file_write_at,
                                                  .sync = mem_file_sync};

// Create a MemoryFile
MemoryFile* create_memory_file(usize initial_capacity)
{
    MemoryFile* mf = (MemoryFile*)malloc(sizeof(MemoryFile));
    mf->data = (u8*)malloc(initial_capacity);
    mf->size = 0;
    mf->capacity = initial_capacity;
    memset(mf->data, 0, initial_capacity);
    return mf;
}

// Destroy a MemoryFile
void destroy_memory_file(MemoryFile* mf)
{
    if (mf)
    {
        if (mf->data) free(mf->data);
        free(mf);
    }
}

// Create a FileHandle wrapping a MemoryFile
static FileHandle* create_mem_file_handle(MemoryFile* mf)
{
    MemFileHandle* mfh = (MemFileHandle*)malloc(sizeof(MemFileHandle));
    if (!mfh) return NULL;
    mfh->base.vtable = &mem_file_vtable;
    mfh->mf = mf;
    return (FileHandle*)mfh;
}

// Test FileBuffer creation
void test_fileBuffer_create(void)
{
    printf("\n=== Test fileBuffer_create ===\n");

    usize buffer_size = 8192;
    FileBuffer* fb = fileBuffer_create(buffer_size);

    if (!fb)
    {
        FAIL("FileBuffer creation failed");
        return;
    }

    PASS("FileBuffer created successfully");

    // Check alignment
    u64 alignment = (u64)fb->internal_buf % FILE_BUFFER_BLOCK_SIZE;
    if (alignment == 0)
    {
        PASS("Buffer is properly aligned to %d bytes", FILE_BUFFER_BLOCK_SIZE);
    }
    else
    {
        FAIL("Buffer alignment failed, offset: %lu", (unsigned long)alignment);
    }

    // Check size calculation
    if (fb->size == buffer_size - FILE_BUFFER_HEADER_SIZE)
    {
        PASS("Buffer size calculation correct");
    }
    else
    {
        FAIL("Buffer size calculation incorrect (expected %zu, got %zu)",
             buffer_size - FILE_BUFFER_HEADER_SIZE, fb->size);
    }

    fileBuffer_destroy(fb);
}

// Test FileBuffer read/write
void test_fileBuffer_read_write(void)
{
    printf("\n=== Test fileBuffer_read_write ===\n");

    usize buffer_size = 4096;
    FileBuffer* fb = fileBuffer_create(buffer_size);
    if (!fb)
    {
        FAIL("FileBuffer creation failed");
        return;
    }

    MemoryFile* mf = create_memory_file(8192);
    FileHandle* fh = create_mem_file_handle(mf);

    // Write test data
    const char* test_data = "Hello, VectorBase FileBuffer Test!";
    memcpy(fb->buffer, test_data, strlen(test_data) + 1);

    // Write to memory file
    int write_result = fileBuffer_write(fb, fh, 0);
    if (write_result == 0)
    {
        PASS("fileBuffer_write succeeded");
    }
    else
    {
        FAIL("fileBuffer_write failed");
    }

    // Clear buffer
    fileBuffer_clear(fb);

    // Read from memory file
    int read_result = fileBuffer_read(fb, fh, 0);
    if (read_result == 0)
    {
        PASS("fileBuffer_read succeeded");

        if (strcmp((char*)fb->buffer, test_data) == 0)
        {
            PASS("Data integrity verified");
        }
        else
        {
            FAIL("Data mismatch");
        }
    }
    else
    {
        FAIL("fileBuffer_read failed (checksum mismatch)");
    }

    VCALL(fh, free);
    destroy_memory_file(mf);
    fileBuffer_destroy(fb);
}

// Test FileBuffer checksum verification
void test_fileBuffer_checksum(void)
{
    printf("\n=== Test fileBuffer_checksum ===\n");

    FileBuffer* fb = fileBuffer_create(4096);
    if (!fb)
    {
        FAIL("FileBuffer creation failed");
        return;
    }

    MemoryFile* mf = create_memory_file(8192);
    FileHandle* fh = create_mem_file_handle(mf);

    // Write data
    const char* test_data = "Checksum Test Data";
    memcpy(fb->buffer, test_data, strlen(test_data) + 1);
    fileBuffer_write(fb, fh, 0);

    // Corrupt the stored data (flip a byte in the data region)
    if (mf->size > (usize)FILE_BUFFER_HEADER_SIZE + 10)
    {
        mf->data[FILE_BUFFER_HEADER_SIZE + 10] ^= 0xFF;
    }

    // Clear and try to read - should fail checksum
    fileBuffer_clear(fb);
    int read_result = fileBuffer_read(fb, fh, 0);

    if (read_result != 0)
    {
        PASS("Checksum verification detected corrupted data");
    }
    else
    {
        FAIL("Checksum verification failed to detect corruption");
    }

    VCALL(fh, free);
    destroy_memory_file(mf);
    fileBuffer_destroy(fb);
}

// Test fileBuffer_clear
void test_fileBuffer_clear(void)
{
    printf("\n=== Test fileBuffer_clear ===\n");

    FileBuffer* fb = fileBuffer_create(4096);
    if (!fb)
    {
        FAIL("FileBuffer creation failed");
        return;
    }

    // Write data
    const char* test_data = "Test Data to Clear";
    memcpy(fb->buffer, test_data, strlen(test_data) + 1);

    // Clear buffer
    fileBuffer_clear(fb);

    // Check if cleared
    bool is_cleared = true;
    for (usize i = 0; i < fb->internal_size; i++)
    {
        if (fb->internal_buf[i] != 0)
        {
            is_cleared = false;
            break;
        }
    }

    if (is_cleared)
    {
        PASS("fileBuffer_clear succeeded");
    }
    else
    {
        FAIL("fileBuffer_clear failed");
    }

    fileBuffer_destroy(fb);
}

// Test multiple read/write operations
void test_fileBuffer_multiple_operations(void)
{
    printf("\n=== Test fileBuffer_multiple_operations ===\n");

    FileBuffer* fb = fileBuffer_create(4096);
    MemoryFile* mf = create_memory_file(16384);
    FileHandle* fh = create_mem_file_handle(mf);

    const char* messages[] = {"First message", "Second message", "Third message"};
    int num_messages = sizeof(messages) / sizeof(messages[0]);

    // Write multiple messages to different locations
    for (int i = 0; i < num_messages; i++)
    {
        fileBuffer_clear(fb);
        memcpy(fb->buffer, messages[i], strlen(messages[i]) + 1);
        fileBuffer_write(fb, fh, i * fb->internal_size);
    }

    // Read and verify each message
    bool all_passed = true;
    for (int i = 0; i < num_messages; i++)
    {
        fileBuffer_clear(fb);
        int result = fileBuffer_read(fb, fh, i * fb->internal_size);

        if (result != 0 || strcmp((char*)fb->buffer, messages[i]) != 0)
        {
            FAIL("Message %d verification failed", i + 1);
            all_passed = false;
        }
    }

    if (all_passed)
    {
        PASS("All %d messages verified successfully", num_messages);
    }

    VCALL(fh, free);
    destroy_memory_file(mf);
    fileBuffer_destroy(fb);
}

// Test FileBuffer with VCALL-based read/write
void test_fileBuffer_vcall_integration(void)
{
    printf("\n=== Test fileBuffer_vcall_integration ===\n");

    FileBuffer* fb = fileBuffer_create(4096);
    MemoryFile* mf = create_memory_file(8192);
    FileHandle* fh = create_mem_file_handle(mf);

    // Use VCALL directly to write raw data
    const char* raw_data = "Raw VCALL write test";
    usize raw_len = strlen(raw_data) + 1;
    int written = VCALL(fh, write_at, raw_data, raw_len, 0);
    if (written == (int)raw_len)
    {
        PASS("VCALL write_at succeeded (%d bytes)", written);
    }
    else
    {
        FAIL("VCALL write_at failed");
    }

    // Read back with VCALL
    char read_buf[64] = {0};
    int read = VCALL(fh, read_at, read_buf, raw_len, 0);
    if (read == (int)raw_len && strcmp(read_buf, raw_data) == 0)
    {
        PASS("VCALL read_at verified data integrity");
    }
    else
    {
        FAIL("VCALL read_at data mismatch");
    }

    VCALL(fh, free);
    destroy_memory_file(mf);
    fileBuffer_destroy(fb);
}

int main(void)
{
    printf("========================================\n");
    printf("   FileBuffer Test Suite\n");
    printf("========================================\n");

    test_fileBuffer_create();
    test_fileBuffer_clear();
    test_fileBuffer_read_write();
    test_fileBuffer_checksum();
    test_fileBuffer_multiple_operations();
    test_fileBuffer_vcall_integration();

    printf("\n========================================\n");
    printf("   Results: %d passed, %d failed\n", pass_count, fail_count);
    printf("========================================\n\n");

    return fail_count > 0 ? 1 : 0;
}
