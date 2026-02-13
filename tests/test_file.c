#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
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

FILE* create_temp_file(const char* filename)
{
    FILE* file = fopen(filename, "w+b");
    if (!file)
    {
        printf("[ERROR] Failed to create temp file: %s\n", filename);
    }
    return file;
}

void cleanup_temp_file(const char* filename)
{
    unlink(filename);
}

void test_file_write_read(void)
{
    printf("\n=== Test file_write_read ===\n");

    const char* test_file = "test_write_read.bin";
    FILE* file = create_temp_file(test_file);
    if (!file)
    {
        FAIL("Cannot create test file");
        return;
    }

    FileHandle* fh = (FileHandle*)create_filesystem_handle_from_FILE(file);
    if (!fh)
    {
        FAIL("Cannot create FileHandle");
        fclose(file);
        cleanup_temp_file(test_file);
        return;
    }

    const char* write_data = "Hello, FileHandle Test!";
    usize write_size = strlen(write_data) + 1;

    int written = VCALL(fh, write, write_data, write_size);
    if (written == (int)write_size)
    {
        PASS("Written %d bytes: \"%s\"", written, write_data);
    }
    else
    {
        FAIL("Write failed, expected %zu bytes, got %d", write_size, written);
    }

    rewind(((FileSystemHandle*)fh)->file);

    char read_buffer[256] = {0};
    int read_bytes = VCALL(fh, read, read_buffer, write_size);

    if (read_bytes == (int)write_size)
    {
        PASS("Read %d bytes: \"%s\"", read_bytes, read_buffer);

        if (strcmp(read_buffer, write_data) == 0)
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
        FAIL("Read failed, expected %zu bytes, got %d", write_size, read_bytes);
    }

    VCALL(fh, free);
    cleanup_temp_file(test_file);
}

void test_file_write_at_read_at(void)
{
    printf("\n=== Test file_write_at_read_at ===\n");

    const char* test_file = "test_write_at_read_at.bin";
    FILE* file = create_temp_file(test_file);
    if (!file)
    {
        FAIL("Cannot create test file");
        return;
    }

    FileHandle* fh = (FileHandle*)create_filesystem_handle_from_FILE(file);
    if (!fh)
    {
        FAIL("Cannot create FileHandle");
        fclose(file);
        cleanup_temp_file(test_file);
        return;
    }

    const char* data1 = "Position_0";
    const char* data2 = "Position_100";
    const char* data3 = "Position_200";

    int w1 = VCALL(fh, write_at, data1, strlen(data1) + 1, 0);
    int w2 = VCALL(fh, write_at, data2, strlen(data2) + 1, 100);
    int w3 = VCALL(fh, write_at, data3, strlen(data3) + 1, 200);

    if (w1 > 0 && w2 > 0 && w3 > 0)
    {
        PASS("Written data at positions 0, 100, 200");
    }
    else
    {
        FAIL("Write_at failed");
    }

    char buffer1[64] = {0};
    char buffer2[64] = {0};
    char buffer3[64] = {0};

    int r1 = VCALL(fh, read_at, buffer1, strlen(data1) + 1, 0);
    int r2 = VCALL(fh, read_at, buffer2, strlen(data2) + 1, 100);
    int r3 = VCALL(fh, read_at, buffer3, strlen(data3) + 1, 200);

    if (r1 > 0 && strcmp(buffer1, data1) == 0)
    {
        PASS("Read from position 0: \"%s\"", buffer1);
    }
    else
    {
        FAIL("Read from position 0 failed");
    }

    if (r2 > 0 && strcmp(buffer2, data2) == 0)
    {
        PASS("Read from position 100: \"%s\"", buffer2);
    }
    else
    {
        FAIL("Read from position 100 failed");
    }

    if (r3 > 0 && strcmp(buffer3, data3) == 0)
    {
        PASS("Read from position 200: \"%s\"", buffer3);
    }
    else
    {
        FAIL("Read from position 200 failed");
    }

    VCALL(fh, free);
    cleanup_temp_file(test_file);
}

void test_file_overwrite(void)
{
    printf("\n=== Test file_overwrite ===\n");

    const char* test_file = "test_overwrite.bin";
    FILE* file = create_temp_file(test_file);
    if (!file)
    {
        FAIL("Cannot create test file");
        return;
    }

    FileHandle* fh = (FileHandle*)create_filesystem_handle_from_FILE(file);
    if (!fh)
    {
        FAIL("Cannot create FileHandle");
        fclose(file);
        cleanup_temp_file(test_file);
        return;
    }

    const char* original_data = "Original Data Here!";
    VCALL(fh, write_at, original_data, strlen(original_data) + 1, 0);

    const char* new_data = "REPLACED";
    VCALL(fh, write_at, new_data, strlen(new_data), 0);

    char buffer[64] = {0};
    VCALL(fh, read_at, buffer, strlen(original_data) + 1, 0);

    if (strncmp(buffer, new_data, strlen(new_data)) == 0)
    {
        PASS("Overwrite successful");
    }
    else
    {
        FAIL("Overwrite failed");
    }

    VCALL(fh, free);
    cleanup_temp_file(test_file);
}

void test_file_random_access(void)
{
    printf("\n=== Test file_random_access ===\n");

    const char* test_file = "test_random_access.bin";
    FILE* file = create_temp_file(test_file);
    if (!file)
    {
        FAIL("Cannot create test file");
        return;
    }

    FileHandle* fh = (FileHandle*)create_filesystem_handle_from_FILE(file);
    if (!fh)
    {
        FAIL("Cannot create FileHandle");
        fclose(file);
        cleanup_temp_file(test_file);
        return;
    }

    struct TestData
    {
        usize position;
        const char* data;
    } test_cases[] = {{500, "Data at 500"},
                      {1024, "Data at 1024"},
                      {50, "Data at 50"},
                      {2000, "Data at 2000"},
                      {10, "Data at 10"}};

    int num_cases = sizeof(test_cases) / sizeof(test_cases[0]);

    for (int i = 0; i < num_cases; i++)
    {
        VCALL(fh, write_at, test_cases[i].data,
              strlen(test_cases[i].data) + 1, test_cases[i].position);
    }

    bool all_verified = true;
    for (int i = 0; i < num_cases; i++)
    {
        char buffer[64] = {0};
        int read_bytes = VCALL(fh, read_at, buffer, strlen(test_cases[i].data) + 1,
                               test_cases[i].position);

        if (read_bytes <= 0 || strcmp(buffer, test_cases[i].data) != 0)
        {
            all_verified = false;
        }
    }

    if (all_verified)
    {
        PASS("All random access operations verified (%d positions)", num_cases);
    }
    else
    {
        FAIL("Some random access operations failed");
    }

    VCALL(fh, free);
    cleanup_temp_file(test_file);
}

void test_file_sequential_read(void)
{
    printf("\n=== Test file_sequential_read ===\n");

    const char* test_file = "test_sequential.bin";
    FILE* file = create_temp_file(test_file);
    if (!file)
    {
        FAIL("Cannot create test file");
        return;
    }

    FileHandle* fh = (FileHandle*)create_filesystem_handle_from_FILE(file);
    if (!fh)
    {
        FAIL("Cannot create FileHandle");
        fclose(file);
        cleanup_temp_file(test_file);
        return;
    }

    const char* data1 = "Block1";
    const char* data2 = "Block2";
    const char* data3 = "Block3";

    VCALL(fh, write, data1, strlen(data1) + 1);
    VCALL(fh, write, data2, strlen(data2) + 1);
    VCALL(fh, write, data3, strlen(data3) + 1);

    rewind(((FileSystemHandle*)fh)->file);

    char buffer1[16] = {0};
    char buffer2[16] = {0};
    char buffer3[16] = {0};

    int r1 = VCALL(fh, read, buffer1, strlen(data1) + 1);
    int r2 = VCALL(fh, read, buffer2, strlen(data2) + 1);
    int r3 = VCALL(fh, read, buffer3, strlen(data3) + 1);

    if (r1 > 0 && strcmp(buffer1, data1) == 0)
    {
        PASS("Sequential read block 1: \"%s\"", buffer1);
    }
    else
    {
        FAIL("Sequential read block 1 failed");
    }

    if (r2 > 0 && strcmp(buffer2, data2) == 0)
    {
        PASS("Sequential read block 2: \"%s\"", buffer2);
    }
    else
    {
        FAIL("Sequential read block 2 failed");
    }

    if (r3 > 0 && strcmp(buffer3, data3) == 0)
    {
        PASS("Sequential read block 3: \"%s\"", buffer3);
    }
    else
    {
        FAIL("Sequential read block 3 failed");
    }

    VCALL(fh, free);
    cleanup_temp_file(test_file);
}

void test_file_edge_cases(void)
{
    printf("\n=== Test file_edge_cases ===\n");

    const char* test_file = "test_edge_cases.bin";
    FILE* file = create_temp_file(test_file);
    if (!file)
    {
        FAIL("Cannot create test file");
        return;
    }

    FileHandle* fh = (FileHandle*)create_filesystem_handle_from_FILE(file);
    if (!fh)
    {
        FAIL("Cannot create FileHandle");
        fclose(file);
        cleanup_temp_file(test_file);
        return;
    }

    // Test 1: Write then immediate read at same position
    const char* test_data = "Immediate Read Test";
    VCALL(fh, write_at, test_data, strlen(test_data) + 1, 0);

    char buffer[64] = {0};
    int read_bytes = VCALL(fh, read_at, buffer, strlen(test_data) + 1, 0);

    if (read_bytes > 0 && strcmp(buffer, test_data) == 0)
    {
        PASS("Immediate read after write successful");
    }
    else
    {
        FAIL("Immediate read after write failed");
    }

    // Test 2: Zero-byte read
    int zero_read = VCALL(fh, read_at, buffer, 0, 0);
    if (zero_read == 0)
    {
        PASS("Zero-byte read handled correctly");
    }
    else
    {
        FAIL("Zero-byte read returned %d", zero_read);
    }

    // Test 3: Zero-byte write
    int zero_write = VCALL(fh, write_at, "", 0, 0);
    if (zero_write == 0)
    {
        PASS("Zero-byte write handled correctly");
    }
    else
    {
        FAIL("Zero-byte write returned %d", zero_write);
    }

    VCALL(fh, free);
    cleanup_temp_file(test_file);
}

void test_file_sync(void)
{
    printf("\n=== Test file_sync ===\n");

    const char* test_file = "test_sync.bin";
    FILE* file = create_temp_file(test_file);
    if (!file)
    {
        FAIL("Cannot create test file");
        return;
    }

    FileHandle* fh = (FileHandle*)create_filesystem_handle_from_FILE(file);
    if (!fh)
    {
        FAIL("Cannot create FileHandle");
        fclose(file);
        cleanup_temp_file(test_file);
        return;
    }

    const char* data = "Sync test data";
    VCALL(fh, write, data, strlen(data) + 1);
    VCALL(fh, sync);

    // Verify data persisted after sync
    rewind(((FileSystemHandle*)fh)->file);
    char buffer[64] = {0};
    int r = VCALL(fh, read, buffer, strlen(data) + 1);
    if (r > 0 && strcmp(buffer, data) == 0)
    {
        PASS("Data intact after sync");
    }
    else
    {
        FAIL("Data corrupted after sync");
    }

    VCALL(fh, free);
    cleanup_temp_file(test_file);
}

int main(void)
{
    printf("========================================\n");
    printf("   FileHandle File Operations Test Suite\n");
    printf("========================================\n");

    test_file_write_read();
    test_file_write_at_read_at();
    test_file_overwrite();
    test_file_random_access();
    test_file_sequential_read();
    test_file_edge_cases();
    test_file_sync();

    printf("\n========================================\n");
    printf("   Results: %d passed, %d failed\n", pass_count, fail_count);
    printf("========================================\n\n");

    return fail_count > 0 ? 1 : 0;
}
