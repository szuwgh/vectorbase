#ifndef STORAGE_H
#define STORAGE_H

#include "vb_type.h"
#include <stdbool.h>

#define BLOCK_SIZE             262144 // 256KB
#define HEADER_SIZE            4096   // 数据库头大小
#define FILE_BUFFER_BLOCK_SIZE 4096   // 文件缓冲区块大小
static int FILE_BUFFER_HEADER_SIZE = sizeof(u64);// 文件缓冲区块头大小

typedef struct
{
    u64 version;
    u64 flags[4];
} MetaHeader;

typedef struct
{
  // 迭代计数，每次存储 checkpoint 时增加 1
    u64 iteration;
  // 指向初始元数据块的指针
    block_id_t meta_block;
  // 指向包含空闲列表的块的指针
    block_id_t free_list;
  // 文件中的块数量。如果文件大于 BLOCK_SIZE * block_count，
  // 则出现在 block_count 之后的任何块都隐式地属于 free_list
    u64 block_count;
} DatabaseHeader;

typedef struct
{
    usize size;
    usize internal_offset;
    usize internal_size;
    usize buffer_offset;
    // data数组是柔性数组，它不占用结构体本身的大小，而是紧跟在结构体之后
    u8 data[];
} FileBuffer;

/* FileHandle Trait */
typedef struct
{
    void *handle;
    void (*free)(void *handle);
    int (*read)(void *handle, void *buffer, usize nr_bytes);
    int (*write)(void *handle, const void *buffer, usize nr_bytes);
    int (*read_at)(void *handle, void *buffer, usize nr_bytes, usize location);
    int (*write_at)(void *handle, const void *buffer, usize nr_bytes, usize location);
} FileHandle;

FileBuffer *FileBuffer_create(usize size);
int FileBuffer_read(FileBuffer *buffer, FileHandle *handle, usize location);
int FileBuffer_write(FileBuffer *buffer, FileHandle *handle, usize location);
void FileBuffer_clear(FileBuffer *buffer);
void FileBuffer_destroy(FileBuffer *buffer);

typedef struct
{
    u8 active_header;
    char *file_path;
    FileHandle *file_handle;
} SingleFileBlockManager;

/**
 * @brief 创建一个新的数据库文件管理器
 *
 * 该函数会创建一个新的数据库文件管理器，用于管理指定路径的数据库文件。
 * 如果文件不存在，会创建一个新文件；如果文件已存在，根据 create_new 参数
 * 决定是否覆盖原有文件。
 *
 * @param path 数据库文件的路径
 * @param create_new 是否创建新文件，true 表示创建新文件，false 表示覆盖原有文件
 * @return SingleFileBlockManager* 新创建的数据库文件管理器指针
 */
SingleFileBlockManager *create_new_database(const char *path, bool create_new);

FileHandle *create_file_handle(
    void *handle, int (*read)(void *, void *, usize), int (*write)(void *, const void *, usize),
    int (*read_at)(void *handle, void *buffer, usize nr_bytes, usize location),
    int (*write_at)(void *handle, const void *buffer, usize nr_bytes, usize location));

#endif  // STORAGE_H