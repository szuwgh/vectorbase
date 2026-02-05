#include "storage.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "hash.h"

FileBuffer* FileBuffer_create(usize bufsiz)
{
    // 计算总内存：结构体 + 实际需要的缓冲区大小 + 对齐空间
    size_t total_size = sizeof(FileBuffer) + bufsiz + (FILE_BUFFER_BLOCK_SIZE - 1);
    // 分配内存
    FileBuffer* fb = (FileBuffer*)malloc(total_size);
    if (!fb)
    {
        return NULL; // 内存分配失败
    }
    memset(fb, 0, total_size);
    // 计算对齐偏移量
    u64 buf_ptr = (u64)(fb);
    u64 base_ptr = buf_ptr + sizeof(FileBuffer);
    u64 rem = base_ptr % FILE_BUFFER_BLOCK_SIZE;
    u64 offset = (rem == 0) ? 0 : (FILE_BUFFER_BLOCK_SIZE - rem);

    // 检查偏移量有效性（确保对齐后有足够空间）
    if (offset + bufsiz > total_size)
    {
        free(fb);
        return NULL;
    }
    base_ptr += offset;
    fb->internal_offset = base_ptr;         // 从分配起点到对齐位置的内存地址偏移
    fb->internal_size = bufsiz;           // 内部缓冲区总大小
    fb->buffer_offset = base_ptr + FILE_BUFFER_HEADER_SIZE;  // 用户数据区起始偏移
    fb->size = bufsiz - FILE_BUFFER_HEADER_SIZE;  // 用户可写区域大小
    return fb;
}

int FileBuffer_read(FileBuffer* fb, FileHandle* handle, usize location)
{
    void* internal_buffer = (void*)(fb->internal_offset);
    // 从Disk读取数据
    handle->read_at(handle->handle, internal_buffer, fb->internal_size, location);
    // 获取校验和
    u64 stored_checksum = *((u64*)internal_buffer);
    // 校验校验和
    u64 computed_checksum = checksum((u8*)internal_buffer + FILE_BUFFER_HEADER_SIZE, fb->size);
    if (stored_checksum != computed_checksum)
    {
        return -1;
    }
    // 校验成功，返回0
    return 0;
}

int FileBuffer_write(FileBuffer* fb, FileHandle* handle, usize location)
{
    void* internal_buffer = (void*)(fb->internal_offset);
    void* buffer = (void*)(fb->internal_offset + FILE_BUFFER_HEADER_SIZE);
    // 计算校验和
    u64 sum = checksum((u8*)buffer, fb->size);
    // 写入校验和
    *((u64*)internal_buffer) = sum;
    // 写入数据
    handle->write_at(handle->handle, internal_buffer, fb->size, location);
    // 校验成功，返回0
    return 0;
}

void FileBuffer_clear(FileBuffer* fb)
{
    memset((void*)(fb->internal_offset + fb->buffer_offset), 0, fb->size);
}

static void free_file_handle(void* handle)
{
    if (handle)
    {
        fclose((FILE*)handle);
    }
}

// 为 FILE* 类型实现具体的读写函数，模拟实现 FileHandle Trait
static int file_read(void* handle, void* buffer, usize size)
{
    return fread(buffer, 1, size, (FILE*)handle);
}

static int file_read_at(void* handle, void* buffer, usize size, usize location)
{
    if (fseek((FILE*)handle, location, SEEK_SET) != 0) return -1;
    return fread(buffer, 1, size, (FILE*)handle);
}

static int file_write(void* handle, const void* buffer, usize size)
{
    return fwrite(buffer, 1, size, (FILE*)handle);
}

static int file_write_at(void* handle, const void* buffer, usize size, usize location)
{
    if (fseek((FILE*)handle, location, SEEK_SET) != 0) return -1;
    return fwrite(buffer, 1, size, (FILE*)handle);
}

/**
 * @brief 创建一个 FileHandle
 *
 * 该函数会创建一个 FileHandle，用于管理指定的文件句柄。
 *
 * @param handle 文件句柄，指向要管理的文件
 * @param read 读取函数指针，用于从文件句柄中读取数据
 * @param write 写入函数指针，用于向文件句柄中写入数据
 * @param read_at 读取指定位置函数指针，用于从文件句柄中读取指定位置的数据
 * @param write_at 写入指定位置函数指针，用于向文件句柄中写入指定位置的数据
 * @return FileHandle* 新创建的 FileHandle 指针
 */
FileHandle* create_file_handle(void* handle, int (*read)(void*, void*, usize),
                               int (*write)(void*, const void*, usize),
                               int (*read_at)(void*, void*, usize, usize),
                               int (*write_at)(void*, const void*, usize, usize))
{
    FileHandle* file_handle = (FileHandle*)malloc(sizeof(FileHandle));
    if (!file_handle)
    {
        return NULL;  // 内存分配失败
    }
    file_handle->handle = handle;
    file_handle->read = read;
    file_handle->write = write;
    file_handle->read_at = read_at;
    file_handle->write_at = write_at;
    return file_handle;
}

/**
 * @brief 从 FILE* 创建一个 FileHandle
 *
 * 该函数会创建一个 FileHandle，用于管理指定的 FILE* 类型文件句柄。
 *
 * @param file FILE* 类型文件句柄，指向要管理的文件
 * @return FileHandle* 新创建的 FileHandle 指针
 */
FileHandle* create_file_handle_from_FILE(FILE* file)
{
    return create_file_handle((void*)file, file_read, file_write, file_read_at, file_write_at);
}

/**
 * @brief 创建一个新的数据库文件管理器
 *
 * 该函数会创建一个新的数据库文件管理器，用于管理指定路径下的数据库文件。
 * 如果文件不存在，会创建一个新文件；如果文件已存在，会覆盖原有文件。
 *
 * @param path 数据库文件的路径
 * @param create_new 是否创建新文件，true 表示创建新文件，false 表示覆盖原有文件
 * @return SingleFileBlockManager* 新创建的数据库文件管理器指针
 */
SingleFileBlockManager* create_new_database(const char* path, bool create_new)
{
    SingleFileBlockManager* manager =
        (SingleFileBlockManager*)malloc(sizeof(SingleFileBlockManager));
    if (!manager)
    {
        return NULL;  // 内存分配失败
    }

    FILE* file = fopen(path, "w+b");  // 以读写模式创建新文件
    if (!file)
    {
        free(manager);
        return NULL;  // 文件创建失败
    }

    FileHandle* file_handle = create_file_handle_from_FILE(file);
    if (!file_handle)
    {
        fclose(file);
        free(manager);
        return NULL;  // 文件句柄创建失败
    }

    if (create_new)
    {
        // 写入初始元数据块
        MetaHeader meta_header = {
            .version = 1,
            .flags = {0, 0, 0, 0},
        };
        file_handle->write(file_handle->handle, &meta_header, sizeof(MetaHeader));
    }

    manager->active_header = 0;
    manager->file_path = strdup(path);
    manager->file_handle = file_handle;

    return manager;
}

/**
 * @brief 销毁数据库文件管理器
 *
 * 该函数会销毁指定的数据库文件管理器，释放所有相关资源。
 *
 * @param manager 要销毁的数据库文件管理器指针
 */
void destroy_manager(SingleFileBlockManager* manager)
{
    if (!manager)
    {
        return;
    }

    // 关闭文件并释放 FileHandle
    if (manager->file_handle)
    {
        if (manager->file_handle->handle)
        {
            manager->file_handle->free(manager->file_handle->handle);
        }
        free(manager->file_handle);
    }

    // 释放文件路径
    if (manager->file_path)
    {
        free(manager->file_path);
    }

    // 释放 manager 本身
    free(manager);
}
