#include "storage.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include "hash.h"
#include "interface.h"
#include "vb_type.h"
#include "vector.h"

static Block* create_block(block_id_t block_id)
{
    Block* block = (Block*)malloc(sizeof(Block));
    if (!block)
    {
        return NULL;  // 内存分配失败
    }
    block->id = block_id;  // 分配新的块 ID
    block->fb = FileBuffer_create(BLOCK_SIZE);  // 创建新的 FileBuffer
    if (!block->fb)
    {
        free(block);
        return NULL;  // FileBuffer 创建失败
    }
    return block;
}

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
    fb->internal_buf = (u8*)base_ptr;         // 从分配起点到对齐位置的内存地址偏移
    fb->internal_size = bufsiz;           // 内部缓冲区总大小
    fb->buffer = (u8*)base_ptr + FILE_BUFFER_HEADER_SIZE;  // 用户数据区起始偏移
    fb->size = bufsiz - FILE_BUFFER_HEADER_SIZE;  // 用户可写区域大小
    return fb;
}

int FileBuffer_read(FileBuffer* fb, FileHandle* handle, usize location)
{
    void* internal_buffer = (void*)(fb->internal_buf);
    // 从Disk读取数据（使用虚表）
    VCALL(handle, read_at, internal_buffer, fb->internal_size, location);
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
    // 计算校验和
    u64 sum = checksum((u8*)fb->buffer, fb->size);
    // 写入校验和
    *((u64*)fb->internal_buf) = sum;
    // 写入数据（使用虚表）
    VCALL(handle, write_at, fb->internal_buf, fb->internal_size, location);
    // handle->vtable->write_at(handle->handle, fb->internal_buf, fb->internal_size, location);
    //  校验成功，返回0
    return 0;
}

void FileBuffer_clear(FileBuffer* fb)
{
    memset((void*)(fb->internal_buf), 0, fb->internal_size);
}

void FileBuffer_destroy(FileBuffer* buffer)
{
    if (buffer)
    {
        free(buffer);
    }
}

static void file_free(FileHandle* handle)
{
    FileSystemHandle* fs_handle = (FileSystemHandle*)handle;
    if (fs_handle->file)
    {
        fclose(fs_handle->file);
    }
    free(fs_handle);
}

// 为 FILE* 类型实现具体的读写函数，模拟实现 FileHandle Trait
static int file_read(FileHandle* handle, void* buffer, usize size)
{
    FileSystemHandle* fs_handle = (FileSystemHandle*)handle;
    return fread(buffer, 1, size, fs_handle->file);
}

static int file_read_at(FileHandle* handle, void* buffer, usize size, usize location)
{
    FileSystemHandle* fs_handle = (FileSystemHandle*)handle;
    if (fseek(fs_handle->file, location, SEEK_SET) != 0) return -1;
    return fread(buffer, 1, size, fs_handle->file);
}

static int file_write(FileHandle* handle, const void* buffer, usize size)
{
    FileSystemHandle* fs_handle = (FileSystemHandle*)handle;
    return fwrite(buffer, 1, size, fs_handle->file);
}

static int file_write_at(FileHandle* handle, const void* buffer, usize size, usize location)
{
    FileSystemHandle* fs_handle = (FileSystemHandle*)handle;
    if (fseek(fs_handle->file, location, SEEK_SET) != 0) return -1;
    return fwrite(buffer, 1, size, fs_handle->file);
}

static void file_sync(FileHandle* handle)
{
    FileSystemHandle* fs_handle = (FileSystemHandle*)handle;
    if (fs_handle->file)
    {
        fflush(fs_handle->file);
    }
}

/**
 * @brief 同步文件句柄
 *
 * 调用虚表中的 sync 函数来同步文件数据到磁盘。
 *
 * @param fh FileHandle 指针
 */
void FileHandle_sync(FileHandle* fh)
{
    if (fh)
    {
        VCALL(fh, sync);
    }
}

// FILE* 类型的虚函数表实例（静态全局变量）
static const FileHandleVTable file_vtable = {.free = file_free,
                                             .read = file_read,
                                             .write = file_write,
                                             .read_at = file_read_at,
                                             .write_at = file_write_at,
                                             .sync = file_sync};

/**
 * @brief 从 FILE* 创建一个 FileHandle
 *
 * 该函数会创建一个 FileHandle，用于管理指定的 FILE* 类型文件句柄。
 * 使用预定义的 file_vtable。
 *
 * @param file FILE* 类型文件句柄，指向要管理的文件
 * @return FileHandle* 新创建的 FileHandle 指针
 */
FileSystemHandle* create_filesystem_handle_from_FILE(FILE* file)
{
    FileSystemHandle* file_handle = (FileSystemHandle*)malloc(sizeof(FileSystemHandle));
    if (!file_handle)
    {
        return NULL;  // 内存分配失败
    }
    file_handle->file = file;
    file_handle->base.vtable = &file_vtable;
    return file_handle;
}

static void MetaBlockReader_read_new_block(MetaBlockReader* reader, block_id_t block_id)
{
    if (!reader || !reader->manager)
    {
        return;
    }

    Block* block = reader->block;
    block->id = block_id;
    // 读取指定块
    VCALL(reader->manager, read, block);
    reader->next_block_id = *((block_id_t*)block->fb->buffer);
    reader->offset = sizeof(block_id_t);
}

static void init_meteblock_reader(MetaBlockReader* reader, BlockManager* manager,
                                  block_id_t block_id)
{
    reader->manager = manager;
    reader->block = (Block*)malloc(sizeof(Block));
    reader->block->fb = FileBuffer_create(BLOCK_SIZE);
    reader->offset = 0;
    reader->next_block_id = -1;
    MetaBlockReader_read_new_block(reader, block_id);
}

void MetaBlockReader_read_data(MetaBlockReader* reader, data_ptr_t buffer, usize read_size)
{
    Block* block = reader->block;
    while (reader->offset + read_size > block->fb->size)
    {
        // cannot read entire entry from block
        // first read what we can from this block
        usize to_read = block->fb->size - reader->offset;
        if (to_read > 0)
        {
            memcpy(buffer, block->fb->buffer + reader->offset, to_read);
            read_size -= to_read;
            buffer += to_read;
        }
        // then move to the next block
        MetaBlockReader_read_new_block(reader, reader->next_block_id);
    }
    // we have enough left in this block to read from the buffer
    memcpy(buffer, block->fb->buffer + reader->offset, read_size);
    reader->offset += read_size;
}

void MetaBlockReader_destroy(MetaBlockReader* reader)
{
    if (!reader)
    {
        return;
    }
    if (reader->block)
    {
        FileBuffer_destroy(reader->block->fb);
        free(reader->block);
    }
}

static void init_meteblock_writer(MetaBlockWriter* writer, BlockManager* manager)
{
    writer->manager = manager;
    writer->block = VCALL(manager, create_block);
    writer->offset = sizeof(block_id_t);
}

static void MetaBlockWriter_flush(MetaBlockWriter* writer)
{
    // flush the block to disk
    Block* block = writer->block;
    // 添加注释 ： 只有当 offset 大于 sizeof(block_id_t) 时，才需要 flush 到磁盘
    // 这是因为 block_id_t 存储在 block 的开始位置
    if (writer->offset > sizeof(block_id_t))
    {
        VCALL(writer->manager, write, block);
        writer->offset = sizeof(block_id_t);
    }
}

static void MetaBlockWriter_destroy(MetaBlockWriter* writer)
{
    if (!writer)
    {
        return;
    }
    MetaBlockWriter_flush(writer);
    if (writer->block)
    {
        FileBuffer_destroy(writer->block->fb);
        free(writer->block);
    }
}

void MetaBlockWriter_write_data(MetaBlockWriter* writer, data_ptr_t buffer, usize write_size)
{
    usize offset = writer->offset;
    Block* block = writer->block;
    while (offset + write_size > block->fb->size)
    {
        // we need to make a new block
        // first copy what we can
        assert(offset <= block->fb->size);
        usize copy_amount = block->fb->size - offset;
        if (copy_amount > 0)
        {
            memcpy(block->fb->buffer + offset, buffer, copy_amount);
            buffer += copy_amount;
            offset += copy_amount;
            write_size -= copy_amount;
        }
        writer->offset = offset;
        // now we need to get a new block id
        block_id_t new_block_id = VCALL(writer->manager, get_free_block_id);
        // write the block id of the new block to the start of the current block
        *((block_id_t*)block->fb->buffer) = new_block_id;
        // first flush the old block
        MetaBlockWriter_flush(writer);
        // now update the block id of the lbock
        block->id = new_block_id;
    }
    memcpy(block->fb->buffer + offset, buffer, write_size);
    offset += write_size;
    writer->offset = offset;
}

void destory_single_manager(SingleFileBlockManager* manager)
{
    if (!manager)
    {
        return;
    }

    // 关闭文件并释放 FileHandle
    if (manager->file_handle)
    {
        VCALL(manager->file_handle, free);
    }

    // 释放文件路径
    if (manager->file_path)
    {
        free(manager->file_path);
    }

    if (manager->header_buffer)
    {
        FileBuffer_destroy(manager->header_buffer);
    }
    // 释放 used_blocks 和 free_list
    Vector_deinit(&manager->used_blocks);
    Vector_deinit(&manager->free_list);

    // 释放 manager 本身
    free(manager);
}

static void single_file_block_manager_read(BlockManager* self, Block* block)
{
    SingleFileBlockManager* manager = (SingleFileBlockManager*)self;
    assert(block->id >= 0);
    // 记录读取的块 → 这些是旧检查点的块
    Vector_push_back(&manager->used_blocks, &block->id);
    FileBuffer_read(block->fb, manager->file_handle, BLOCK_START + block->id * BLOCK_SIZE);
}

static void single_file_block_manager_write(BlockManager* self, Block* block)
{
    SingleFileBlockManager* manager = (SingleFileBlockManager*)self;
    assert(block->id >= 0);
    FileBuffer_write(block->fb, manager->file_handle, BLOCK_START + block->id * BLOCK_SIZE);
}

static block_id_t single_file_block_manager_get_free_block_id(BlockManager* self)
{
    SingleFileBlockManager* manager = (SingleFileBlockManager*)self;
    if (manager->free_list.size > 0)
    {
        block_id_t block_id;
        Vector_pop_back(&manager->free_list, &block_id);
        return block_id;
    }
    return manager->max_block++;
}

static Block* single_file_block_manager_create_block(BlockManager* self)
{
    SingleFileBlockManager* manager = (SingleFileBlockManager*)self;
    Block* block = create_block(single_file_block_manager_get_free_block_id(self));
    return block;
}

/*
  旧数据块 11:
  ┌─────────────────────────┐
  │ [1, 2]                  │  ← 原始数据
  └─────────────────────────┘
           ↓ Read()
           ↓ used_blocks.push_back(11)
           ↓
        [内存]
  ┌─────────────────────────┐
  │ Block(11)               │
  │ buffer: [1, 2]          │  ← 读取到内存
  └─────────────────────────┘
           ↓ Scan()
           ↓ 返回数据
           ↓
        [内存]
  ┌─────────────────────────┐
  │ DataChunk               │
  │ data: [1, 2]            │  ← 扫描结果
  └─────────────────────────┘
           ↓ WriteColumnData()
           ↓
        [内存]
  ┌─────────────────────────┐
  │ Block(22)               │
  │ buffer: [1, 2]          │  ← 新块，相同内容
  └─────────────────────────┘
           ↓ Write()
           ↓
  新数据块 22:
  ┌─────────────────────────┐
  │ [1, 2]                  │  ← 写入磁盘
  └─────────────────────────┘

  结果：
    块11: [1, 2]  ← 旧数据（可回收）
    块22: [1, 2]  ← 新数据（活动）

    free_list = [11]
*/
  /**
 * WriteHeader - 写入数据库头部（实现原子性检查点）
 *
 * 功能：将数据库的当前状态（元数据块位置、空闲块列表等）原子性地写入磁盘
 *
 * 关键设计：
 * 1. 双缓冲头部（H1和H2）- 保证崩溃时至少有一个完整的头部
 * 2. 迭代计数 - 通过比较H1和H2的iteration，选择最新的有效头部
 * 3. 空闲块管理 - 将上次检查点使用的块标记为可重用
 *
 * @param header: 要写入的数据库头部（包含meta_block等信息）
 */
static void single_file_block_manager_write_header(BlockManager* self, DatabaseHeader header)
{
    SingleFileBlockManager* manager = DOWNCAST(self, SingleFileBlockManager);
    header.iteration = ++manager->iteration_count;
    header.block_count = manager->max_block;
    if (manager->used_blocks.size > 0)
    {
        MetaBlockWriter writer;
        init_meteblock_writer(&writer, self);
        header.free_list_id = writer.block->id;
        Serializer_write(&writer, (data_ptr_t)&manager->used_blocks.size, sizeof(u64));
        // Read 的块会放进 free_list，因为 Checkpoint
        // 会把这些数据重新写入到新块中，旧块的内容已经被复制了
        // checkpoint manager 是全量复制 Copy-Everything Checkpoint 策略
        for (u64 i = 0; i < manager->used_blocks.size; i++)
        {
            block_id_t* block_id = Vector_get(&manager->used_blocks, i);
            Serializer_write(&writer, (data_ptr_t)block_id, sizeof(block_id_t));
        }
        MetaBlockWriter_flush(&writer);
        MetaBlockWriter_destroy(&writer);
    }
    else
    {
        header.free_list_id = INVALID_BLOCK;
    }
    FileBuffer* header_buffer = manager->header_buffer;
    FileBuffer_clear(header_buffer);
    *((DatabaseHeader*)header_buffer->buffer) = header;
    FileBuffer_write(header_buffer, manager->file_handle,
                     manager->active_header == 1 ? HEADER_SIZE : HEADER_SIZE * 2);
                     // 写入 H2 时，偏移量为 HEADER_SIZE * 2
    manager->active_header = 1 - manager->active_header;// 切换到 H2
    FileHandle_sync(manager->file_handle);
    Vector_deinit(&manager->free_list);
    manager->free_list = manager->used_blocks;
    Vector_init(&manager->used_blocks, sizeof(block_id_t), 0);
}

static void single_file_block_manager_destroy(BlockManager* self)
{
    SingleFileBlockManager* manager = (SingleFileBlockManager*)self;
    destory_single_manager(manager);
}

  // 虚函数表实例
static BlockManagerVTable single_file_block_manager_vtable = {
    .read = single_file_block_manager_read,
    .write = single_file_block_manager_write,
    .create_block = single_file_block_manager_create_block,
    .destroy = single_file_block_manager_destroy,
    .get_free_block_id = single_file_block_manager_get_free_block_id,
    .write_header = single_file_block_manager_write_header};

static void initialize_manager(SingleFileBlockManager* manager, DatabaseHeader* header)
{
    if (header->free_list_id != INVALID_BLOCK)
    {
        MetaBlockReader reader = {0};
        init_meteblock_reader(&reader, (BlockManager*)manager, header->free_list_id);
        u64 free_list_count = 0;
        Deserializer_read(&reader, (data_ptr_t)&free_list_count, sizeof(u64));
        Vector_reserve(&manager->free_list, free_list_count);
        for (u64 i = 0; i < free_list_count; i++)
        {
            block_id_t block_id = 0;
            Deserializer_read(&reader, (data_ptr_t)&block_id, sizeof(block_id_t));
            Vector_push_back(&manager->free_list, &block_id);
        }
        MetaBlockReader_destroy(&reader);
    }
    manager->meta_block = header->meta_block;
    manager->iteration_count = header->iteration;
    manager->max_block = header->block_count;
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
    memset(manager, 0, sizeof(SingleFileBlockManager));

    // 初始化 BlockManager 基类字段
    manager->base.vtable = &single_file_block_manager_vtable;
    manager->base.type = BLOCK_MANAGER_SINGLE_FILE;
    manager->file_path = strdup(path);
    Vector used_blocks = {0};
    Vector_init(&used_blocks, sizeof(block_id_t), 0);
    manager->used_blocks = used_blocks;
    Vector free_list = {0};
    Vector_init(&free_list, sizeof(block_id_t), 0);
    manager->free_list = free_list;
    FileBuffer* header_buffer = FileBuffer_create(HEADER_SIZE);
    if (!header_buffer)
    {
        single_file_block_manager_destroy((BlockManager*)manager);
        return NULL;  // FileBuffer 创建失败
    }
    manager->header_buffer = header_buffer;  // 将 FileBuffer 结构体内容

    FILE* file = fopen(path, "w+b");  // 以读写模式创建新文件
    if (!file)
    {
        single_file_block_manager_destroy((BlockManager*)manager);
        return NULL;  // 文件创建失败
    }

    FileSystemHandle* file_handle = create_filesystem_handle_from_FILE(file);
    if (!file_handle)
    {
        fclose(file);
        single_file_block_manager_destroy((BlockManager*)manager);
        return NULL;  // 文件句柄创建失败
    }
    manager->file_handle = (FileHandle*)file_handle;  // 将 FileHandle 结构体内容
    if (create_new)
    {
        FileBuffer_clear(header_buffer);
        MasterHeader* meta_header = (MasterHeader*)header_buffer->buffer;
        meta_header->version = VERSION_NUMBER;
        FileBuffer_write(header_buffer, (FileHandle*)file_handle, 0);

        FileBuffer_clear(header_buffer);
        DatabaseHeader* db_header = (DatabaseHeader*)header_buffer->buffer;
        // header 1
        db_header->iteration = 0;
        db_header->meta_block = INVALID_BLOCK;
        db_header->free_list_id = INVALID_BLOCK;
        db_header->block_count = 0;
        FileBuffer_write(header_buffer, (FileHandle*)file_handle, HEADER_SIZE);
        // header 2
        db_header->iteration = 1;
        FileBuffer_write(header_buffer, (FileHandle*)file_handle, HEADER_SIZE * 2);
        FileHandle_sync((FileHandle*)file_handle);
        manager->active_header = 1;  // 新文件默认 header 2 为活跃
        manager->max_block = 0;  // 新文件默认最大块编号为 2
        manager->meta_block = INVALID_BLOCK;
    }
    else
    {
        MasterHeader header;
        FileBuffer_read(header_buffer, (FileHandle*)file_handle, 0);
        if (header.version != VERSION_NUMBER)
        {
            single_file_block_manager_destroy((BlockManager*)manager);
            return NULL;  // 版本不匹配
        }
        DatabaseHeader db_header1, db_header2;
        FileBuffer_read(header_buffer, (FileHandle*)file_handle, HEADER_SIZE);
        db_header1 = *(DatabaseHeader*)header_buffer->buffer;
        FileBuffer_read(header_buffer, (FileHandle*)file_handle, HEADER_SIZE * 2);
        db_header2 = *(DatabaseHeader*)header_buffer->buffer;

        if (db_header1.iteration > db_header2.iteration)
        {
            manager->active_header = 0;
            initialize_manager(manager, &db_header1);
        }
        else
        {
            manager->active_header = 1;
            initialize_manager(manager, &db_header2);
        }
    }

    return manager;
}
