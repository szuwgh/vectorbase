#include "storage.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include "catalog.h"
#include "hash.h"
#include "interface.h"
#include "parser.h"
#include "types.h"
#include "vb_type.h"
#include "vector.h"
#include "datatable.h"
#include "operator.h"

Block* Block_create(block_id_t block_id)
{
    Block* block = (Block*)malloc(sizeof(Block));
    if (!block)
    {
        return NULL;  // 内存分配失败
    }
    block->id = block_id;  // 分配新的块 ID
    block->fb = NEW(FileBuffer, BLOCK_SIZE);  // 创建新的 FileBuffer
    if (!block->fb)
    {
        free(block);
        return NULL;  // FileBuffer 创建失败
    }
    return block;
}

void block_destroy(Block* block)
{
    if (!block)
    {
        return;
    }
    if (block->fb)
    {
        fileBuffer_destroy(block->fb);
    }
    free(block);
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

int fileBuffer_read(FileBuffer* fb, FileHandle* handle, usize location)
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

int fileBuffer_write(FileBuffer* fb, FileHandle* handle, usize location)
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

void fileBuffer_clear(FileBuffer* fb)
{
    memset((void*)(fb->internal_buf), 0, fb->internal_size);
}

void fileBuffer_destroy(FileBuffer* buffer)
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
void fileHandle_sync(FileHandle* fh)
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

static void metaBlockReader_read_new_block(MetaBlockReader* reader, block_id_t block_id)
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

void MetaBlockReader_init(MetaBlockReader* reader, BlockManager* manager,
                                 block_id_t block_id)
{
    reader->manager = manager;
    reader->block = (Block*)malloc(sizeof(Block));
    reader->block->fb = NEW(FileBuffer, BLOCK_SIZE);
    reader->offset = 0;
    reader->next_block_id = -1;
    metaBlockReader_read_new_block(reader, block_id);
}

void metaBlockReader_read_data(MetaBlockReader* reader, data_ptr_t buffer, usize read_size)
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
        metaBlockReader_read_new_block(reader, reader->next_block_id);
    }
    // we have enough left in this block to read from the buffer
    memcpy(buffer, block->fb->buffer + reader->offset, read_size);
    reader->offset += read_size;
}

void metaBlockReader_deinit(MetaBlockReader* reader)
{
    if (!reader)
    {
        return;
    }
    if (reader->block)
    {
        block_destroy(reader->block);
    }
}

void metaBlockReader_destroy(MetaBlockReader* reader)
{
    if (!reader)
    {
        return;
    }
    metaBlockReader_deinit(reader);
    free(reader);
}

static void MetaBlockWriter_init(MetaBlockWriter* writer, BlockManager* manager)
{
    writer->manager = manager;
    writer->block = VCALL(manager, create_block);
    writer->offset = sizeof(block_id_t);
}

MetaBlockWriter* MetaBlockWriter_create(BlockManager* manager)
{
    MetaBlockWriter* writer = (MetaBlockWriter*)malloc(sizeof(MetaBlockWriter));
    MetaBlockWriter_init(writer, manager);
    return writer;
}

void metaBlockWriter_flush(MetaBlockWriter* writer)
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

static void metaBlockWriter_deinit(MetaBlockWriter* writer)
{
    if (!writer)
    {
        return;
    }
    metaBlockWriter_flush(writer);
    if (writer->block)
    {
        fileBuffer_destroy(writer->block->fb);
        free(writer->block);
        writer->block = NULL;
    }
}

void metaBlockWriter_destroy(MetaBlockWriter* writer)
{
    if (!writer)
    {
        return;
    }
    metaBlockWriter_deinit(writer);
    free(writer);
}

void metaBlockWriter_write_data(MetaBlockWriter* writer, data_ptr_t buffer, usize write_size)
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
        metaBlockWriter_flush(writer);
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
        fileBuffer_destroy(manager->header_buffer);
    }
    // 释放 used_blocks 和 free_list
    vector_deinit(&manager->used_blocks);
    vector_deinit(&manager->free_list);

    // 释放 manager 本身
    free(manager);
}

static void single_file_block_manager_read(BlockManager* self, Block* block)
{
    SingleFileBlockManager* manager = (SingleFileBlockManager*)self;
    assert(block->id >= 0);
    // 记录读取的块 → 这些是旧检查点的块
    vector_push_back(&manager->used_blocks, &block->id);
    fileBuffer_read(block->fb, manager->file_handle, BLOCK_START + block->id * BLOCK_SIZE);
}

static void single_file_block_manager_write(BlockManager* self, Block* block)
{
    SingleFileBlockManager* manager = (SingleFileBlockManager*)self;
    assert(block->id >= 0);
    fileBuffer_write(block->fb, manager->file_handle, BLOCK_START + block->id * BLOCK_SIZE);
}

static block_id_t single_file_block_manager_get_free_block_id(BlockManager* self)
{
    SingleFileBlockManager* manager = (SingleFileBlockManager*)self;
    if (manager->free_list.size > 0)
    {
        block_id_t block_id;
        vector_pop_back(&manager->free_list, &block_id);
        return block_id;
    }
    return manager->max_block++;
}

static block_id_t single_file_block_manager_get_frist_meta_block(BlockManager* self)
{
    SingleFileBlockManager* manager = (SingleFileBlockManager*)self;
    return manager->meta_block;
}

static Block* single_file_block_manager_create_block(BlockManager* self)
{
    SingleFileBlockManager* manager = (SingleFileBlockManager*)self;
    Block* block = Block_create(single_file_block_manager_get_free_block_id(self));
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
        MetaBlockWriter_init(&writer, self);
        header.free_list_id = writer.block->id;
        SERIALIZER_WRITE_TYPE(&writer, (data_ptr_t)&manager->used_blocks.size, usize);
        // Read 的块会放进 free_list，因为 Checkpoint
        // 会把这些数据重新写入到新块中，旧块的内容已经被复制了
        // checkpoint manager 是全量复制 Copy-Everything Checkpoint 策略
        for (u64 i = 0; i < manager->used_blocks.size; i++)
        {
            block_id_t* block_id = vector_get(&manager->used_blocks, i);
            SERIALIZER_WRITE_TYPE(&writer, (data_ptr_t)block_id, block_id_t);
        }
        metaBlockWriter_deinit(&writer);
    }
    else
    {
        header.free_list_id = INVALID_BLOCK;
    }
    FileBuffer* header_buffer = manager->header_buffer;
    fileBuffer_clear(header_buffer);
    *((DatabaseHeader*)header_buffer->buffer) = header;
    fileBuffer_write(header_buffer, manager->file_handle,
                     manager->active_header == 1 ? HEADER_SIZE : HEADER_SIZE * 2);
                     // 写入 H2 时，偏移量为 HEADER_SIZE * 2
    manager->active_header = 1 - manager->active_header;// 切换到 H2
    manager->meta_block = header.meta_block;
    fileHandle_sync(manager->file_handle);
    vector_deinit(&manager->free_list);
    manager->free_list = manager->used_blocks;
    manager->used_blocks = VEC(block_id_t, 0);
}

static void single_file_block_manager_destroy(BlockManager* self)
{
    SingleFileBlockManager* manager = (SingleFileBlockManager*)self;
    destory_single_manager(manager);
}

  // 虚函数表实例
static BlockManagerVTable single_file_block_manager_vtable = {
    VTABLE_ENTRY(read, single_file_block_manager_read),
    VTABLE_ENTRY(write, single_file_block_manager_write),
    VTABLE_ENTRY(create_block, single_file_block_manager_create_block),
    VTABLE_ENTRY(destroy, single_file_block_manager_destroy),
    VTABLE_ENTRY(get_free_block_id, single_file_block_manager_get_free_block_id),
    VTABLE_ENTRY(get_frist_meta_block, single_file_block_manager_get_frist_meta_block),
    VTABLE_ENTRY(write_header, single_file_block_manager_write_header)};

static void initialize_manager(SingleFileBlockManager* manager, DatabaseHeader* header)
{
    if (header->free_list_id != INVALID_BLOCK)
    {
        MetaBlockReader reader =
            MAKE(MetaBlockReader, (BlockManager*)manager, header->free_list_id);
      //  MetaBlockReader_init(&reader, (BlockManager*)manager, header->free_list_id);
        u64 free_list_count = 0;
        DESERIALIZER_READ(&reader, (data_ptr_t)&free_list_count, sizeof(u64));
        vector_reserve(&manager->free_list, free_list_count);
        for (u64 i = 0; i < free_list_count; i++)
        {
            block_id_t block_id = 0;
            DESERIALIZER_READ(&reader, (data_ptr_t)&block_id, sizeof(block_id_t));
            vector_push_back(&manager->free_list, &block_id);
        }
        metaBlockReader_deinit(&reader);
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
    Vector used_blocks = VEC(block_id_t, 0);
    manager->used_blocks = used_blocks;
    Vector free_list = VEC(block_id_t, 0);
    manager->free_list = free_list;
    FileBuffer* header_buffer = NEW(FileBuffer, HEADER_SIZE);
    if (!header_buffer)
    {
        single_file_block_manager_destroy((BlockManager*)manager);
        return NULL;  // FileBuffer 创建失败
    }
    manager->header_buffer = header_buffer;  // 将 FileBuffer 结构体内容

    FILE* file = fopen(path, create_new ? "w+b" : "r+b");
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
        fileBuffer_clear(header_buffer);
        MasterHeader* meta_header = (MasterHeader*)header_buffer->buffer;
        meta_header->magic = MAGIC_NUMBER;
        meta_header->version = VERSION_NUMBER;
        fileBuffer_write(header_buffer, (FileHandle*)file_handle, 0);

        fileBuffer_clear(header_buffer);
        DatabaseHeader* db_header = (DatabaseHeader*)header_buffer->buffer;
        // header 1
        db_header->iteration = 0;
        db_header->meta_block = INVALID_BLOCK;
        db_header->free_list_id = INVALID_BLOCK;
        db_header->block_count = 0;
        fileBuffer_write(header_buffer, (FileHandle*)file_handle, HEADER_SIZE);
        // header 2
        db_header->iteration = 1;
        fileBuffer_write(header_buffer, (FileHandle*)file_handle, HEADER_SIZE * 2);
        fileHandle_sync((FileHandle*)file_handle);
        manager->active_header = 1;  // 新文件默认 header 2 为活跃
        manager->max_block = 0;
        manager->meta_block = INVALID_BLOCK;
        manager->iteration_count =
            1;  // H2 初始 iteration=1，保证首次 checkpoint 写入 H1 时 iteration > 1
    }
    else
    {
        fileBuffer_read(header_buffer, (FileHandle*)file_handle, 0);
        MasterHeader* master = (MasterHeader*)header_buffer->buffer;
        if (master->version != VERSION_NUMBER)
        {
            single_file_block_manager_destroy((BlockManager*)manager);
            return NULL;  // 版本不匹配
        }
        DatabaseHeader db_header1, db_header2;
        fileBuffer_read(header_buffer, (FileHandle*)file_handle, HEADER_SIZE);
        db_header1 = *(DatabaseHeader*)header_buffer->buffer;
        fileBuffer_read(header_buffer, (FileHandle*)file_handle, HEADER_SIZE * 2);
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

static void schema_scan_fn(CatalogEntry* entry, void* ctx)
{
    if (entry->deleted) return;
    Vector* schemas = (Vector*)ctx;
    SchemaCatalogEntry* schema = (SchemaCatalogEntry*)entry;
    vector_push_back(schemas, schema);
}

static void checkpointManager_destroy(CheckpointManager* self)
{
    metaBlockWriter_destroy(self->meta_block_writer);
    free(self);
}

static void checkpointManager_write_table_catalog(CheckpointManager* self, TableCatalogEntry* entry)
{
    // 写入 schema 名称
    SERIALIZER_WRITE_STRING(self->meta_block_writer, entry->schema->base.name);
    // 写入 table 名称
    SERIALIZER_WRITE_STRING(self->meta_block_writer, entry->base.name);
    // 写入列数量
    SERIALIZER_WRITE_U32(self->meta_block_writer, entry->column_count);
    // 写入列类型
    for (int i = 0; i < entry->column_count; i++)
    {
        // 写入列名称
        SERIALIZER_WRITE_STRING(self->meta_block_writer, entry->columns[i].name);
        // 写入列类型
        SERIALIZER_WRITE_U8(self->meta_block_writer, entry->columns[i].type);
    }
}

static void TableDataWriter_init(TableDataWriter* self, CheckpointManager* manager,
                                 TableCatalogEntry* table)
{
   // TableDataWriter* tabledata_writer = (TableDataWriter*)malloc(sizeof(TableDataWriter));
    self->manager = manager;
    self->table = table;
    Vector_init(&self->blocks, sizeof(Block*), table->column_count);
    Vector_init(&self->offsets, sizeof(usize), 0);
    Vector_init(&self->tuple_counts, sizeof(usize), 0);
    Vector_init(&self->row_numbers, sizeof(usize), 0);
    Vector_init(&self->indexes, sizeof(usize), 0);
    Vector_init(&self->data_pointers, sizeof(Vector), table->column_count);
}

static void tableDataWriter_deinit(TableDataWriter* self)
{
    vector_deinit(&self->blocks);
    vector_deinit(&self->offsets);
    vector_deinit(&self->tuple_counts);
    vector_deinit(&self->row_numbers);
    vector_deinit(&self->indexes);
    VECTOR_FOREACH(&self->data_pointers, data_pointer)
    {
        vector_deinit(data_pointer);
    }
    vector_deinit(&self->data_pointers);
}

static usize get_type_header_size(SQLType type)
{
    return 0;
}

static void TableDataWriter_flush_block(TableDataWriter* self, usize col)
{
    if (VECTOR_AT(&self->tuple_counts, col, usize) == 0) return;
    // assert()
    DataPointer data;
    Block* blk = VECTOR_AT(&self->blocks, col, Block*);
    blk->id = VCALL(self->manager->block_manager, get_free_block_id);
    data.min = 0;
    data.max = 0;
    data.block_id = blk->id;
    data.offset = 0;
    data.tuple_count = VECTOR_AT(&self->tuple_counts, col, usize);
    data.row_start = VECTOR_AT(&self->row_numbers, col, usize);
    Vector* data_pointers = VECTOR_GET(&self->data_pointers, col, Vector);
    vector_push_back(data_pointers, &data);
    VCALL(self->manager->block_manager, write, blk);
    usize zero = 0;
    vector_set(&self->offsets, col, &zero);
    usize row_number = VECTOR_AT(&self->row_numbers, col, usize);
    usize new_row_number = row_number + VECTOR_AT(&self->tuple_counts, col, usize);
    vector_set(&self->row_numbers, col, &new_row_number);
    vector_set(&self->tuple_counts, col, &zero);
}

static void TableDataWriter_flush_if_full(TableDataWriter* self, usize col, usize write_size)
{
    if (VECTOR_AT(&self->offsets, col, usize) + write_size >= BLOCK_SIZE)
    {
        TableDataWriter_flush_block(self, col);
    }
}

static void tableDataWriter_writecolumndata(TableDataWriter* self, DataChunk* chunk,
                                            usize column_index)
{
    TypeID type = chunk->columns[column_index].type;
    usize size = get_typeid_size(type) * dataChunk_size(chunk);
    TableDataWriter_flush_if_full(self, column_index, size);
    data_ptr_t ptr = VECTOR_AT(&self->blocks, column_index, Block*)->fb->buffer +
                     VECTOR_AT(&self->offsets, column_index, usize);
    ColumnVector source = chunk->columns[column_index];
    copy_to_storage(&source, ptr, 0, source.count);

    usize new_offset = VECTOR_AT(&self->offsets, column_index, usize) + size;
    vector_set(&self->offsets, column_index, &new_offset);
    usize new_tc = VECTOR_AT(&self->tuple_counts, column_index, usize) + dataChunk_size(chunk);
    vector_set(&self->tuple_counts, column_index, &new_tc);
}

static void tableDataWriter_write_data_pointers(TableDataWriter* self)
{
    VECTOR_FOREACH(&self->data_pointers, data_pointer_list)
    {
        usize size = vector_size(data_pointer_list);
        SERIALIZER_WRITE_U64(self->manager->tabledata_writer, size);
        VECTOR_FOREACH(data_pointer_list, dp)
        {
            DataPointer* data_pointer = (DataPointer*)dp;
            SERIALIZER_WRITE_F64(self->manager->tabledata_writer, data_pointer->min);
            SERIALIZER_WRITE_F64(self->manager->tabledata_writer, data_pointer->max);
            SERIALIZER_WRITE_U64(self->manager->tabledata_writer, data_pointer->row_start);
            SERIALIZER_WRITE_U64(self->manager->tabledata_writer, data_pointer->tuple_count);
            SERIALIZER_WRITE_U64(self->manager->tabledata_writer, data_pointer->block_id);
            SERIALIZER_WRITE_U32(self->manager->tabledata_writer, data_pointer->offset);
        }
    }
}

static void tableDataWriter_write_data(TableDataWriter* self)
{
    ScanState state;
    datatable_init_scan(self->table->datatable, &state);
    // 为每列准备一个  Block 作为写缓冲区：
    Vector column_ids = VEC(Oid, self->table->column_count);
    for (int i = 0; i < self->table->column_count; i++)
    {
        vector_push_back(&column_ids, &self->table->columns[i].oid);
    }
    Vector types = tableCatalogEntry_get_types(self->table);
    DataChunk chunk = MAKE(DataChunk, types);
    usize zero = 0;
    for (int i = 0; i < self->table->column_count; i++)
    {
        // for each column, create a block that serves as the buffer for that blocks data
        Block* blk = Block_create(INVALID_BLOCK);
        vector_push_back(&self->blocks, &blk);
        vector_push_back(&self->offsets, &zero);
        vector_push_back(&self->tuple_counts, &zero);
        vector_push_back(&self->row_numbers, &zero);
        Vector dp_list;
        Vector_init(&dp_list, sizeof(DataPointer), 0);
        vector_push_back(&self->data_pointers, &dp_list);
    }
    while (true)
    {
        dataChunk_reset(&chunk);
        datatable_scan(self->table->datatable, &state, &chunk, column_ids.data,
                       self->table->column_count);

         // 每次返回最多 STANDARD_VECTOR_SIZE
        if (dataChunk_size(&chunk) == 0) break;
        for (usize i = 0; i < chunk.column_count; i++)
        {
            assert(chunk.columns[i].type == get_internal_type(self->table->columns[i].type));
            tableDataWriter_writecolumndata(self, &chunk, i);
        }
    }
    for (int i = 0; i < self->table->column_count; i++)
    {
        TableDataWriter_flush_block(self, i);
    }
    scanstate_deinit(&state);
    vector_deinit(&column_ids);
    vector_deinit(&types);
    tableDataWriter_write_data_pointers(self);
}

//   DBHeader.meta_block
//       → metadata_writer 链 (Schema 定义 + Table 定义 + td_block/td_offset)
//           → tabledata_writer 链 (每列的 DataPointer 索引数组)
//               → DataPointer.block_id → 实际列数据 Block (256KB 原始数据)
void checkpointManager_write_table(CheckpointManager* self, TableCatalogEntry* entry)
{
    checkpointManager_write_table_catalog(self, entry);
    // 写入 td_block_id
    SERIALIZER_WRITE_U64(self->meta_block_writer, self->tabledata_writer->block->id);
    // 写入 td_offset
    SERIALIZER_WRITE_U64(self->meta_block_writer, self->tabledata_writer->offset);

    TableDataWriter writer = MAKE(TableDataWriter, self, entry);
    tableDataWriter_write_data(&writer);
    tableDataWriter_deinit(&writer);
}

static void checkpointManager_write_schema(CheckpointManager* self, SchemaCatalogEntry* entry)
{
    // 写入 schema 名称
    SERIALIZER_WRITE_STRING(self->meta_block_writer, GET_PARENT_FIELD(entry, name));
    // 写 table 数量
    SERIALIZER_WRITE_U32(self->meta_block_writer, catalogSet_get_entry_count(&entry->tables));
    HMAP_FOREACH(&entry->tables.data, _table_raw)
    {
        TableCatalogEntry* tbl = *(TableCatalogEntry**)_table_raw;
       // 跳过已删除的条目（与 catalogSet_get_entry_count 计数逻辑一致）
        if (tbl->base.deleted) continue;
        checkpointManager_write_table(self, tbl);
    }
    // 写索引数量（当前阶段为 0，占位）
    SERIALIZER_WRITE_U32(self->meta_block_writer, 0);
}

CheckpointManager* CheckpointManager_create(BlockManager* block_manager, Catalog* catalog)
{
    CheckpointManager* self = (CheckpointManager*)malloc(sizeof(CheckpointManager));
    self->block_manager = block_manager;
    self->catalog = catalog;

    return self;
}

void checkpointManager_createpoint(CheckpointManager* self)
{
    BlockManager* block_manager = self->block_manager;
    self->meta_block_writer = MetaBlockWriter_create(block_manager);
    self->tabledata_writer = MetaBlockWriter_create(block_manager);
    MetaBlockWriter* meta_block_writer = self->meta_block_writer;
    block_id_t meta_block = meta_block_writer->block->id;
    Vector schemas = VEC(SchemaCatalogEntry, 0);
    catalogSet_scan(&self->catalog->schemas, schema_scan_fn, &schemas);
    u32 schema_count = (u32)schemas.size;
    SERIALIZER_WRITE_TYPE(meta_block_writer, (data_ptr_t)&schema_count, u32);
    VECTOR_FOREACH(&schemas, schema)
    {
        // 写入 schema 到元数据块
        checkpointManager_write_schema(self, schema);
    }
    vector_deinit(&schemas);
    metaBlockWriter_flush(meta_block_writer);
    metaBlockWriter_flush(self->tabledata_writer);

       // 释放 MetaBlockWriter 资源（flush 后 Block 内容已写入磁盘）
    metaBlockWriter_destroy(meta_block_writer);
    metaBlockWriter_destroy(self->tabledata_writer);

    self->meta_block_writer = NULL;
    self->tabledata_writer = NULL;

    DatabaseHeader header;
    header.meta_block = meta_block;
    VCALL(block_manager, write_header, header);
}

static void TableDataReader_init(TableDataReader* self, CheckpointManager* manager,
                                 TableCatalogEntry* table, MetaBlockReader* reader)
{
    self->manager = manager;
    self->table = table;
    self->reader = reader;

    Vector_init(&self->blocks, sizeof(Block*), table->column_count);
    Vector_init(&self->offsets, sizeof(usize), 0);
    Vector_init(&self->tuple_counts, sizeof(usize), 0);
    Vector_init(&self->row_numbers, sizeof(usize), 0);
    Vector_init(&self->indexes, sizeof(usize), 0);
    Vector_init(&self->data_pointers, sizeof(Vector), table->column_count);
}

static void TableDataReader_deinit(TableDataReader* self)
{
    for (usize i = 0; i < vector_size(&self->data_pointers); i++)
    {
        Vector* dp_vec = vector_get(&self->data_pointers, i);
        vector_deinit(dp_vec);
    }
    vector_deinit(&self->data_pointers);
    for (usize i = 0; i < vector_size(&self->blocks); i++)
    {
        Block* blk = VECTOR_AT(&self->blocks, i, Block*);
        block_destroy(blk);
    }
    vector_deinit(&self->blocks);
    vector_deinit(&self->offsets);
    vector_deinit(&self->tuple_counts);
    vector_deinit(&self->indexes);
}

static void tableDataReader_read_data_pointers(TableDataReader* self)
{
    for (usize i = 0; i < self->table->column_count; i++)
    {
        u64 dp_count = DESERIALIZER_READ_U64(self->reader);
        Vector dp_list = VEC(DataPointer, dp_count);
        for (u64 j = 0; j < dp_count; j++)
        {
            DataPointer dp;
            dp.min = DESERIALIZER_READ_F64(self->reader);
            dp.max = DESERIALIZER_READ_F64(self->reader);
            dp.row_start = DESERIALIZER_READ_U64(self->reader);
            dp.tuple_count = DESERIALIZER_READ_U64(self->reader);
            dp.block_id = DESERIALIZER_READ_U64(self->reader);
            dp.offset = DESERIALIZER_READ_U32(self->reader);
            vector_push_back(&dp_list, &dp);
        }
        vector_push_back(&self->data_pointers, &dp_list);
    }
}

static bool tableDataReader_read_block(TableDataReader* self, usize col)
{
    usize idx = VECTOR_AT(&self->indexes, col, usize);
    Vector* dp_list = VECTOR_GET(&self->data_pointers, col, Vector);
    if (idx >= vector_size(dp_list)) return false;

    DataPointer* dp = vector_get(dp_list, idx);
    Block* blk = VECTOR_AT(&self->blocks, col, Block*);
    blk->id = dp->block_id;
    // read the data for the block from disk
    VCALL(self->manager->block_manager, read, blk);
    usize off = (usize)dp->offset;
    vector_set(&self->offsets, col, &off);
    usize zero = 0;
    vector_set(&self->tuple_counts, col, &zero);
    usize next_idx = idx + 1;
    vector_set(&self->indexes, col, &next_idx);
    return true;
}

static void tableDataReader_read_table(TableDataReader* self)
{
    // 读取数据指针
    tableDataReader_read_data_pointers(self);
    usize col_count = self->table->column_count;
    Vector* dp0 = VECTOR_GET(&self->data_pointers, 0, Vector);
    if (vector_size(dp0) == 0) return; // 空表

    usize zero = 0;
    for (usize col = 0; col < col_count; col++)
    {
        Block* blk = Block_create(INVALID_BLOCK);
        vector_push_back(&self->blocks, &blk);
        vector_push_back(&self->offsets, &zero);
        vector_push_back(&self->tuple_counts, &zero);
        vector_push_back(&self->indexes, &zero);
        tableDataReader_read_block(self, col);
    }

    Vector types = tableCatalogEntry_get_types(self->table);
    DataChunk chunk = MAKE(DataChunk, types); // DataChunk_init
    while (true)
    {
        dataChunk_reset(&chunk);
        for (usize col = 0; col < col_count; col++)
        {
            TypeID type = get_internal_type(self->table->columns[col].type);
            usize type_size = get_typeid_size(type);
            usize filled = 0;
            while (filled < STANDARD_VECTOR_SIZE)
            {
                usize idx = VECTOR_AT(&self->indexes, col, usize);
                if (idx == 0) break;
                Vector* dp_list = VECTOR_GET(&self->data_pointers, col, Vector);
                DataPointer* dp = VECTOR_GET(dp_list, idx - 1, DataPointer);
                usize tc = VECTOR_AT(&self->tuple_counts, col, usize);
                // 计算当前块中剩余的元组数量
                usize remaining_in_block = dp->tuple_count - tc;
                if (remaining_in_block == 0)
                {
                    // no tuples left in this block
                    // move to next block
                    if (!tableDataReader_read_block(self, col)) break;
                    continue;
                }
                usize to_read = MIN(STANDARD_VECTOR_SIZE - filled, remaining_in_block);
                usize off = VECTOR_AT(&self->offsets, col, usize);
                Block* blk = VECTOR_AT(&self->blocks, col, Block*);
                data_ptr_t src = blk->fb->buffer + off;
                data_ptr_t dst = chunk.columns[col].data + filled * type_size;
                memcpy(dst, src, to_read * type_size);
                filled += to_read;
                usize new_off = off + to_read * type_size;
                vector_set(&self->offsets, col, &new_off);
                usize new_tc = tc + to_read;
                vector_set(&self->tuple_counts, col, &new_tc);
            }
            chunk.columns[col].type = type;
            chunk.columns[col].count = filled;
        }
        if (dataChunk_size(&chunk) == 0) break;
        datatable_append(self->table->datatable, &chunk);
    }
    dataChunk_deinit(&chunk);
    vector_deinit(&types);
}

void checkpointManager_read_table(CheckpointManager* self, MetaBlockReader* reader)
{
    CreateTableInfo info;
    createTableInfo_deserialize(&info, reader);
    // 写入 table 到 catalog
    catalog_create_table(self->catalog, &info);
    block_id_t td_block_id = DESERIALIZER_READ_U64(reader);
    // 读取 td_offset
    u64 td_offset = DESERIALIZER_READ_U64(reader);

    MetaBlockReader td_reader = MAKE(MetaBlockReader, self->block_manager, td_block_id);
    td_reader.offset = td_offset;

    TableCatalogEntry* table_entry =
        catalog_get_table(self->catalog, info.schema_name, info.table_name);

    TableDataReader data_reader = MAKE(TableDataReader, self, table_entry, &td_reader);
    tableDataReader_read_table(&data_reader);
    TableDataReader_deinit(&data_reader);
    metaBlockReader_deinit(&td_reader);

    // 5. 释放：table_name 和 columns 数组已由 catalog 拷贝，可安全释放
   //    schema_name 被 DataTable 直接持有，不释放
    //    columns[i].name 被 catalog memcpy 共享，不释放
    free(info.table_name);
    free(info.columns);
}

static void checkpointManager_read_schema(CheckpointManager* self, MetaBlockReader* reader)
{
    CreateSchemaInfo schema;
    createSchemaInfo_deserialize(&schema, reader);
    // 写入 schema 到 catalog
    catalog_create_schema(self->catalog, &schema);
    // 读取 table 数量
    u32 table_count = DESERIALIZER_READ_U32(reader);
    for (u32 i = 0; i < table_count; i++)
    {
        checkpointManager_read_table(self, reader);
    }

    // 读取 index_count（对称 write_schema 写入的 index_count = 0）
    u32 index_count = DESERIALIZER_READ_U32(reader);
    (void)index_count;
        // 释放反序列化分配的 schema_name
    free(schema.schema_name);
}

void checkpointManager_loadfromstorage(CheckpointManager* self)
{
    BlockManager* block_manager = self->block_manager;
    block_id_t meta_block = VCALL(block_manager, get_frist_meta_block);
    if (meta_block == INVALID_BLOCK) return;
    DatabaseHeader header;
    MetaBlockReader reader = MAKE(MetaBlockReader, block_manager, meta_block);
    u32 schema_count = DESERIALIZER_READ_U32(&reader);
    for (u32 i = 0; i < schema_count; i++)
    {
        // 读取 schema 到元数据块
        checkpointManager_read_schema(self, &reader);
    }
    metaBlockReader_deinit(&reader);
}
