#ifndef STORAGE_H
#define STORAGE_H

#include <stdio.h>
#include "vb_type.h"
#include "interface.h"
#include "vector.h"
#include "wal.h"

#define BLOCK_SIZE             262144 // 256KB
#define HEADER_SIZE            4096   // 数据库头大小
#define FILE_BUFFER_BLOCK_SIZE 4096   // 文件缓冲区块大小
#define VERSION_NUMBER         1      // 数据库版本号
#define INVALID_BLOCK          -1    // 无效块标识

static int FILE_BUFFER_HEADER_SIZE = sizeof(u64);// 文件缓冲区块头大小 存储 checksum 校验和
static int BLOCK_START = HEADER_SIZE * 3; // 数据块起始位置，从第3个 HEADER_SIZE 开始
//
typedef struct
{
    u64 version;  // 版本号，用于未来的兼容性检查
    u64 flags[4]; // 保留字段·1，供将来扩展使用
} MasterHeader;

typedef struct
{
    // 迭代计数。每次做 checkpoint 时会把某个 DatabaseHeader 的 iteration 自增并写回磁盘，
    // 用于决定哪个 header 是“活跃”的（启动时选择 iteration 更大的那个）。
    // 通过交替写两个 header 并比较 iteration 实现原子切换（写入 h1/h2 交替）。
    u64 iteration;
    // 指向 meta block 的块 id。meta block 存放更复杂的元数据
    // （例如 catalog 序列化数据、表/列的元信息等）。
    // meta_block == INVALID_BLOCK 表示尚无 meta block。
    // 在恢复时会通过该 id 用 MetaBlockReader 读取元数据。
    block_id_t meta_block;
    // 指向存放 free list 的 meta block（即记录当前可回收/空闲数据块 id 列表的块）。
    // 若为 INVALID_BLOCK 则表示当前没有显式 free list。
    // free_list 指向的 meta block 中通常先存 free_list 的长度，然后是每个可用 block_id。
    // 初始化时会把这些 id 加入内存 free_list 向量。
    block_id_t free_list_id;
    // 文件中的块数量。如果文件大于 BLOCK_SIZE * block_count，
    // 则出现在 block_count 之后的任何块都隐式地属于 free_list
    u64 block_count;
} DatabaseHeader;

typedef struct
{
    usize size;
    u8* internal_buf;
    usize internal_size;
    u8* buffer;
    // data数组是柔性数组，它不占用结构体本身的大小，而是紧跟在结构体之后
    u8 data[];
} FileBuffer;

FileBuffer* FileBuffer_create(usize size);

// BlockManager 类型枚举 - 用于区分不同的实现
typedef enum
{
    FILEHANDLE_FILE = 0,   // 文件句柄
} FileHandleType;

// // FileHandle 虚函数表
// typedef struct FileHandleVTable
// {
//     void (*free)(void* handle);
//     int (*read)(void* handle, void* buffer, usize nr_bytes);
//     int (*write)(void* handle, const void* buffer, usize nr_bytes);
//     int (*read_at)(void* handle, void* buffer, usize nr_bytes, usize location);
//     int (*write_at)(void* handle, const void* buffer, usize nr_bytes, usize location);
//     void (*sync)(void* handle);
// } FileHandleVTable;
// clang-format off
DEFINE_CLASS(FileHandle,
    VMETHOD(FileHandle, free, void)
    VMETHOD(FileHandle, read, int, void* buffer, usize nr_bytes)
    VMETHOD(FileHandle, write, int, const void* buffer, usize nr_bytes)
    VMETHOD(FileHandle, read_at, int, void* buffer, usize nr_bytes, usize location)
    VMETHOD(FileHandle, write_at, int, const void* buffer, usize nr_bytes, usize location)
    VMETHOD(FileHandle, sync, void)
    ,
    FIELD(type, FileHandleType)
)
// clang-format on

// FileHandle 结构
typedef struct
{
    EXTENDS(FileHandle);
    FILE* file;
} FileSystemHandle;

FileSystemHandle* create_filesystem_handle_from_FILE(FILE* file);

int FileBuffer_read(FileBuffer* buffer, FileHandle* handle, usize location);
int FileBuffer_write(FileBuffer* buffer, FileHandle* handle, usize location);
void FileBuffer_clear(FileBuffer* buffer);
void FileBuffer_destroy(FileBuffer* buffer);

typedef struct Block
{
    block_id_t id;
    FileBuffer* fb;
} Block;

  // BlockManager 虚类（接口）- 使用函数指针实现多态
typedef struct BlockManager BlockManager;

// BlockManager 类型枚举 - 用于区分不同的实现
typedef enum
{
    BLOCK_MANAGER_SINGLE_FILE = 0,   // 单文件块管理器
    BLOCK_MANAGER_MULTI_FILE,        // 多文件块管理器（预留）
    BLOCK_MANAGER_MEMORY,            // 内存块管理器（预留）
} BlockManagerType;

// // BlockManager 的虚函数表（V-Table）
// typedef struct BlockManagerVTable
// {
//       // 读取块的函数指针（纯虚函数）
//     void (*read)(BlockManager* self, Block* block);

//       // 写入块的函数指针（纯虚函数）
//     void (*write)(BlockManager* self, Block* block);

//       // 创建块的函数指针（纯虚函数）
//     Block* (*create_block)(BlockManager* self);

//       // 析构函数
//     void (*destroy)(BlockManager* self);
// } BlockManagerVTable;

//   // BlockManager 基类结构
// struct BlockManager
// {
//     BlockManagerVTable* vtable;  // 虚函数表指针
//     BlockManagerType type;       // 管理器类型标识
// };

// 定义BlockManager虚表和基类
// clang-format off
DEFINE_CLASS(BlockManager,
    VMETHOD(BlockManager, read, void, Block* block)
    VMETHOD(BlockManager, write, void, Block* block)
    VMETHOD(BlockManager, get_free_block_id, block_id_t)
    VMETHOD(BlockManager, create_block, Block*)
    VMETHOD(BlockManager, write_header, void, DatabaseHeader)
    VMETHOD(BlockManager, destroy, void)
    ,
    FIELD(type, BlockManagerType)
)
// clang-format on

typedef struct
{
    EXTENDS(BlockManager);           // 继承 BlockManager（组合方式）
    // 用于决定哪个 header 是“活跃”的（启动时选择 iteration 更大的那个）。
    u8 active_header;
    // 数据库文件路径
    char* file_path;
    // 文件句柄
    FileHandle* file_handle;
    // 用于读写 header 的缓冲区
    FileBuffer* header_buffer;
    // 空闲块列表（记录可回收/空闲数据块 id）。
    Vector free_list;
    // 已使用块列表（记录已分配/使用的数据块 id）。
    Vector used_blocks;
    // 当前文件中已分配的最大块编号（从0开始）。新块分配时从 max_block + 1 开始。
    block_id_t max_block;
    // 当前 meta block 的块 id（如果有）。meta block 存放更复杂的元数据。
    block_id_t meta_block;
    // 迭代次数，用于版本控制和元数据更新。
    u64 iteration_count;
} SingleFileBlockManager;

SingleFileBlockManager* create_new_database(const char* path, bool create_new);

#define BlockManager_write(mptr, block) \
    GENERIC_DISPATCH(mptr, SingleFileBlockManager* : single_file_block_manager_write)(mptr, block)

typedef enum
{
    META_BLOCK_READER = 0,   //
} DeserializerType;

// typedef struct Deserializer Deserializer;

// typedef struct DeserializerVTable
// {
//       // 纯虚函数：读取数据
//     void (*read_data)(Deserializer* self, data_ptr_t buffer, usize read_size);

//       // 析构函数
//     void (*destroy)(Deserializer* self);
// } DeserializerVTable;

// struct Deserializer
// {
//     DeserializerVTable* vtable; //
//     DeserializerType type; //
// };

// 定义Deserializer虚表和基类
// clang-format off
DEFINE_CLASS(Deserializer,
    VMETHOD(Deserializer, read_data, void, data_ptr_t buffer, usize read_size)
    VMETHOD(Deserializer, destroy, void)
    ,
    FIELD(type, DeserializerType)
)
// clang-format on

typedef struct
{
    EXTENDS(Deserializer); // 继承
    BlockManager* manager;  // 管理器指针
    Block* block;  // 当前块指针
    usize offset;  // 当前块内的偏移量
    block_id_t next_block_id;  // 下一个块的 ID
} MetaBlockReader;

#define INTERFACE_Deserializer(DO, type) DO(type, read_data, void, data_ptr_t buffer, usize size)
INTERFACE(Deserializer, (read_data, void, (data_ptr_t buffer, size_t size)))
// 实现接口
IMPL_INTERFACE(Deserializer, MetaBlockReader);

// 泛型函数宏（编译时展开，完全不使用虚表）
#define Deserializer_read(dr_ptr, buffer, size) \
    GENERIC_DISPATCH(dr_ptr, MetaBlockReader* : MetaBlockReader_read_data)(dr_ptr, buffer, size)

typedef enum
{
    META_BLOCK_WRITER = 0,   // 元数据块写入器
} SerializerType;

// clang-format off
DEFINE_CLASS(Serializer,
    VMETHOD(Serializer, write_data, void, data_ptr_t buffer, usize write_size)
    VMETHOD(Serializer, destroy, void)
    ,
    FIELD(type, SerializerType)
)
// clang-format on

typedef struct
{
    EXTENDS(Serializer); // 继承
    BlockManager* manager;  // 管理器指针
    Block* block;  // 当前块指针
    usize offset;  // 当前块内的偏移量
} MetaBlockWriter;

#define INTERFACE_Serializer(DO, type) DO(type, write_data, void, data_ptr_t buffer, usize size)
INTERFACE(Serializer, (write_data, void, (data_ptr_t buffer, size_t size)))
// 实现接口
IMPL_INTERFACE(Serializer, MetaBlockWriter);

// 泛型函数宏（编译时展开，完全不使用虚表）
#define Serializer_write(sw_ptr, buffer, size) \
    GENERIC_DISPATCH(sw_ptr, MetaBlockWriter* : MetaBlockWriter_write_data)(sw_ptr, buffer, size)

typedef struct
{
    BlockManager* block_manager; // 块管理器指针
    WALManager* wal_manager; // WAL 管理器指针
} StorageManager;

typedef struct
{
} CHeckpointManager;

#endif  // STORAGE_H