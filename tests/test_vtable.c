#include "../src/interface.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/**
 * 演示：使用虚表宏定义类和继承
 */

/* ============================================================================
 * 示例：BlockManager 虚类
 * ============================================================================ */

// 前置声明
typedef struct Block
{
    int id;
    char data[64];
} Block;

// 定义枚举
typedef enum
{
    BLOCK_MANAGER_SINGLE_FILE = 0,
    BLOCK_MANAGER_MEMORY,
} BlockManagerType;

// 一步完成：定义虚表和基类
// clang-format off
DEFINE_CLASS(BlockManager,
    VMETHOD(BlockManager, read, void, Block* block)
    VMETHOD(BlockManager, write, void, Block* block)
    VMETHOD(BlockManager, create_block, Block*)
    VMETHOD(BlockManager, destroy, void)
    ,
    FIELD(type, BlockManagerType)
)
// clang-format on

/* ============================================================================
 * 子类1: SingleFileBlockManager
 * ============================================================================ */

typedef struct SingleFileBlockManager
{
    EXTENDS(BlockManager); // 继承 BlockManager
    const char* file_path;
    int block_count;
} SingleFileBlockManager;

// 声明虚方法实现
IMPL_VTABLE_METHOD(SingleFileBlockManager, read, void, Block* block)
IMPL_VTABLE_METHOD(SingleFileBlockManager, write, void, Block* block)
IMPL_VTABLE_METHOD(SingleFileBlockManager, create_block, Block*)
IMPL_VTABLE_METHOD(SingleFileBlockManager, destroy, void)

// 实现虚方法
void SingleFileblockManager_read(SingleFileBlockManager* self, Block* block)
{
    printf("  [SingleFile] 读取块 %d 从 %s\n", block->id, self->file_path);
    snprintf(block->data, sizeof(block->data), "Data from %s", self->file_path);
}

void SingleFileblockManager_write(SingleFileBlockManager* self, Block* block)
{
    printf("  [SingleFile] 写入块 %d 到 %s: %s\n", block->id, self->file_path, block->data);
}

Block* SingleFileblockManager_create_block(SingleFileBlockManager* self)
{
    Block* block = (Block*)malloc(sizeof(Block));
    block->id = self->block_count++;
    memset(block->data, 0, sizeof(block->data));
    printf("  [SingleFile] 创建块 %d\n", block->id);
    return block;
}

void SingleFileblockManager_destroy(SingleFileBlockManager* self)
{
    printf("  [SingleFile] 销毁管理器: %s\n", self->file_path);
    free((void*)self->file_path);
    free(self);
}

// 初始化虚表
static BlockManagerVTable single_file_vtable = {
    VTABLE_ENTRY(read, SingleFileblockManager_read),
    VTABLE_ENTRY(write, SingleFileblockManager_write),
    VTABLE_ENTRY(create_block, SingleFileblockManager_create_block),
    VTABLE_ENTRY(destroy, SingleFileblockManager_destroy)};

/* ============================================================================
 * 子类2: MemoryBlockManager
 * ============================================================================ */

typedef struct MemoryBlockManager
{
    EXTENDS(BlockManager); // 继承 BlockManager
    Block** blocks;
    size_t capacity;
    size_t count;
} MemoryBlockManager;

// 声明虚方法实现
IMPL_VTABLE_METHOD(MemoryBlockManager, read, void, Block* block)
IMPL_VTABLE_METHOD(MemoryBlockManager, write, void, Block* block)
IMPL_VTABLE_METHOD(MemoryBlockManager, create_block, Block*)
IMPL_VTABLE_METHOD(MemoryBlockManager, destroy, void)

// 实现虚方法
void MemoryblockManager_read(MemoryBlockManager* self, Block* block)
{
    if (block->id < (int)self->count)
    {
        memcpy(block->data, self->blocks[block->id]->data, sizeof(block->data));
        printf("  [Memory] 读取块 %d 从内存\n", block->id);
    }
}

void MemoryblockManager_write(MemoryBlockManager* self, Block* block)
{
    if (block->id < (int)self->capacity)
    {
        if (self->blocks[block->id] == NULL)
        {
            self->blocks[block->id] = (Block*)malloc(sizeof(Block));
        }
        memcpy(self->blocks[block->id], block, sizeof(Block));
        if (block->id >= (int)self->count)
        {
            self->count = block->id + 1;
        }
        printf("  [Memory] 写入块 %d 到内存: %s\n", block->id, block->data);
    }
}

Block* MemoryblockManager_create_block(MemoryBlockManager* self)
{
    Block* block = (Block*)malloc(sizeof(Block));
    block->id = self->count;
    memset(block->data, 0, sizeof(block->data));
    printf("  [Memory] 创建块 %d\n", block->id);
    return block;
}

void MemoryblockManager_destroy(MemoryBlockManager* self)
{
    printf("  [Memory] 销毁管理器 (释放 %zu 个块)\n", self->count);
    for (size_t i = 0; i < self->count; i++)
    {
        if (self->blocks[i])
        {
            free(self->blocks[i]);
        }
    }
    free(self->blocks);
    free(self);
}

// 初始化虚表
static BlockManagerVTable memory_vtable = {
    VTABLE_ENTRY(read, MemoryblockManager_read), VTABLE_ENTRY(write, MemoryblockManager_write),
    VTABLE_ENTRY(create_block, MemoryblockManager_create_block),
    VTABLE_ENTRY(destroy, MemoryblockManager_destroy)};

/* ============================================================================
 * 工厂函数
 * ============================================================================ */

SingleFileBlockManager* create_single_file_manager(const char* path)
{
    SingleFileBlockManager* mgr = (SingleFileBlockManager*)malloc(sizeof(SingleFileBlockManager));

    // 初始化基类
    mgr->base.vtable = &single_file_vtable;
    mgr->base.type = BLOCK_MANAGER_SINGLE_FILE;

    // 初始化子类
    mgr->file_path = strdup(path);
    mgr->block_count = 0;

    return mgr;
}

MemoryBlockManager* create_memory_manager(size_t capacity)
{
    MemoryBlockManager* mgr = (MemoryBlockManager*)malloc(sizeof(MemoryBlockManager));

    // 初始化基类
    mgr->base.vtable = &memory_vtable;
    mgr->base.type = BLOCK_MANAGER_MEMORY;

    // 初始化子类
    mgr->blocks = (Block**)calloc(capacity, sizeof(Block*));
    mgr->capacity = capacity;
    mgr->count = 0;

    return mgr;
}

/* ============================================================================
 * 测试函数
 * ============================================================================ */

void test_single_file_manager()
{
    printf("\n[测试1] SingleFileBlockManager\n");
    printf("=====================================\n");

    SingleFileBlockManager* mgr = create_single_file_manager("test.db");

    // 向上转型为基类指针
    BlockManager* base = UPCAST(mgr, BlockManager);

    // 通过虚表调用方法（多态）
    Block* block = VCALL(base, create_block);
    strcpy(block->data, "Hello from file");
    VCALL(base, write, block);
    VCALL(base, read, block);

    printf("  块数据: %s\n", block->data);

    free(block);
    VCALL(base, destroy);

    printf("  ✓ 测试通过\n");
}

void test_memory_manager()
{
    printf("\n[测试2] MemoryBlockManager\n");
    printf("=====================================\n");

    MemoryBlockManager* mgr = create_memory_manager(10);

    // 向上转型
    BlockManager* base = UPCAST(mgr, BlockManager);

    // 创建和写入块
    Block* block1 = VCALL(base, create_block);
    strcpy(block1->data, "Block 1 data");
    VCALL(base, write, block1);

    Block* block2 = VCALL(base, create_block);
    strcpy(block2->data, "Block 2 data");
    VCALL(base, write, block2);

    // 读取块
    Block read_block = {0};
    read_block.id = 0;
    VCALL(base, read, &read_block);
    printf("  读取块0: %s\n", read_block.data);

    free(block1);
    free(block2);
    VCALL(base, destroy);

    printf("  ✓ 测试通过\n");
}

void test_polymorphism()
{
    printf("\n[测试3] 多态性测试\n");
    printf("=====================================\n");

    // 创建两个不同的管理器
    SingleFileBlockManager* file_mgr = create_single_file_manager("poly.db");
    MemoryBlockManager* mem_mgr = create_memory_manager(5);

    // 都转换为基类指针
    BlockManager* managers[2] = {UPCAST(file_mgr, BlockManager), UPCAST(mem_mgr, BlockManager)};

    // 通过基类指针多态调用
    for (int i = 0; i < 2; i++)
    {
        printf("  管理器 %d (类型=%d):\n", i, managers[i]->type);
        Block* block = VCALL(managers[i], create_block);
        snprintf(block->data, sizeof(block->data), "Data from manager %d", i);
        VCALL(managers[i], write, block);
        VCALL(managers[i], read, block);
        free(block);
    }

    // 清理
    VCALL(managers[0], destroy);
    VCALL(managers[1], destroy);

    printf("  ✓ 多态性测试通过\n");
}

/* ============================================================================
 * 主函数
 * ============================================================================ */

int main()
{
    printf("\n");
    printf("╔════════════════════════════════════════════════════╗\n");
    printf("║       虚表（VTable）和继承测试                     ║\n");
    printf("╚════════════════════════════════════════════════════╝\n");

    test_single_file_manager();
    test_memory_manager();
    test_polymorphism();

    printf("\n");
    printf("╔════════════════════════════════════════════════════╗\n");
    printf("║           所有测试通过! ✓                          ║\n");
    printf("╚════════════════════════════════════════════════════╝\n");

    printf("\n关键宏：\n");
    printf("  BEGIN_VTABLE / END_VTABLE - 定义虚表\n");
    printf("  BEGIN_CLASS / END_CLASS   - 定义基类\n");
    printf("  BEGIN_SUBCLASS / END_SUBCLASS - 定义子类\n");
    printf("  VMETHOD                   - 定义虚方法\n");
    printf("  IMPL_VTABLE_METHOD        - 实现虚方法\n");
    printf("  VCALL                     - 调用虚方法\n");
    printf("  UPCAST / DOWNCAST         - 类型转换\n");
    printf("\n");

    return 0;
}
