#include "../src/interface.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/**
 * DEFINE_CLASS simplified demo
 */

typedef struct Block
{
    int id;
    char data[64];
} Block;

typedef enum
{
    BLOCK_MANAGER_SINGLE_FILE = 0,
    BLOCK_MANAGER_MEMORY,
} BlockManagerType;

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

/* Subclass definition using EXTENDS (replaces non-existent BEGIN_SUBCLASS) */
typedef struct SingleFileBlockManager
{
    EXTENDS(BlockManager);
    const char* file_path;
    int block_count;
} SingleFileBlockManager;

/* Virtual method declarations */
IMPL_VTABLE_METHOD(SingleFileBlockManager, read, void, Block* block)
IMPL_VTABLE_METHOD(SingleFileBlockManager, write, void, Block* block)
IMPL_VTABLE_METHOD(SingleFileBlockManager, create_block, Block*)
IMPL_VTABLE_METHOD(SingleFileBlockManager, destroy, void)

void SingleFileblockManager_read(SingleFileBlockManager* self, Block* block)
{
    printf("  Read block %d from %s\n", block->id, self->file_path);
    snprintf(block->data, sizeof(block->data), "Data from %s", self->file_path);
}

void SingleFileblockManager_write(SingleFileBlockManager* self, Block* block)
{
    printf("  Write block %d to %s: %s\n", block->id, self->file_path, block->data);
}

Block* SingleFileblockManager_create_block(SingleFileBlockManager* self)
{
    Block* block = (Block*)malloc(sizeof(Block));
    block->id = self->block_count++;
    memset(block->data, 0, sizeof(block->data));
    printf("  Create block %d\n", block->id);
    return block;
}

void SingleFileblockManager_destroy(SingleFileBlockManager* self)
{
    printf("  Destroy manager: %s\n", self->file_path);
    free((void*)self->file_path);
    free(self);
}

// VTable using VTABLE_ENTRY
static BlockManagerVTable vtable = {
    VTABLE_ENTRY(read, SingleFileblockManager_read),
    VTABLE_ENTRY(write, SingleFileblockManager_write),
    VTABLE_ENTRY(create_block, SingleFileblockManager_create_block),
    VTABLE_ENTRY(destroy, SingleFileblockManager_destroy)};

SingleFileBlockManager* create_manager(const char* path)
{
    SingleFileBlockManager* mgr = malloc(sizeof(SingleFileBlockManager));
    mgr->base.vtable = &vtable;
    mgr->base.type = BLOCK_MANAGER_SINGLE_FILE;
    mgr->file_path = strdup(path);
    mgr->block_count = 0;
    return mgr;
}

int main()
{
    printf("\n=== DEFINE_CLASS + EXTENDS Demo ===\n\n");

    SingleFileBlockManager* mgr = create_manager("data.db");
    BlockManager* base = UPCAST(mgr, BlockManager);

    Block* block = VCALL(base, create_block);
    strcpy(block->data, "Hello World");
    VCALL(base, write, block);
    VCALL(base, read, block);
    printf("  Data: %s\n", block->data);

    free(block);
    VCALL(base, destroy);

    printf("\n[PASS] DEFINE_CLASS + EXTENDS test completed\n");
    return 0;
}
