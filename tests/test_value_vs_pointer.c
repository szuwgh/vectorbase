#include "../src/interface.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stddef.h>

/**
 * 测试：值类型 vs 指针类型的区别
 */

// 假设的 Block 结构
typedef struct Block
{
    int id;
    char data[64];
    struct Block* next; // 假设有个指针字段
} Block;

typedef struct Deserializer
{
    void* vtable;
    int type;
} Deserializer;

/* ============================================================================
 * 方式1：使用指针（原始方式）
 * ============================================================================ */
typedef struct MetaBlockReader_Pointer
{
    EXTENDS(Deserializer);
    void* manager;
    Block* block;      // 指针！
    size_t offset;
    int next_block_id;
} MetaBlockReader_Pointer;

/* ============================================================================
 * 方式2：使用值类型
 * ============================================================================ */
typedef struct MetaBlockReader_Value
{
    EXTENDS(Deserializer);
    void* manager;
    Block block;       // 值类型！
    size_t offset;
    int next_block_id;
} MetaBlockReader_Value;

/* ============================================================================
 * 测试和对比
 * ============================================================================ */

void test_memory_size()
{
    printf("\n[测试1] 内存占用对比\n");
    printf("=====================================\n");

    printf("  Block 大小: %zu 字节\n", sizeof(Block));
    printf("  指针大小: %zu 字节\n", sizeof(void*));
    printf("\n");

    printf("  MetaBlockReader_Pointer 大小: %zu 字节\n",
           sizeof(MetaBlockReader_Pointer));
    printf("  MetaBlockReader_Value 大小:   %zu 字节\n",
           sizeof(MetaBlockReader_Value));
    printf("\n");

    size_t diff = sizeof(MetaBlockReader_Value) - sizeof(MetaBlockReader_Pointer);
    printf("  差异: %zu 字节 (值类型多占用)\n", diff);

    // 如果创建 1000 个对象
    printf("\n  如果创建 1000 个对象:\n");
    printf("    指针方式: %zu KB\n",
           (sizeof(MetaBlockReader_Pointer) * 1000) / 1024);
    printf("    值方式:   %zu KB\n",
           (sizeof(MetaBlockReader_Value) * 1000) / 1024);
}

void test_null_handling()
{
    printf("\n[测试2] NULL 处理\n");
    printf("=====================================\n");

    MetaBlockReader_Pointer reader_ptr = {0};
    // MetaBlockReader_Value reader_val = {0};

    printf("  指针方式:\n");
    if (reader_ptr.block == NULL)
    {
        printf("    ✓ 可以检查 block 是否为 NULL\n");
        printf("    ✓ 可以延迟初始化\n");
    }

    printf("\n  值类型方式:\n");
    printf("    ✗ 无法表示 \"无 block\" 状态\n");
    printf("    ✗ 必须立即分配完整的 Block 空间\n");
    printf("    ✓ 但是保证 block 始终有效（不会悬空指针）\n");
}

void test_sharing()
{
    printf("\n[测试3] 共享和所有权\n");
    printf("=====================================\n");

    // 创建一个共享的 Block
    Block* shared_block = (Block*)malloc(sizeof(Block));
    shared_block->id = 42;
    strcpy(shared_block->data, "Shared data");

    // 指针方式：多个 reader 可以共享同一个 block
    MetaBlockReader_Pointer reader1 = {0};
    MetaBlockReader_Pointer reader2 = {0};
    reader1.block = shared_block;
    reader2.block = shared_block;

    printf("  指针方式:\n");
    printf("    reader1.block == reader2.block: %s\n",
           reader1.block == reader2.block ? "true" : "false");
    printf("    ✓ 可以共享同一个 Block\n");
    printf("    ✓ 节省内存\n");
    printf("    ⚠ 需要管理生命周期（谁负责 free？）\n");

    printf("\n  值类型方式:\n");
    printf("    ✗ 每个 reader 都有自己的 Block 副本\n");
    printf("    ✗ 修改一个不影响另一个\n");
    printf("    ✓ 所有权清晰（自己管理自己的）\n");
    printf("    ⚠ 如果 Block 很大，复制开销大\n");

    free(shared_block);
}

void test_initialization()
{
    printf("\n[测试4] 初始化方式\n");
    printf("=====================================\n");

    printf("  指针方式:\n");
    printf("    MetaBlockReader_Pointer reader = {0};\n");
    printf("    reader.block = create_block();  // 灵活\n");
    printf("    ✓ 可以延迟分配\n");
    printf("    ✓ 可以指向栈、堆或静态区的 Block\n");

    printf("\n  值类型方式:\n");
    printf("    MetaBlockReader_Value reader = {0};\n");
    printf("    reader.block.id = 1;  // 直接使用\n");
    printf("    ✓ 自动分配，无需手动 malloc\n");
    printf("    ✓ 作用域结束自动释放\n");
    printf("    ✗ 栈上分配，如果 Block 很大可能栈溢出\n");
}

void test_cache_locality()
{
    printf("\n[测试5] 缓存局部性\n");
    printf("=====================================\n");

    printf("  指针方式:\n");
    printf("    reader → [Deserializer|manager|block*|offset]\n");
    printf("                                      ↓\n");
    printf("                               [Block 在别处]\n");
    printf("    ⚠ 两次内存访问（指针跳转）\n");
    printf("    ⚠ Block 可能不在缓存中\n");

    printf("\n  值类型方式:\n");
    printf("    reader → [Deserializer|manager|Block内嵌|offset]\n");
    printf("    ✓ 一次内存访问\n");
    printf("    ✓ Block 紧邻其他字段，缓存友好\n");
    printf("    ✓ 访问速度可能更快（如果 Block 不大）\n");
}

void test_deep_copy_problem()
{
    printf("\n[测试6] 深拷贝问题\n");
    printf("=====================================\n");

    Block original = {0};
    original.id = 1;
    original.next = (Block*)malloc(sizeof(Block)); // 指向另一个 Block
    original.next->id = 2;
    strcpy(original.data, "Original");

    printf("  如果 Block 包含指针字段:\n");
    printf("    Block 结构:\n");
    printf("      int id;\n");
    printf("      char data[64];\n");
    printf("      Block* next;  ← 指针字段！\n");

    printf("\n  值类型方式的问题:\n");
    printf("    MetaBlockReader_Value reader1, reader2;\n");
    printf("    reader1.block = original;  // 浅拷贝\n");
    printf("    reader2.block = reader1.block;  // 再次浅拷贝\n");
    printf("\n");
    printf("    ⚠ reader1.block.next == reader2.block.next\n");
    printf("    ⚠ 两个副本共享同一个 next 指针\n");
    printf("    ⚠ 释放时会 double-free！\n");

    printf("\n  指针方式:\n");
    printf("    reader1.block = &original;\n");
    printf("    reader2.block = &original;\n");
    printf("    ✓ 明确共享，生命周期管理清晰\n");

    free(original.next);
}

void print_recommendations()
{
    printf("\n");
    printf("╔════════════════════════════════════════════════════╗\n");
    printf("║              何时使用哪种方式？                    ║\n");
    printf("╚════════════════════════════════════════════════════╝\n");
    printf("\n");
    printf("使用指针（Block* block）的情况：\n");
    printf("  ✓ Block 结构较大（> 64 字节）\n");
    printf("  ✓ 需要共享同一个 Block\n");
    printf("  ✓ Block 的生命周期比 reader 长\n");
    printf("  ✓ 需要 NULL 表示 \"无 block\" 状态\n");
    printf("  ✓ Block 包含指针字段（避免浅拷贝问题）\n");
    printf("  ✓ 需要动态分配（堆上）\n");
    printf("\n");
    printf("使用值类型（Block block）的情况：\n");
    printf("  ✓ Block 结构较小（< 32 字节）\n");
    printf("  ✓ 每个 reader 独立拥有自己的 Block\n");
    printf("  ✓ 不需要共享\n");
    printf("  ✓ Block 不包含指针字段（或可以安全浅拷贝）\n");
    printf("  ✓ 追求缓存局部性和访问速度\n");
    printf("  ✓ 自动内存管理（栈分配）\n");
    printf("\n");
    printf("推荐：\n");
    printf("  对于 MetaBlockReader 这种场景，\n");
    printf("  使用指针（Block* block）更合适，因为：\n");
    printf("  1. Block 可能需要在多个 reader 间共享\n");
    printf("  2. Block 可能包含其他指针字段\n");
    printf("  3. 可以用 NULL 表示 \"当前无 block\" 状态\n");
    printf("  4. 生命周期管理更灵活\n");
    printf("\n");
}

int main()
{
    printf("\n");
    printf("╔════════════════════════════════════════════════════╗\n");
    printf("║       值类型 vs 指针：深度对比分析                ║\n");
    printf("╚════════════════════════════════════════════════════╝\n");

    test_memory_size();
    test_null_handling();
    test_sharing();
    test_initialization();
    test_cache_locality();
    test_deep_copy_problem();

    printf("\n");
    printf("╔════════════════════════════════════════════════════╗\n");
    printf("║              分析完成                              ║\n");
    printf("╚════════════════════════════════════════════════════╝\n");

    print_recommendations();

    return 0;
}
