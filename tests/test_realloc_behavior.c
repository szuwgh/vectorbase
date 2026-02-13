#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>

/**
 * 深度分析：realloc 的行为和内存管理
 *
 * 核心问题：realloc 会自动释放旧指针吗？
 */

/* ============================================================================
 * realloc 的三种情况
 * ============================================================================ */

void test_realloc_in_place()
{
    printf("\n[情况1] realloc 就地扩展（原地址后有空间）\n");
    printf("================================================\n");

    void* ptr1 = malloc(100);
    printf("  原指针: %p\n", ptr1);
    memset(ptr1, 0xAA, 100);

    // 尝试小幅扩展，可能就地进行
    void* ptr2 = realloc(ptr1, 120);
    printf("  新指针: %p\n", ptr2);

    if (ptr1 == ptr2)
    {
        printf("  ✓ 地址相同 - 就地扩展\n");
        printf("  ✓ realloc 没有移动内存\n");
        printf("  ✓ ptr1 和 ptr2 指向同一位置\n");
        printf("  ✓ 不需要释放 ptr1（因为就是同一个指针）\n");
    }
    else
    {
        printf("  ✗ 地址不同 - 重新分配了\n");
        printf("  ⚠ 注意：realloc 已经自动释放了 ptr1！\n");
    }

    free(ptr2);
}

void test_realloc_relocate()
{
    printf("\n[情况2] realloc 重新分配（需要移动到新位置）\n");
    printf("================================================\n");

    void* ptr1 = malloc(100);
    printf("  原指针: %p (100 字节)\n", ptr1);
    memcpy(ptr1, "Hello", 6);

    // 大幅扩展，很可能需要重新分配
    void* ptr2 = realloc(ptr1, 10000);
    printf("  新指针: %p (10000 字节)\n", ptr2);

    if (ptr1 != ptr2)
    {
        printf("  ✓ 地址不同 - realloc 重新分配了内存\n");
        printf("  ✓ realloc 自动做了这些事情：\n");
        printf("      1. 分配新的 10000 字节内存\n");
        printf("      2. 复制旧数据（100字节）到新内存\n");
        printf("      3. ⭐ 自动释放旧内存（ptr1）\n");
        printf("      4. 返回新指针（ptr2）\n");
        printf("\n");
        printf("  ⚠ 重要：ptr1 已经被 realloc 释放了！\n");
        printf("  ⚠ 不能再使用 ptr1，也不能 free(ptr1)！\n");
        printf("  ✓ 数据已复制: %s\n", (char*)ptr2);
    }

    free(ptr2); // 只需要释放新指针
}

void test_realloc_failure()
{
    printf("\n[情况3] realloc 失败（内存不足）\n");
    printf("================================================\n");

    void* ptr1 = malloc(100);
    printf("  原指针: %p\n", ptr1);
    memcpy(ptr1, "Important data", 15);

    // 尝试分配一个超大内存（可能失败）
    // 注意：这里可能不会真的失败，取决于系统
    printf("  尝试 realloc 到 SIZE_MAX/2...\n");
    void* ptr2 = realloc(ptr1, SIZE_MAX / 2);

    if (ptr2 == NULL)
    {
        printf("  ✓ realloc 返回 NULL（失败）\n");
        printf("  ⭐ 关键：ptr1 仍然有效！\n");
        printf("  ⭐ realloc 失败时不会释放原内存！\n");
        printf("  ✓ 可以继续使用 ptr1: %s\n", (char*)ptr1);
        printf("  ✓ 必须手动 free(ptr1)\n");
        free(ptr1); // 必须释放
    }
    else
    {
        printf("  ✗ realloc 成功了（分配了巨大内存）\n");
        printf("  ⭐ ptr1 已被 realloc 自动释放\n");
        free(ptr2);
    }
}

/* ============================================================================
 * 常见错误示例
 * ============================================================================ */

void test_common_mistake_1()
{
    printf("\n[错误示例1] Double Free\n");
    printf("================================================\n");

    void* ptr1 = malloc(100);
    printf("  原指针: %p\n", ptr1);

    void* ptr2 = realloc(ptr1, 200);
    printf("  新指针: %p\n", ptr2);

    printf("\n  ❌ 错误代码:\n");
    printf("     free(ptr1);  // 错误！ptr1 可能已被 realloc 释放\n");
    printf("     free(ptr2);  // 第二次释放（如果 ptr1==ptr2 就是 double free）\n");

    printf("\n  ✓ 正确做法:\n");
    printf("     // 不要 free(ptr1)！\n");
    printf("     free(ptr2);  // 只释放新指针\n");

    free(ptr2);
}

void test_common_mistake_2()
{
    printf("\n[错误示例2] 覆盖原指针导致内存泄漏\n");
    printf("================================================\n");

    printf("  ❌ 错误代码:\n");
    printf("     void* ptr = malloc(100);\n");
    printf("     ptr = realloc(ptr, 200);  // 如果失败，ptr 变成 NULL\n");
    printf("                                // 原内存泄漏！\n");

    printf("\n  ✓ 正确做法:\n");
    printf("     void* ptr = malloc(100);\n");
    printf("     void* new_ptr = realloc(ptr, 200);\n");
    printf("     if (new_ptr) {\n");
    printf("         ptr = new_ptr;  // 成功才更新\n");
    printf("     }\n");
    printf("     // 失败时 ptr 仍然有效，可继续使用或释放\n");
}

/* ============================================================================
 * 分析原始 vector_grow 代码
 * ============================================================================ */

void analyze_original_code()
{
    printf("\n[分析原始代码] vector_grow 函数\n");
    printf("================================================\n");

    printf("  代码:\n");
    printf("  ┌────────────────────────────────────────────┐\n");
    printf("  │ void* new_data = realloc(vec->data,       │\n");
    printf("  │     vec->element_size * new_capacity);    │\n");
    printf("  │                                            │\n");
    printf("  │ if (!new_data) {                           │\n");
    printf("  │     return -1;  // 失败                    │\n");
    printf("  │ }                                          │\n");
    printf("  │                                            │\n");
    printf("  │ vec->data = new_data;  // 成功才更新      │\n");
    printf("  └────────────────────────────────────────────┘\n");

    printf("\n  问题：需要释放旧的 vec->data 吗？\n");
    printf("  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n");

    printf("\n  答案：❌ 不需要！\n");
    printf("\n  理由：\n");
    printf("  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n");

    printf("\n  1️⃣  realloc 成功时：\n");
    printf("     • realloc 会自动释放旧内存（vec->data）\n");
    printf("     • 返回新内存指针（new_data）\n");
    printf("     • 更新 vec->data = new_data\n");
    printf("     ✓ 旧指针已被 realloc 自动释放\n");

    printf("\n  2️⃣  realloc 失败时：\n");
    printf("     • realloc 返回 NULL\n");
    printf("     • ⭐ 旧内存（vec->data）仍然有效！\n");
    printf("     • 不更新 vec->data（保持原值）\n");
    printf("     • 函数返回 -1\n");
    printf("     ✓ 调用者可继续使用 vec（降级处理）\n");

    printf("\n  结论：\n");
    printf("  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n");
    printf("  ✓ 这段代码是正确的！\n");
    printf("  ✓ 不需要手动 free 旧指针\n");
    printf("  ✓ realloc 会自动管理内存\n");

    printf("\n  ⚠️  如果手动 free 旧指针会怎样？\n");
    printf("  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n");
    printf("  ❌ 错误代码:\n");
    printf("     void* old_data = vec->data;\n");
    printf("     void* new_data = realloc(vec->data, new_size);\n");
    printf("     if (new_data) {\n");
    printf("         free(old_data);  // ❌ 致命错误！\n");
    printf("         vec->data = new_data;\n");
    printf("     }\n");
    printf("\n");
    printf("  结果：\n");
    printf("     • 如果 realloc 重新分配：Double Free!\n");
    printf("     • 如果 realloc 就地扩展：Double Free!\n");
    printf("     • 程序崩溃！\n");
}

/* ============================================================================
 * 最佳实践
 * ============================================================================ */

void print_best_practices()
{
    printf("\n");
    printf("╔════════════════════════════════════════════════════╗\n");
    printf("║           realloc 使用最佳实践                     ║\n");
    printf("╚════════════════════════════════════════════════════╝\n");

    printf("\n✓ 正确用法:\n");
    printf("  ┌────────────────────────────────────────────────┐\n");
    printf("  │ void* new_ptr = realloc(old_ptr, new_size);   │\n");
    printf("  │ if (new_ptr) {                                 │\n");
    printf("  │     old_ptr = new_ptr;  // 只有成功才更新     │\n");
    printf("  │ } else {                                       │\n");
    printf("  │     // old_ptr 仍然有效，可以：               │\n");
    printf("  │     // 1. 继续使用                            │\n");
    printf("  │     // 2. 降级处理                            │\n");
    printf("  │     // 3. 清理并返回错误                      │\n");
    printf("  │ }                                              │\n");
    printf("  └────────────────────────────────────────────────┘\n");

    printf("\n❌ 常见错误:\n");
    printf("  1. 覆盖原指针：\n");
    printf("     ptr = realloc(ptr, size);  // ❌ 失败时 ptr 丢失\n");

    printf("\n  2. 手动释放旧指针：\n");
    printf("     free(old_ptr);  // ❌ realloc 已经释放了\n");

    printf("\n  3. 忘记检查返回值：\n");
    printf("     ptr = realloc(ptr, size);\n");
    printf("     // ❌ 没检查是否为 NULL\n");

    printf("\n⭐ 关键要点:\n");
    printf("  • realloc 成功时会自动释放旧内存\n");
    printf("  • realloc 失败时不会释放旧内存\n");
    printf("  • 永远不要手动 free realloc 的旧指针\n");
    printf("  • 总是用临时变量接收 realloc 返回值\n");
    printf("  • 失败时原指针仍然有效且必须释放\n");

    printf("\n");
}

/* ============================================================================
 * 主函数
 * ============================================================================ */

int main()
{
    printf("\n");
    printf("╔════════════════════════════════════════════════════╗\n");
    printf("║     realloc 内存管理深度分析                      ║\n");
    printf("╚════════════════════════════════════════════════════╝\n");

    test_realloc_in_place();
    test_realloc_relocate();
    test_realloc_failure();

    test_common_mistake_1();
    test_common_mistake_2();

    analyze_original_code();
    print_best_practices();

    printf("\n");
    printf("╔════════════════════════════════════════════════════╗\n");
    printf("║              分析完成                              ║\n");
    printf("╚════════════════════════════════════════════════════╝\n");
    printf("\n");

    return 0;
}
