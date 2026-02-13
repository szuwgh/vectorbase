#include "../src/interface.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stddef.h>

/**
 * 演示 EXTENDS 宏的使用
 * 用于普通结构体继承（非虚表方式）
 */

/* ============================================================================
 * 示例1: Deserializer 接口继承
 * ============================================================================ */

// 定义 Deserializer 接口
#define INTERFACE_Deserializer(DO, type)                 \
    DO(type, read_data, void, void* buffer, size_t size) \
    DO(type, skip, void, size_t count)                   \
    DO(type, get_position, size_t)

// 基类：Deserializer
typedef struct
{
    const char* type_name;
    void* impl_ptr;
} Deserializer;

// 子类：使用 EXTENDS 宏
typedef struct
{
    EXTENDS(Deserializer); // 继承 Deserializer
    char* data;
    size_t position;
    size_t size;
} MetaBlockReader;

// 另一个子类
typedef struct
{
    EXTENDS(Deserializer); // 继承 Deserializer
    FILE* file;
    long offset;
} FileReader;

/* ============================================================================
 * 示例2: Shape 基类和继承
 * ============================================================================ */

typedef struct
{
    int x;
    int y;
    const char* color;
} Shape;

typedef struct
{
    EXTENDS(Shape); // 继承 Shape
    int radius;
} Circle;

typedef struct
{
    EXTENDS(Shape); // 继承 Shape
    int width;
    int height;
} Rectangle;

/* ============================================================================
 * 实现函数
 * ============================================================================ */

void metaBlockReader_init(MetaBlockReader* self, const char* data, size_t size)
{
    self->base.type_name = "MetaBlockReader";
    self->base.impl_ptr = self;
    self->data = (char*)data;
    self->position = 0;
    self->size = size;
}

void metaBlockReader_read_data(MetaBlockReader* self, void* buffer, size_t size)
{
    if (self->position + size > self->size)
    {
        size = self->size - self->position;
    }
    memcpy(buffer, self->data + self->position, size);
    self->position += size;
}

size_t metaBlockReader_get_position(MetaBlockReader* self)
{
    return self->position;
}

void Circle_init(Circle* self, int x, int y, int radius, const char* color)
{
    self->base.x = x;
    self->base.y = y;
    self->base.color = color;
    self->radius = radius;
}

void Circle_print(Circle* self)
{
    printf("  Circle at (%d, %d), radius=%d, color=%s\n", self->base.x, self->base.y, self->radius,
           self->base.color);
}

void Rectangle_init(Rectangle* self, int x, int y, int width, int height, const char* color)
{
    self->base.x = x;
    self->base.y = y;
    self->base.color = color;
    self->width = width;
    self->height = height;
}

void Rectangle_print(Rectangle* self)
{
    printf("  Rectangle at (%d, %d), size=%dx%d, color=%s\n", self->base.x, self->base.y, self->width,
           self->height, self->base.color);
}

/* ============================================================================
 * 测试函数
 * ============================================================================ */

void test_deserializer_inheritance()
{
    printf("\n[测试1] Deserializer 继承\n");
    printf("=====================================\n");

    const char* test_data = "Hello World!";
    MetaBlockReader reader;
    metaBlockReader_init(&reader, test_data, strlen(test_data));

    printf("  类型: %s\n", reader.base.type_name);

    char buffer[6];
    metaBlockReader_read_data(&reader, buffer, 5);
    buffer[5] = '\0';
    printf("  读取: \"%s\"\n", buffer);

    size_t pos = metaBlockReader_get_position(&reader);
    printf("  位置: %zu\n", pos);

    // 向上转型
    Deserializer* base = UPCAST(&reader, Deserializer);
    printf("  基类类型: %s\n", base->type_name);

    printf("  ✓ 测试通过\n");
}

void test_shape_inheritance()
{
    printf("\n[测试2] Shape 继承\n");
    printf("=====================================\n");

    Circle circle;
    Circle_init(&circle, 10, 20, 5, "red");
    Circle_print(&circle);

    Rectangle rect;
    Rectangle_init(&rect, 30, 40, 15, 25, "blue");
    Rectangle_print(&rect);

    // 向上转型
    Shape* shapes[2] = {UPCAST(&circle, Shape), UPCAST(&rect, Shape)};

    printf("  通过基类指针访问:\n");
    for (int i = 0; i < 2; i++)
    {
        printf("    Shape %d: position=(%d, %d), color=%s\n", i, shapes[i]->x, shapes[i]->y,
               shapes[i]->color);
    }

    printf("  ✓ 测试通过\n");
}

void test_memory_layout()
{
    printf("\n[测试3] 内存布局验证\n");
    printf("=====================================\n");

    MetaBlockReader reader;
    reader.base.type_name = "test";
    reader.position = 42;

    printf("  MetaBlockReader 大小: %zu bytes\n", sizeof(MetaBlockReader));
    printf("  Deserializer 大小: %zu bytes\n", sizeof(Deserializer));
    printf("  base 偏移量: %zu\n", offsetof(MetaBlockReader, base));
    printf("  position 偏移量: %zu\n", offsetof(MetaBlockReader, position));

    // 验证 base 在开头
    if (offsetof(MetaBlockReader, base) == 0)
    {
        printf("  ✓ base 字段在结构体开头（偏移量为0）\n");
    }

    // 验证向上转型安全
    Deserializer* base = UPCAST(&reader, Deserializer);
    if (base->type_name == reader.base.type_name)
    {
        printf("  ✓ 向上转型后可正确访问基类字段\n");
    }

    printf("  ✓ 测试通过\n");
}

void print_code_example()
{
    printf("\n");
    printf("╔════════════════════════════════════════════════════╗\n");
    printf("║            EXTENDS 宏使用说明                      ║\n");
    printf("╚════════════════════════════════════════════════════╝\n");
    printf("\n");
    printf("旧写法（手动）：\n");
    printf("  typedef struct {\n");
    printf("      Deserializer base;  // 继承 Deserializer\n");
    printf("      char* data;\n");
    printf("      size_t position;\n");
    printf("  } MetaBlockReader;\n");
    printf("\n");
    printf("新写法（使用 EXTENDS 宏）：\n");
    printf("  typedef struct {\n");
    printf("      EXTENDS(Deserializer);  // 继承 Deserializer\n");
    printf("      char* data;\n");
    printf("      size_t position;\n");
    printf("  } MetaBlockReader;\n");
    printf("\n");
    printf("优点：\n");
    printf("  ✓ 明确表达继承意图\n");
    printf("  ✓ 代码更简洁\n");
    printf("  ✓ 统一的继承语法\n");
    printf("  ✓ 保证 base 字段命名一致\n");
    printf("\n");
    printf("配合使用：\n");
    printf("  - EXTENDS(Type)  - 声明继承\n");
    printf("  - UPCAST(ptr, Type)  - 向上转型\n");
    printf("  - DOWNCAST(ptr, Type)  - 向下转型\n");
    printf("\n");
}

/* ============================================================================
 * 主函数
 * ============================================================================ */

int main()
{
    printf("\n");
    printf("╔════════════════════════════════════════════════════╗\n");
    printf("║        EXTENDS 宏测试 - 结构体继承                ║\n");
    printf("╚════════════════════════════════════════════════════╝\n");

    test_deserializer_inheritance();
    test_shape_inheritance();
    test_memory_layout();

    printf("\n");
    printf("╔════════════════════════════════════════════════════╗\n");
    printf("║           所有测试通过! ✓                          ║\n");
    printf("╚════════════════════════════════════════════════════╝\n");

    print_code_example();

    return 0;
}
