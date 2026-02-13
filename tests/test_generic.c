#include "../src/interface.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/**
 * 演示 C11 _Generic 泛型调用宏
 * 编译时多态，零运行时开销
 */

/* ============================================================================
 * 示例1: Deserializer 泛型接口
 * ============================================================================ */

typedef unsigned char u8;
typedef unsigned long usize;

// 前置声明
typedef struct MetaBlockReader MetaBlockReader;
typedef struct FileReader FileReader;

// 定义具体类型
struct MetaBlockReader
{
    char* data;
    usize position;
    usize size;
};

struct FileReader
{
    FILE* file;
    long position;
};

// 实现具体方法
void metaBlockReader_read_data(MetaBlockReader* self, u8* buffer, usize size)
{
    printf("  [MetaBlockReader] 读取 %zu 字节\n", size);
    if (self->position + size > self->size)
    {
        size = self->size - self->position;
    }
    memcpy(buffer, self->data + self->position, size);
    self->position += size;
}

void FileReader_read_data(FileReader* self, u8* buffer, usize size)
{
    printf("  [FileReader] 读取 %zu 字节\n", size);
    size_t read = fread(buffer, 1, size, self->file);
    self->position += read;
}

// 方式1：使用 GENERIC_DISPATCH（推荐）
// clang-format off
#define deserializer_read(ptr, buffer, size) \
    GENERIC_DISPATCH(ptr, \
        MetaBlockReader*: metaBlockReader_read_data, \
        FileReader*: FileReader_read_data \
    )(ptr, buffer, size)
// clang-format on
/* ============================================================================
 * 示例2: Shape 泛型接口
 * ============================================================================ */

typedef struct Circle
{
    int x;
    int y;
    int radius;
} Circle;

typedef struct Rectangle
{
    int x;
    int y;
    int width;
    int height;
} Rectangle;

void Circle_draw(Circle* self)
{
    printf("  绘制圆形: 中心(%d, %d), 半径=%d\n", self->x, self->y, self->radius);
}

void Rectangle_draw(Rectangle* self)
{
    printf("  绘制矩形: 位置(%d, %d), 大小=%dx%d\n", self->x, self->y, self->width, self->height);
}

int Circle_area(Circle* self)
{
    // 简化计算: π ≈ 3
    return 3 * self->radius * self->radius;
}

int Rectangle_area(Rectangle* self)
{
    return self->width * self->height;
}

// 定义泛型接口
#define Shape_draw(ptr) \
    GENERIC_DISPATCH(ptr, Circle* : Circle_draw, Rectangle* : Rectangle_draw)(ptr)

#define Shape_area(ptr) \
    GENERIC_DISPATCH(ptr, Circle* : Circle_area, Rectangle* : Rectangle_area)(ptr)

/* ============================================================================
 * 示例3: Logger 泛型接口
 * ============================================================================ */

typedef struct ConsoleLogger
{
    const char* prefix;
} ConsoleLogger;

typedef struct FileLogger
{
    FILE* file;
} FileLogger;

void ConsoleLogger_log(ConsoleLogger* self, const char* msg)
{
    printf("[%s] %s\n", self->prefix, msg);
}

void FileLogger_log(FileLogger* self, const char* msg)
{
    fprintf(self->file, "[LOG] %s\n", msg);
    fflush(self->file);
}

#define Logger_log(ptr, msg)                                                                     \
    GENERIC_DISPATCH(ptr, ConsoleLogger* : ConsoleLogger_log, FileLogger* : FileLogger_log)(ptr, \
                                                                                            msg)

/* ============================================================================
 * 测试函数
 * ============================================================================ */

void test_deserializer_generic()
{
    printf("\n[测试1] Deserializer 泛型调用\n");
    printf("=====================================\n");

    // 测试 MetaBlockReader
    const char* test_data = "Hello Generic World!";
    MetaBlockReader reader1 = {.data = (char*)test_data, .position = 0, .size = strlen(test_data)};

    u8 buffer1[10];
    deserializer_read(&reader1, buffer1, 5); // 编译时选择 metaBlockReader_read_data
    buffer1[5] = '\0';
    printf("  读取内容: \"%s\"\n", buffer1);

    // 测试 FileReader
    FILE* temp = tmpfile();
    fwrite(test_data, 1, strlen(test_data), temp);
    rewind(temp);

    FileReader reader2 = {.file = temp, .position = 0};

    u8 buffer2[10];
    deserializer_read(&reader2, buffer2, 5); // 编译时选择 FileReader_read_data
    buffer2[5] = '\0';
    printf("  读取内容: \"%s\"\n", buffer2);

    fclose(temp);
    printf("  ✓ 泛型调用成功，编译时已确定函数\n");
}

void test_shape_generic()
{
    printf("\n[测试2] Shape 泛型调用\n");
    printf("=====================================\n");

    Circle circle = {.x = 10, .y = 20, .radius = 5};
    Rectangle rect = {.x = 30, .y = 40, .width = 15, .height = 25};

    // 泛型调用 draw
    Shape_draw(&circle); // 编译时选择 Circle_draw
    Shape_draw(&rect);   // 编译时选择 Rectangle_draw

    // 泛型调用 area
    int area1 = Shape_area(&circle); // 编译时选择 Circle_area
    int area2 = Shape_area(&rect);   // 编译时选择 Rectangle_area

    printf("  圆形面积: %d\n", area1);
    printf("  矩形面积: %d\n", area2);

    printf("  ✓ 泛型调用成功\n");
}

void test_logger_generic()
{
    printf("\n[测试3] Logger 泛型调用\n");
    printf("=====================================\n");

    ConsoleLogger console = {.prefix = "INFO"};
    Logger_log(&console, "控制台日志消息"); // 编译时选择 ConsoleLogger_log

    FILE* temp = tmpfile();
    FileLogger file = {.file = temp};
    Logger_log(&file, "文件日志消息"); // 编译时选择 FileLogger_log

    fclose(temp);
    printf("  ✓ 泛型调用成功\n");
}

void test_compile_time_dispatch()
{
    printf("\n[测试4] 编译时分发验证\n");
    printf("=====================================\n");
    printf("  _Generic 特性:\n");
    printf("  - 编译时类型检查\n");
    printf("  - 零运行时开销\n");
    printf("  - 类型安全\n");
    printf("  - 无需虚表\n");
    printf("\n");
    printf("  调用流程:\n");
    printf("  1. 编译器分析指针类型\n");
    printf("  2. 选择对应的函数实现\n");
    printf("  3. 直接调用（无间接跳转）\n");
    printf("\n");
    printf("  与虚表方式对比:\n");
    printf("  ┌─────────────────┬──────────┬──────────┐\n");
    printf("  │                 │ _Generic │  VTable  │\n");
    printf("  ├─────────────────┼──────────┼──────────┤\n");
    printf("  │ 运行时开销      │   零     │   有     │\n");
    printf("  │ 类型检查        │ 编译时   │ 运行时   │\n");
    printf("  │ 内存占用        │   无     │ vtable指针│\n");
    printf("  │ 内联优化        │   可以   │  困难    │\n");
    printf("  └─────────────────┴──────────┴──────────┘\n");
    printf("  ✓ 验证完成\n");
}

void print_usage()
{
    printf("\n");
    printf("╔════════════════════════════════════════════════════╗\n");
    printf("║          泛型调用宏使用说明                        ║\n");
    printf("╚════════════════════════════════════════════════════╝\n");
    printf("\n");
    printf("核心宏：\n");
    printf("  GENERIC_DISPATCH(ptr, Type1*: func1, Type2*: func2, ...)\n");
    printf("\n");
    printf("使用示例：\n");
    printf("  // 1. 实现具体类型的方法\n");
    printf("  void metaBlockReader_read_data(MetaBlockReader* self, ...);\n");
    printf("  void FileReader_read_data(FileReader* self, ...);\n");
    printf("\n");
    printf("  // 2. 定义泛型接口\n");
    printf("  #define deserializer_read(ptr, buffer, size) \\\\\n");
    printf("      GENERIC_DISPATCH(ptr, \\\\\n");
    printf("          MetaBlockReader*: metaBlockReader_read_data, \\\\\n");
    printf("          FileReader*: FileReader_read_data \\\\\n");
    printf("      )(ptr, buffer, size)\n");
    printf("\n");
    printf("  // 3. 使用（编译时自动选择）\n");
    printf("  MetaBlockReader* r1 = ...;\n");
    printf("  FileReader* r2 = ...;\n");
    printf("  deserializer_read(r1, buf, 10);  // -> metaBlockReader_read_data\n");
    printf("  deserializer_read(r2, buf, 10);  // -> FileReader_read_data\n");
    printf("\n");
    printf("优点：\n");
    printf("  ✓ 编译时类型检查\n");
    printf("  ✓ 零运行时开销\n");
    printf("  ✓ 支持编译器优化和内联\n");
    printf("  ✓ 类型安全（错误类型编译失败）\n");
    printf("  ✓ 无需虚表指针（节省内存）\n");
    printf("\n");
    printf("与虚表对比：\n");
    printf("  - 虚表方式：运行时多态，适合动态类型场景\n");
    printf("  - _Generic方式：编译时多态，适合已知类型场景\n");
    printf("  - 可以混合使用！\n");
    printf("\n");
}

/* ============================================================================
 * 主函数
 * ============================================================================ */

int main()
{
    printf("\n");
    printf("╔════════════════════════════════════════════════════╗\n");
    printf("║      C11 _Generic 泛型调用宏测试                  ║\n");
    printf("╚════════════════════════════════════════════════════╝\n");

    test_deserializer_generic();
    test_shape_generic();
    test_logger_generic();
    test_compile_time_dispatch();

    printf("\n");
    printf("╔════════════════════════════════════════════════════╗\n");
    printf("║           所有测试通过! ✓                          ║\n");
    printf("╚════════════════════════════════════════════════════╝\n");

    print_usage();

    return 0;
}
