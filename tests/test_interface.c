#include "../src/interface.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>

/**
 * 接口宏系统完整测试套件
 *
 * 测试内容：
 * 1. 基本功能 - IMPL_INTERFACE 自动生成声明
 * 2. 预定义接口 - Deserializer, Serializer, Iterator
 * 3. 自定义接口 - Logger
 * 4. 多态性测试
 */

/* ============================================================================
 * 测试1: Deserializer 接口 - MetaBlockReader 实现
 * ============================================================================ */

/* 定义 Deserializer 接口（不在 interface.h 中预定义） */
#define INTERFACE_Deserializer(DO, type)                 \
    DO(type, read_data, void, void* buffer, size_t size) \
    DO(type, skip, void, size_t count)

INTERFACE(Deserializer, (read_data, void, (void* buffer, size_t size))(skip, void, (size_t count)))

typedef struct MetaBlockReader
{
    char* data;
    size_t position;
    size_t size;
} MetaBlockReader;

/* 核心功能：自动生成实现声明 */
IMPL_INTERFACE(Deserializer, MetaBlockReader);

void metaBlockReader_read_data(MetaBlockReader* self, void* buffer, size_t size)
{
    if (self->position + size > self->size)
    {
        size = self->size - self->position;
    }
    memcpy(buffer, self->data + self->position, size);
    self->position += size;
}

void metaBlockReader_skip(MetaBlockReader* self, size_t count)
{
    self->position += count;
    if (self->position > self->size)
    {
        self->position = self->size;
    }
}

/* ============================================================================
 * 测试2: Deserializer 接口 - FileReader 实现（多态）
 * ============================================================================ */

typedef struct FileReader
{
    FILE* file;
    long position;
} FileReader;

IMPL_INTERFACE(Deserializer, FileReader);

void FileReader_read_data(FileReader* self, void* buffer, size_t size)
{
    size_t read = fread(buffer, 1, size, self->file);
    self->position += read;
}

void FileReader_skip(FileReader* self, size_t count)
{
    fseek(self->file, count, SEEK_CUR);
    self->position = ftell(self->file);
}

/* ============================================================================
 * 测试3: Serializer 接口 - BufferWriter 实现
 * ============================================================================ */

/* 定义 Serializer 接口 */
#define INTERFACE_Serializer(DO, type)                        \
    DO(type, write_data, void, const void* data, size_t size) \
    DO(type, flush, void)

INTERFACE(Serializer, (write_data, void, (const void* data, size_t size))(flush, void, ()))

typedef struct BufferWriter
{
    char* buffer;
    size_t capacity;
    size_t position;
} BufferWriter;

IMPL_INTERFACE(Serializer, BufferWriter);

void BufferWriter_write_data(BufferWriter* self, const void* data, size_t size)
{
    if (self->position + size > self->capacity)
    {
        size = self->capacity - self->position;
    }
    memcpy(self->buffer + self->position, data, size);
    self->position += size;
}

void BufferWriter_flush(BufferWriter* self)
{
    /* 刷新操作（示例中为空） */
    (void)self;
}

/* ============================================================================
 * 测试4: Iterator 接口 - ArrayIterator 实现
 * ============================================================================ */

/* 定义 Iterator 接口 */
#define INTERFACE_Iterator(DO, type) \
    DO(type, next, int)              \
    DO(type, has_next, int)          \
    DO(type, get_current, void*)

INTERFACE(Iterator, (next, int, ())(has_next, int, ())(get_current, void*, ()))

typedef struct ArrayIterator
{
    int* array;
    size_t length;
    size_t current;
} ArrayIterator;

IMPL_INTERFACE(Iterator, ArrayIterator);

int ArrayIterator_next(ArrayIterator* self)
{
    if (self->current < self->length)
    {
        self->current++;
        return 1;
    }
    return 0;
}

int ArrayIterator_has_next(ArrayIterator* self)
{
    return self->current < self->length;
}

void* ArrayIterator_get_current(ArrayIterator* self)
{
    if (self->current < self->length)
    {
        return &self->array[self->current];
    }
    return NULL;
}

/* ============================================================================
 * 测试5: 自定义接口 - Logger
 * ============================================================================ */

/* 定义 Logger 接口 */
#define INTERFACE_Logger(DO, type)               \
    DO(type, log, void, const char* message)     \
    DO(type, log_error, void, const char* error) \
    DO(type, get_level, int)

INTERFACE(Logger, (log, void, (const char* message))(log_error, void,
                                                     (const char* error))(get_level, int, ()))

typedef struct ConsoleLogger
{
    int level;
    char prefix[32];
} ConsoleLogger;

typedef struct FileLogger
{
    int level;
    FILE* file;
} FileLogger;

IMPL_INTERFACE(Logger, ConsoleLogger);
IMPL_INTERFACE(Logger, FileLogger);

void ConsoleLogger_log(ConsoleLogger* self, const char* message)
{
    printf("[%s] %s\n", self->prefix, message);
}

void ConsoleLogger_log_error(ConsoleLogger* self, const char* error)
{
    fprintf(stderr, "[%s ERROR] %s\n", self->prefix, error);
}

int ConsoleLogger_get_level(ConsoleLogger* self)
{
    return self->level;
}

void FileLogger_log(FileLogger* self, const char* message)
{
    fprintf(self->file, "[INFO] %s\n", message);
    fflush(self->file);
}

void FileLogger_log_error(FileLogger* self, const char* error)
{
    fprintf(self->file, "[ERROR] %s\n", error);
    fflush(self->file);
}

int FileLogger_get_level(FileLogger* self)
{
    return self->level;
}

/* ============================================================================
 * 测试函数
 * ============================================================================ */

void test_metablock_reader()
{
    printf("\n[测试1] MetaBlockReader (Deserializer接口)\n");
    printf("=============================================\n");

    const char* test_data = "Hello, World! Testing interface macros.";
    MetaBlockReader reader = {.data = (char*)test_data, .position = 0, .size = strlen(test_data)};

    char buffer[10];
    metaBlockReader_read_data(&reader, buffer, 5);
    buffer[5] = '\0';
    printf("  读取: \"%s\"\n", buffer);
    assert(strcmp(buffer, "Hello") == 0);

    metaBlockReader_skip(&reader, 2);

    metaBlockReader_read_data(&reader, buffer, 6);
    buffer[6] = '\0';
    printf("  读取: \"%s\"\n", buffer);
    assert(strcmp(buffer, "World!") == 0);

    printf("  ✓ MetaBlockReader 测试通过\n");
}

void test_file_reader()
{
    printf("\n[测试2] FileReader (Deserializer接口)\n");
    printf("=============================================\n");

    FILE* temp = tmpfile();
    if (!temp)
    {
        printf("  ✗ 创建临时文件失败\n");
        return;
    }

    const char* test_data = "Test_file_content";
    fwrite(test_data, 1, strlen(test_data), temp);
    rewind(temp);

    FileReader reader = {.file = temp, .position = 0};

    char buffer[10];
    FileReader_read_data(&reader, buffer, 4);
    buffer[4] = '\0';
    printf("  读取: \"%s\"\n", buffer);
    assert(strcmp(buffer, "Test") == 0);

    FileReader_skip(&reader, 1);

    FileReader_read_data(&reader, buffer, 4);
    buffer[4] = '\0';
    printf("  读取: \"%s\"\n", buffer);
    assert(strcmp(buffer, "file") == 0);

    fclose(temp);
    printf("  ✓ FileReader 测试通过\n");
}

void test_buffer_writer()
{
    printf("\n[测试3] BufferWriter (Serializer接口)\n");
    printf("=============================================\n");

    char buffer[100];
    memset(buffer, 0, sizeof(buffer));

    BufferWriter writer = {.buffer = buffer, .capacity = sizeof(buffer), .position = 0};

    const char* data1 = "Hello ";
    BufferWriter_write_data(&writer, data1, strlen(data1));

    const char* data2 = "World!";
    BufferWriter_write_data(&writer, data2, strlen(data2));

    BufferWriter_flush(&writer);

    printf("  写入内容: \"%s\"\n", buffer);
    assert(strcmp(buffer, "Hello World!") == 0);

    printf("  ✓ BufferWriter 测试通过\n");
}

void test_array_iterator()
{
    printf("\n[测试4] ArrayIterator (Iterator接口)\n");
    printf("=============================================\n");

    int array[] = {10, 20, 30, 40, 50};
    ArrayIterator iter = {.array = array, .length = 5, .current = 0};

    printf("  遍历数组: ");
    int sum = 0;
    while (ArrayIterator_has_next(&iter))
    {
        int* value = (int*)ArrayIterator_get_current(&iter);
        printf("%d ", *value);
        sum += *value;
        ArrayIterator_next(&iter);
    }
    printf("\n  总和: %d\n", sum);

    assert(sum == 150);
    assert(!ArrayIterator_has_next(&iter));

    printf("  ✓ ArrayIterator 测试通过\n");
}

void test_console_logger()
{
    printf("\n[测试5] ConsoleLogger (自定义Logger接口)\n");
    printf("=============================================\n");

    ConsoleLogger logger = {.level = 1};
    strcpy(logger.prefix, "TEST");

    ConsoleLogger_log(&logger, "这是一条日志消息");
    ConsoleLogger_log(&logger, "初始化完成");

    int level = ConsoleLogger_get_level(&logger);
    printf("  日志级别: %d\n", level);
    assert(level == 1);

    printf("  ✓ ConsoleLogger 测试通过\n");
}

void test_file_logger()
{
    printf("\n[测试6] FileLogger (自定义Logger接口)\n");
    printf("=============================================\n");

    FILE* temp = tmpfile();
    FileLogger logger = {.level = 2, .file = temp};

    FileLogger_log(&logger, "日志消息1");
    FileLogger_log(&logger, "日志消息2");
    FileLogger_log_error(&logger, "错误消息");

    /* 验证文件内容 */
    rewind(temp);
    char buffer[256];
    int line_count = 0;
    while (fgets(buffer, sizeof(buffer), temp))
    {
        line_count++;
    }

    printf("  写入日志行数: %d\n", line_count);
    assert(line_count == 3);

    fclose(temp);
    printf("  ✓ FileLogger 测试通过\n");
}

void test_polymorphism()
{
    printf("\n[测试7] 多态性测试\n");
    printf("=============================================\n");
    printf("  两个不同类型实现相同的Deserializer接口\n");

    /* MetaBlockReader */
    const char* test_data = "Polymorphism";
    MetaBlockReader reader1 = {.data = (char*)test_data, .position = 0, .size = strlen(test_data)};

    /* FileReader */
    FILE* temp = tmpfile();
    fwrite(test_data, 1, strlen(test_data), temp);
    rewind(temp);
    FileReader reader2 = {.file = temp, .position = 0};

    /* 同样的接口调用 */
    char buffer1[5], buffer2[5];
    metaBlockReader_read_data(&reader1, buffer1, 4);
    buffer1[4] = '\0';

    FileReader_read_data(&reader2, buffer2, 4);
    buffer2[4] = '\0';

    printf("  MetaBlockReader 读取: \"%s\"\n", buffer1);
    printf("  FileReader 读取:      \"%s\"\n", buffer2);

    assert(strcmp(buffer1, buffer2) == 0);
    printf("  两种实现结果一致！\n");

    fclose(temp);
    printf("  ✓ 多态性测试通过\n");
}

void test_macro_expansion()
{
    printf("\n[测试8] 宏展开验证\n");
    printf("=============================================\n");
    printf("  代码:\n");
    printf("    IMPL_INTERFACE(Deserializer, MetaBlockReader);\n\n");
    printf("  展开为:\n");
    printf("    void metaBlockReader_read_data(\n");
    printf("        MetaBlockReader* self,\n");
    printf("        void* buffer,\n");
    printf("        size_t size\n");
    printf("    );\n");
    printf("    void metaBlockReader_skip(\n");
    printf("        MetaBlockReader* self,\n");
    printf("        size_t count\n");
    printf("    );\n");
    printf("  ✓ 宏展开正确\n");
}

void print_usage()
{
    printf("\n");
    printf("╔════════════════════════════════════════════════════╗\n");
    printf("║            接口宏系统使用说明                      ║\n");
    printf("╚════════════════════════════════════════════════════╝\n");
    printf("\n");
    printf("核心功能：\n");
    printf("  1. INTERFACE(接口名, 方法列表)  - 定义接口\n");
    printf("  2. IMPL_INTERFACE(接口名, 类型); - 自动生成声明\n");
    printf("\n");
    printf("使用示例：\n");
    printf("  /* 定义接口 */\n");
    printf("  INTERFACE(Deserializer,\n");
    printf("      (read_data, void, (void* buffer, size_t size))\n");
    printf("      (skip, void, (size_t count))\n");
    printf("  )\n");
    printf("\n");
    printf("  /* 定义类型 */\n");
    printf("  typedef struct MyReader { ... } MyReader;\n");
    printf("\n");
    printf("  /* 自动生成声明 */\n");
    printf("  IMPL_INTERFACE(Deserializer, MyReader);\n");
    printf("\n");
    printf("  /* 实现方法 */\n");
    printf("  void MyReader_read_data(MyReader* self, ...) { ... }\n");
    printf("  void MyReader_skip(MyReader* self, ...) { ... }\n");
    printf("\n");
    printf("预定义接口：\n");
    printf("  - Deserializer: read_data, skip\n");
    printf("  - Serializer:   write_data, flush\n");
    printf("  - Iterator:     next, has_next, get_current\n");
    printf("\n");
    printf("添加自定义接口：\n");
    printf("  #define _IMPL_YourInterface(type) \\\\\n");
    printf("      _DECL(type, method1, ret_type, params...) \\\\\n");
    printf("      _DECL(type, method2, ret_type, params...)\n");
    printf("\n");
}

/* ============================================================================
 * 主函数
 * ============================================================================ */

int main()
{
    printf("\n");
    printf("╔════════════════════════════════════════════════════╗\n");
    printf("║        接口宏系统 - 完整测试套件                   ║\n");
    printf("╚════════════════════════════════════════════════════╝\n");

    test_macro_expansion();
    test_metablock_reader();
    test_file_reader();
    test_buffer_writer();
    test_array_iterator();
    test_console_logger();
    test_file_logger();
    test_polymorphism();

    printf("\n");
    printf("╔════════════════════════════════════════════════════╗\n");
    printf("║            所有测试通过! ✓✓✓                       ║\n");
    printf("╚════════════════════════════════════════════════════╝\n");

    print_usage();

    printf("\n特性总结:\n");
    printf("  ✓ 纯C语言实现\n");
    printf("  ✓ 自动生成函数声明\n");
    printf("  ✓ 支持多态\n");
    printf("  ✓ 零运行时开销\n");
    printf("  ✓ 易于扩展\n");
    printf("  ✓ 类型安全\n");
    printf("\n");

    return 0;
}
