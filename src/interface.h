#ifndef INTERFACE_H
#define INTERFACE_H

/* ============================================================================
 * 核心宏
 * ============================================================================ */

/**
 * IMPL_INTERFACE - 为具体类型生成实现声明
 *
 * 前提：需要先定义 INTERFACE_<name> 宏
 *
 * 示例：
 * #define INTERFACE_Deserializer(DO, type) \
 *     DO(type, read_data, void, void* buffer, size_t size) \
 *     DO(type, skip, void, size_t count)
 *
 * IMPL_INTERFACE(Deserializer, MetaBlockReader);
 *
 * 展开为：
 * void MetaBlockReader_read_data(MetaBlockReader* self, void* buffer, size_t size);
 * void MetaBlockReader_skip(MetaBlockReader* self, size_t count);
 */
#define IMPL_INTERFACE(interface_name, concrete_type) \
    INTERFACE_##interface_name(_DECL, concrete_type)

/**
 * _DECL - 生成单个方法声明
 */
#define _DECL(type, name, ret, ...) ret type##_##name(type* self, ##__VA_ARGS__);

/**
 * INTERFACE - 可选宏，仅用于文档（可以不调用）
 */
#define INTERFACE(name, ...) /* 文档用途，可选 */

/* ============================================================================
 * 虚表（VTable）和类继承支持
 * ============================================================================ */

/**
 * BEGIN_VTABLE - 开始定义虚表
 *
 * 示例：
 * BEGIN_VTABLE(BlockManager)
 *     VMETHOD(BlockManager, read, void, Block* block)
 *     VMETHOD(BlockManager, write, void, Block* block)
 *     VMETHOD(BlockManager, create_block, Block*)
 *     VMETHOD(BlockManager, destroy, void)
 * END_VTABLE(BlockManager)
 */
#define BEGIN_VTABLE(name)      \
    typedef struct name name;   \
    typedef struct name##VTable \
    {
#define VMETHOD(class_name, method_name, ret_type, ...) \
    ret_type (*method_name)(class_name * self, ##__VA_ARGS__);

#define END_VTABLE(name) \
    }                    \
    name##VTable;

/**
 * BEGIN_CLASS - 开始定义类（带虚表）
 *
 * 示例：
 * BEGIN_CLASS(BlockManager)
 *     VTABLE_FIELD(BlockManagerVTable)
 *     FIELD(type, BlockManagerType)
 * END_CLASS(BlockManager)
 */
#define BEGIN_CLASS(name) \
    struct name           \
    {
#define VTABLE_FIELD(vtable_type) vtable_type* vtable;

#define FIELD(name, type)         type name;

#define END_CLASS(name) \
    }                   \
    ;

/**
 * IMPL_VTABLE_METHOD - 为虚表方法提供实现
 *
 * 示例：
 * IMPL_VTABLE_METHOD(SingleFileBlockManager, read, void, Block* block)
 *
 * 展开为：
 * void SingleFileBlockManager_read(SingleFileBlockManager* self, Block* block);
 */
#define IMPL_VTABLE_METHOD(class_name, method_name, ret_type, ...) \
    ret_type class_name##_##method_name(class_name* self, ##__VA_ARGS__);

/**
 * VTABLE_INIT - 初始化虚表的辅助宏
 *
 * 示例：
 * static BlockManagerVTable single_file_vtable = {
 *     VTABLE_ENTRY(read, SingleFileBlockManager_read),
 *     VTABLE_ENTRY(write, SingleFileBlockManager_write),
 *     VTABLE_ENTRY(create_block, SingleFileBlockManager_create_block),
 *     VTABLE_ENTRY(destroy, SingleFileBlockManager_destroy)
 * };
 */
#define VTABLE_ENTRY(method_name, impl) .method_name = (void*)impl

/**
 * DEFINE_CLASS - 一步完成类和虚表定义（简化版）
 *
 * 示例：
 * DEFINE_CLASS(BlockManager,
 *     // 虚方法
 *     VMETHOD(BlockManager, read, void, Block* block)
 *     VMETHOD(BlockManager, write, void, Block* block)
 *     VMETHOD(BlockManager, create_block, Block*)
 *     VMETHOD(BlockManager, destroy, void)
 *     ,
 *     // 类字段
 *     FIELD(type, BlockManagerType)
 * )
 */
#define DEFINE_CLASS(class_name, methods, fields)                                           \
    BEGIN_VTABLE(class_name)                                                                \
    methods END_VTABLE(class_name) BEGIN_CLASS(class_name) VTABLE_FIELD(class_name##VTable) \
        fields END_CLASS(class_name)

/**
 * EXTENDS - 声明继承关系（用于普通结构体）
 *
 * 示例：
 * typedef struct {
 *     EXTENDS(Deserializer);  // 继承 Deserializer
 *     int my_field;
 * } MetaBlockReader;
 */
#define EXTENDS(base_type)                   base_type base

/**
 * UPCAST - 向上转型（子类转父类）
 */
#define UPCAST(ptr, base_type)               ((base_type*)ptr)

/**
 * DOWNCAST - 向下转型（父类转子类，不安全）
 */
#define DOWNCAST(ptr, derived_type)          ((derived_type*)ptr)

/**
 * VCALL - 通过虚表调用方法
 *
 * 示例：
 * VCALL(block_manager, read, block);
 * 等价于：
 * block_manager->vtable->read(block_manager, block);
 */
#define VCALL(obj, method, ...)              ((obj)->vtable->method((obj), ##__VA_ARGS__))

/* ============================================================================
 * 泛型调用宏（C11 _Generic）- 编译时多态
 * ============================================================================ */

/**
 * GENERIC_CALL - 泛型函数调用（编译时展开，零运行时开销）
 *
 * 使用 C11 _Generic 特性根据指针类型在编译时选择正确的函数
 *
 * 示例：
 * // 定义泛型调用
 * #define Deserializer_read(dr_ptr, buffer, size) \
 *     GENERIC_CALL(dr_ptr, read_data, buffer, size, \
 *         MetaBlockReader*: MetaBlockReader_read_data, \
 *         FileReader*: FileReader_read_data \
 *     )
 *
 * // 使用
 * MetaBlockReader* reader1 = ...;
 * FileReader* reader2 = ...;
 * Deserializer_read(reader1, buf, 10);  // 调用 MetaBlockReader_read_data
 * Deserializer_read(reader2, buf, 10);  // 调用 FileReader_read_data
 */
#define GENERIC_CALL(ptr, method, ...)       _GENERIC_CALL_IMPL(ptr, method, __VA_ARGS__)

/**
 * _GENERIC_CALL_IMPL - 内部实现宏
 *
 * 参数：
 * - ptr: 对象指针
 * - method: 方法名（不使用，仅用于文档）
 * - ...: 可变参数，最后的参数是类型映射
 *
 * 格式：arg1, arg2, ..., Type1*: func1, Type2*: func2, ...
 */
#define _GENERIC_CALL_IMPL(ptr, method, ...) _GENERIC_CALL_EXTRACT(ptr, __VA_ARGS__)

/**
 * _GENERIC_CALL_EXTRACT - 提取参数和类型映射
 */
#define _GENERIC_CALL_EXTRACT(ptr, ...) \
    _GENERIC_CALL_SELECT(ptr, __VA_ARGS__)(ptr, _GENERIC_CALL_ARGS(__VA_ARGS__))

/**
 * _GENERIC_CALL_SELECT - 使用 _Generic 选择函数
 */
#define _GENERIC_CALL_SELECT(ptr, ...) _Generic((ptr), _GENERIC_CALL_TYPES(__VA_ARGS__))

/**
 * _GENERIC_CALL_TYPES - 提取类型映射部分
 * 这是一个辅助宏，需要用户手动提供类型映射
 */
#define _GENERIC_CALL_TYPES(...)       __VA_ARGS__

/**
 * _GENERIC_CALL_ARGS - 提取函数参数部分
 */
#define _GENERIC_CALL_ARGS(...)        __VA_ARGS__

/**
 * DEFINE_GENERIC_CALL - 简化定义泛型调用的辅助宏
 *
 * 示例：
 * DEFINE_GENERIC_CALL(Deserializer_read,
 *     read_data,
 *     (buffer, size),
 *     MetaBlockReader*: MetaBlockReader_read_data,
 *     FileReader*: FileReader_read_data
 * )
 *
 * 展开为：
 * #define Deserializer_read(ptr, buffer, size) \
 *     _Generic((ptr), \
 *         MetaBlockReader*: MetaBlockReader_read_data, \
 *         FileReader*: FileReader_read_data \
 *     )(ptr, buffer, size)
 */
#define DEFINE_GENERIC_CALL(name, method, args, ...) \
    _DEFINE_GENERIC_CALL_IMPL(name, method, args, __VA_ARGS__)

#define _DEFINE_GENERIC_CALL_IMPL(name, method, args, ...)  /* 此宏用于文档，实际定义需要手动编写 \
                                                             */

/**
 * GENERIC_DISPATCH - 简化的泛型分发宏（推荐使用）
 *
 * 示例：
 * #define Deserializer_read(ptr, buffer, size) \
 *     GENERIC_DISPATCH(ptr, \
 *         MetaBlockReader*: MetaBlockReader_read_data, \
 *         FileReader*: FileReader_read_data \
 *     )(ptr, buffer, size)
 */
#define GENERIC_DISPATCH(ptr, ...)                          _Generic((ptr), __VA_ARGS__)

/**
 * GENERIC_METHOD - 更简洁的泛型方法调用宏
 *
 * 自动添加类型后缀的命名约定
 *
 * 示例：
 * #define Deserializer_read(ptr, ...) \
 *     GENERIC_METHOD(ptr, read_data, __VA_ARGS__, \
 *         MetaBlockReader*, \
 *         FileReader* \
 *     )
 *
 * 注意：要求函数命名遵循 Type_method 约定
 */
#define GENERIC_METHOD(ptr, method_name, args, ...) \
    _Generic((ptr), _GENERIC_METHOD_MAP(method_name, __VA_ARGS__))(ptr, args)

#endif  // INTERFACE_H
