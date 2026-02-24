#ifndef PARSER_H
#define PARSER_H
#include "vb_type.h"

// 前向声明，避免 parser.h → storage.h → catalog.h → parser.h 循环依赖
typedef struct MetaBlockReader MetaBlockReader;

typedef enum
{
    SQLT_INT = 1,
    SQLT_FLOAT = 2,
    SQLT_STRING = 3,
} SQLType;

// 创建模式语句
typedef struct
{
    char* schema_name;
    bool if_not_exists;
} CreateSchemaInfo;

typedef struct
{
    char* name;
    SQLType type;
} ColumnDefinition;

void createSchemaInfo_deserialize(CreateSchemaInfo* info, MetaBlockReader* reader);

#endif