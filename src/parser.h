#ifndef PARSER_H
#define PARSER_H
#include "vb_type.h"
#include "types.h"

// 前向声明，避免 parser.h → storage.h → catalog.h → parser.h 循环依赖
typedef struct MetaBlockReader MetaBlockReader;

typedef enum
{
    SQLT_TINYINT = 1,
    SQLT_SMALLINT = 2,
    SQLT_INTEGER = 3,
    SQLT_BIGINT = 4,
    SQLT_FLOAT = 5,
    SQLT_DOUBLE = 6,
    SQLT_VARCHAR = 7,
    SQLT_VECTOR = 8,
} SQLType;

TypeID get_internal_type(SQLType type);
// 创建模式语句
typedef struct
{
    char* schema_name;
    bool if_not_exists;
} CreateSchemaInfo;
void createSchemaInfo_deserialize(CreateSchemaInfo* info, MetaBlockReader* reader);

typedef struct
{
    i16 dim;
} VectorColumnOptions;

typedef struct
{
    char* name;
    Oid oid;
    SQLType type;
    union
    {
        VectorColumnOptions vector;
          /* future: DecimalColumnOptions decimal; */
    } options;
} ColumnDefinition;

typedef struct
{
    char* schema_name;
    char* table_name;
    bool if_not_exists;
    ColumnDefinition* columns;
    usize col_count;
} CreateTableInfo;
void createTableInfo_deserialize(CreateTableInfo* info, MetaBlockReader* reader);
#endif