#ifndef PARSER_H
#define PARSER_H
#include "vb_type.h"

// 创建模式语句
typedef struct
{
    char* schema_name;
    bool if_not_exists;
} CreateSchemaInfo;

#endif