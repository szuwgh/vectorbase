#include "parser.h"
#include "storage.h"
#include <stdlib.h>

void createSchemaInfo_deserialize(CreateSchemaInfo* info, MetaBlockReader* reader)
{
    info->schema_name = DESERIALIZER_READ_STRING(reader);
    info->if_not_exists = false;
    // 读取 table_count 和 index_count 占位符（与 write_schema 对应）
    DESERIALIZER_READ_U32(reader);  // table_count
    DESERIALIZER_READ_U32(reader);  // index_count
}