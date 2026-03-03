#include "parser.h"
#include "storage.h"
#include <stdlib.h>

TypeID get_internal_type(SQLType type)
{
    switch (type)
    {
        case SQLT_TINYINT:
            return TYPE_INT8;
        case SQLT_SMALLINT:
            return TYPE_INT16;
        case SQLT_INTEGER:
            return TYPE_INT32;
        case SQLT_BIGINT:
            return TYPE_INT64;
        case SQLT_FLOAT:
            return TYPE_FLOAT32;
        case SQLT_DOUBLE:
            return TYPE_FLOAT64;
        default:
            return TYPE_INVALID;
    }
}

void createSchemaInfo_deserialize(CreateSchemaInfo* info, MetaBlockReader* reader)
{
    info->schema_name = DESERIALIZER_READ_STRING(reader);
    info->if_not_exists = false;
    // table_count 和 index_count 由 checkpointManager_read_schema 负责读取
}

void createTableInfo_deserialize(CreateTableInfo* info, MetaBlockReader* reader)
{
    info->schema_name = DESERIALIZER_READ_STRING(reader);
    info->table_name = DESERIALIZER_READ_STRING(reader);
    info->if_not_exists = true;
    info->col_count = DESERIALIZER_READ_U32(reader);
    info->columns = malloc(info->col_count * sizeof(ColumnDefinition));
    for (usize i = 0; i < info->col_count; ++i)
    {
        ColumnDefinition* col = &info->columns[i];
        col->name = DESERIALIZER_READ_STRING(reader);
        col->oid = i;
        col->type = (SQLType)DESERIALIZER_READ_U8(reader);
    }
}
