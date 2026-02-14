#include "parser.h"
#include "storage.h"
#include <stdlib.h>

void createSchemaInfo_deserialize(CreateSchemaInfo* info, MetaBlockReader* reader)
{
    info->schema_name = DESERIALIZER_READ_STRING(reader);
    info->if_not_exists = false;
}