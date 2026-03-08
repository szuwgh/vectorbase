#ifndef DB_H
#define DB_H
#include "storage.h"
#include "catalog.h"

typedef struct
{
    StorageManager *storage_manager;
    Catalog *catalog;
} DB;

#endif // VECTORBASE_H