#ifndef VECTORBASE_H
#define VECTORBASE_H
#include "storage.h"
#include "catalog.h"

typedef struct
{
    StorageManager *storage_manager;
    Catalog *catalog;
} VectorBase;

#endif // VECTORBASE_H