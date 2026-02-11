#ifndef CATALOG_H
#define CATALOG_H
#include "vb_type.h"

enum CatalogType
{
    INVALID = 0,
    TABLE = 1,
    SCHEMA = 2,
    INDEX = 3,
};

typedef struct
{
} SchemaCatalogEntry;

typedef struct
{
} Catalog;

#endif