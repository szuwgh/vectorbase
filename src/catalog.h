#ifndef CATALOG_H
#define CATALOG_H
#include "interface.h"
#include "vb_type.h"
#include "hash.h"
#include "parser.h"

#define DEFAULT_SCHEMA "main"

typedef enum
{
    INVALID = 0,
    TABLE = 1,
    SCHEMA = 2,
    INDEX = 3,
} CatalogType;

typedef struct CatalogEntry CatalogEntry;

struct CatalogEntry
{
    CatalogType type; // 目录项类型
    char* name;       // 目录项名称
    bool deleted; // 是否已删除（逻辑删除）
    CatalogEntry* parent; // 父目录项
    CatalogEntry* child;  // 子目录项
};

// MVCC 版本化容器，存放 CatalogEntry
typedef struct
{
    hmap data;
} CatalogSet;

void CatalogSet_init(CatalogSet* set);

void CatalogSet_deinit(CatalogSet* set);

bool CatalogSet_create_entry(CatalogSet* set, const char* name, CatalogEntry* entry);

CatalogEntry* CatalogSet_get_entry(CatalogSet* set, const char* name);

bool CatalogSet_drop_entry(CatalogSet* set, const char* name);

// 模式目录项 相当于pg database
typedef struct
{
    EXTENDS(CatalogEntry);
    CatalogSet tables;
    CatalogSet indexes;
} SchemaCatalogEntry;

// 目录项 相当于pg catalog
typedef struct
{
    CatalogSet schemas;
} Catalog;

// 创建目录项
Catalog* Catalog_create();
// 销毁目录项
void Catalog_destroy(Catalog* catalog);

int Catalog_create_schema(Catalog* catalog, CreateSchemaInfo* info);

SchemaCatalogEntry* Catalog_get_schema(Catalog* catalog, const char* schema_name);

int Catalog_drop_schema(Catalog* catalog, const char* schema_name);

#endif