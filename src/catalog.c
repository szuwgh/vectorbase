#include <stdlib.h>
#include <string.h>
#include "catalog.h"
#include "hash.h"

void CatalogSet_init(CatalogSet* set)
{
    hmap_init_str(&set->data);
}

void CatalogSet_deinit(CatalogSet* set)
{
    hmap_deinit(&set->data);
}

static void CatalogEntry_init(CatalogEntry* entry, CatalogType type, char* name)
{
    entry->type = type;
    entry->name = name;
    entry->deleted = false;
    entry->parent = NULL;
    entry->child = NULL;
}

static CatalogEntry* make_entry(CatalogType type, char* name)
{
    CatalogEntry* entry = malloc(sizeof(CatalogEntry));
    if (!entry) return NULL;
    CatalogEntry_init(entry, type, name);
    return entry;
}

static SchemaCatalogEntry* make_schema_entry(char* name)
{
    SchemaCatalogEntry* entry = malloc(sizeof(SchemaCatalogEntry));
    if (!entry) return NULL;
    CatalogEntry_init(&entry->base, SCHEMA, name);
    CatalogSet_init(&entry->tables);
    CatalogSet_init(&entry->indexes);
    return entry;
}

static void SchemaCatalogEntry_destroy(SchemaCatalogEntry* entry)
{
    CatalogSet_deinit(&entry->tables);
    CatalogSet_deinit(&entry->indexes);
    free(entry->base.name);
    free(entry);
}

/*
  ┌─────────────────┬────────────────────────────────────────────────────────────────────┐
  │ 版本链           │ 多个事务同时看到同一个对象的不同状态（MVCC 隔离）                       │
  ├─────────────────┼────────────────────────────────────────────────────────────────────┤
  │ dummy 节点      │ 给版本链一个"不存在"的终止状态，让先于 CREATE 的事务有正确的返回值       │
  ├─────────────────┼────────────────────────────────────────────────────────────────────┤
  │ parent 反向指针  │ 回滚时能从旧节点找到新节点并摘除，不需要遍历整条链                       │
  └─────────────────┴────────────────────────────────────────────────────────────────────┘

*/
bool CatalogSet_create_entry(CatalogSet* set, const char* name, CatalogEntry* value)
{
    /* hmap_get 返回 hmap_node* (即存储的 CatalogEntry*), 不存在则返回 NULL */
    hmap_node* node = hmap_get(&set->data, name);
    if (!node)
    {
        /* ---- 从未存在过 ---- */
        /* 创建 dummy 节点: type=INVALID 表示"已删除/不存在" */
        char* name_copy = strdup(name);
        if (!name_copy) return false;
        CatalogEntry* dummy = make_entry(INVALID, name_copy);
        if (!dummy)
        {
            free(name_copy);
            return false;
        }
        /* 插入 hmap，key 与 dummy->name 共用同一份 strdup 拷贝 */
        node = hmap_insert(&set->data, dummy->name, dummy);
        if (!node)
        {
            free(dummy->name);
            free(dummy);
            return false;
        }
    }
    else
    {
        CatalogEntry* current = node->value;
        if (!current->deleted)
        {
            /* 未被删除 = 已存在，创建失败 */
            return false;
        }
    }
    /* ---- 将 value 插入版本链头 ---- */
    /*  value->child 指向旧链头 */
    value->child = node->value;
    /* 建立反向链接 */
    value->child->parent = value;
    /* 更新 hmap，value 成为新链头
     * hmap_insert 对已存在的 key 会原地更新 value */
    node->value = value;
    return true;
}

CatalogEntry* CatalogSet_get_entry(CatalogSet* set, const char* name)
{
    hmap_node* node = hmap_get(&set->data, name);
    if (!node) return NULL;
    if (((CatalogEntry*)node->value)->deleted) return NULL;
    return node->value;
}

static void CatalogSet_drop_entry_impl(CatalogSet* set, hmap_node* node)
{
    // 复制一份entry->name，因为hmap_insert会修改entry->name
    CatalogEntry* entry = node->value;
    CatalogEntry* value = make_entry(INVALID, strdup(entry->name));
    if (!value) return;
    // 插入 dummy 节点，覆盖旧链头
    value->child = entry;// move(data[current.name]);
    value->child->parent = value;
    value->deleted = true;
    // 更新 hmap，value 成为新链头
    node->value = value;
}

bool CatalogSet_drop_entry(CatalogSet* set, const char* name)
{
    hmap_node* node = hmap_get(&set->data, name);
    if (!node) return false;
    CatalogSet_drop_entry_impl(set, node);
    return true;
}

int Catalog_create_schema(Catalog* catalog, CreateSchemaInfo* info)
{
    char* name_copy = strdup(info->schema_name);
    if (!name_copy) return -1;
    SchemaCatalogEntry* entry = make_schema_entry(name_copy);
    if (!entry)
    {
        free(name_copy);
        return -1;
    }
    if (!CatalogSet_create_entry(&catalog->schemas, info->schema_name, (CatalogEntry*)entry))
    {
        SchemaCatalogEntry_destroy(entry);
        if (!info->if_not_exists) return -2; // 已存在
    }
    return 0;
}

SchemaCatalogEntry* Catalog_get_schema(Catalog* catalog, const char* schema_name)
{
    return (SchemaCatalogEntry*)CatalogSet_get_entry(&catalog->schemas, schema_name);
}

int Catalog_drop_schema(Catalog* catalog, const char* schema_name)
{   // 不能删除默认 schema
    if (!strcmp(schema_name, DEFAULT_SCHEMA)) return -1;
    CatalogSet_drop_entry(&catalog->schemas, schema_name);
    return 0;
}

Catalog* Catalog_create()
{
    Catalog* catalog = malloc(sizeof(Catalog));
    if (!catalog) return NULL;
    CatalogSet_init(&catalog->schemas);
    return catalog;
}

void Catalog_destroy(Catalog* catalog)
{
    if (!catalog) return;
    CatalogSet_deinit(&catalog->schemas);
    free(catalog);
}