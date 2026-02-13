#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "../src/catalog.h"

static int pass_count = 0;
static int fail_count = 0;

#define PASS(msg, ...)                             \
    do {                                           \
        pass_count++;                              \
        printf("[PASS] " msg "\n", ##__VA_ARGS__); \
    } while (0)

#define FAIL(msg, ...)                             \
    do {                                           \
        fail_count++;                              \
        printf("[FAIL] " msg "\n", ##__VA_ARGS__); \
    } while (0)

// Helper: create a CatalogEntry on the heap (make_entry is static in catalog.c)
static CatalogEntry* test_make_entry(CatalogType type, const char* name)
{
    CatalogEntry* entry = malloc(sizeof(CatalogEntry));
    if (!entry) return NULL;
    entry->type = type;
    entry->name = strdup(name);
    entry->deleted = false;
    entry->parent = NULL;
    entry->child = NULL;
    return entry;
}

// ========== Test: CatalogSet init / deinit ==========
void test_catalogset_init_deinit(void)
{
    printf("\n=== Test CatalogSet init/deinit ===\n");

    CatalogSet set;
    catalogSet_init(&set);

    // catalogSet_init uses hmap_init_str internally
    if (set.data.nbuckets == HMAP_DEFAULT_NBUCKETS && set.data.len == 0)
        PASS("catalogSet_init: hmap initialized (buckets=%zu, len=0)", set.data.nbuckets);
    else
        FAIL("catalogSet_init: unexpected state");

    catalogSet_deinit(&set);

    if (set.data.buckets == NULL && set.data.nbuckets == 0 && set.data.len == 0)
        PASS("catalogSet_deinit: hmap cleaned up");
    else
        FAIL("catalogSet_deinit: unexpected state after deinit");
}

// ========== Test: CatalogSet create_entry basic ==========
void test_catalogset_create_entry(void)
{
    printf("\n=== Test CatalogSet create_entry ===\n");

    CatalogSet set;
    catalogSet_init(&set);

    CatalogEntry* e1 = test_make_entry(TABLE, "users");
    CatalogEntry* e2 = test_make_entry(TABLE, "orders");

    bool ok1 = catalogSet_create_entry(&set, "users", e1);
    bool ok2 = catalogSet_create_entry(&set, "orders", e2);

    if (ok1 && ok2)
        PASS("Created 2 entries successfully");
    else
        FAIL("Failed to create entries (ok1=%d, ok2=%d)", ok1, ok2);

    // Verify they're retrievable
    CatalogEntry* got1 = catalogSet_get_entry(&set, "users");
    CatalogEntry* got2 = catalogSet_get_entry(&set, "orders");

    if (got1 == e1 && got2 == e2)
        PASS("Get returns correct entry pointers");
    else
        FAIL("Get returned wrong pointers");

    if (got1 && got1->type == TABLE && strcmp(got1->name, "users") == 0)
        PASS("Entry 'users' has correct type and name");
    else
        FAIL("Entry 'users' fields incorrect");

    catalogSet_deinit(&set);
}

// ========== Test: CatalogSet duplicate entry ==========
void test_catalogset_duplicate_entry(void)
{
    printf("\n=== Test CatalogSet duplicate entry ===\n");

    CatalogSet set;
    catalogSet_init(&set);

    CatalogEntry* e1 = test_make_entry(TABLE, "users");
    CatalogEntry* e2 = test_make_entry(TABLE, "users");

    bool ok1 = catalogSet_create_entry(&set, "users", e1);
    bool ok2 = catalogSet_create_entry(&set, "users", e2);

    if (ok1 && !ok2)
        PASS("Duplicate entry creation correctly rejected");
    else
        FAIL("Duplicate handling incorrect (ok1=%d, ok2=%d)", ok1, ok2);

    // Original still accessible
    CatalogEntry* got = catalogSet_get_entry(&set, "users");
    if (got == e1)
        PASS("Original entry still accessible after duplicate rejected");
    else
        FAIL("Original entry corrupted");

    free(e2->name);
    free(e2);
    catalogSet_deinit(&set);
}

// ========== Test: CatalogSet get_entry ==========
void test_catalogset_get_entry(void)
{
    printf("\n=== Test CatalogSet get_entry ===\n");

    CatalogSet set;
    catalogSet_init(&set);

    // Get from empty set
    CatalogEntry* got = catalogSet_get_entry(&set, "nonexistent");
    if (got == NULL)
        PASS("Get from empty set returns NULL");
    else
        FAIL("Get from empty set should return NULL");

    // Insert then get
    CatalogEntry* e1 = test_make_entry(TABLE, "products");
    catalogSet_create_entry(&set, "products", e1);

    got = catalogSet_get_entry(&set, "products");
    if (got == e1)
        PASS("Get existing entry returns correct pointer");
    else
        FAIL("Get existing entry failed");

    // Get non-existent (non-empty set)
    got = catalogSet_get_entry(&set, "missing");
    if (got == NULL)
        PASS("Get non-existent entry returns NULL");
    else
        FAIL("Get non-existent should return NULL");

    catalogSet_deinit(&set);
}

// ========== Test: CatalogSet drop_entry ==========
void test_catalogset_drop_entry(void)
{
    printf("\n=== Test CatalogSet drop_entry ===\n");

    CatalogSet set;
    catalogSet_init(&set);

    CatalogEntry* e1 = test_make_entry(TABLE, "temp_table");
    catalogSet_create_entry(&set, "temp_table", e1);

    // Verify exists before drop
    CatalogEntry* before = catalogSet_get_entry(&set, "temp_table");
    if (before == e1)
        PASS("Entry exists before drop");
    else
        FAIL("Entry should exist before drop");

    // Drop
    bool dropped = catalogSet_drop_entry(&set, "temp_table");
    if (dropped)
        PASS("catalogSet_drop_entry returned true");
    else
        FAIL("catalogSet_drop_entry should return true");

    // Verify not accessible after drop
    CatalogEntry* after = catalogSet_get_entry(&set, "temp_table");
    if (after == NULL)
        PASS("Entry not accessible after drop (logical delete)");
    else
        FAIL("Entry should not be accessible after drop");

    catalogSet_deinit(&set);
}

// ========== Test: CatalogSet drop non-existent ==========
void test_catalogset_drop_nonexistent(void)
{
    printf("\n=== Test CatalogSet drop nonexistent ===\n");

    CatalogSet set;
    catalogSet_init(&set);

    // Drop from empty set
    bool result = catalogSet_drop_entry(&set, "ghost");
    if (!result)
        PASS("Drop non-existent entry returns false");
    else
        FAIL("Drop non-existent should return false");

    catalogSet_deinit(&set);
}

// ========== Test: CatalogSet create after drop (version chain) ==========
void test_catalogset_create_after_drop(void)
{
    printf("\n=== Test CatalogSet create after drop ===\n");

    CatalogSet set;
    catalogSet_init(&set);

    // Create -> drop -> re-create
    CatalogEntry* e1 = test_make_entry(TABLE, "recycled");
    catalogSet_create_entry(&set, "recycled", e1);
    catalogSet_drop_entry(&set, "recycled");

    // Should be droppable now, re-create
    CatalogEntry* e2 = test_make_entry(TABLE, "recycled");
    bool ok = catalogSet_create_entry(&set, "recycled", e2);

    if (ok)
        PASS("Re-create after drop succeeds");
    else
        FAIL("Re-create after drop should succeed");

    CatalogEntry* got = catalogSet_get_entry(&set, "recycled");
    if (got == e2)
        PASS("Re-created entry is the new one");
    else
        FAIL("Re-created entry should be the new pointer");

    if (got && got->type == TABLE)
        PASS("Re-created entry type is TABLE");
    else
        FAIL("Re-created entry type incorrect");

    catalogSet_deinit(&set);
}

// ========== Test: Version chain integrity ==========
void test_catalogset_version_chain(void)
{
    printf("\n=== Test CatalogSet version chain ===\n");

    CatalogSet set;
    catalogSet_init(&set);

    // Create entry
    CatalogEntry* e1 = test_make_entry(TABLE, "versioned");
    catalogSet_create_entry(&set, "versioned", e1);

    // After create: e1->child should be the dummy node
    if (e1->child != NULL && e1->child->type == INVALID)
        PASS("After create: child is dummy (type=INVALID)");
    else
        FAIL("After create: child should be dummy node");

    // parent link: dummy->parent should be e1
    if (e1->child->parent == e1)
        PASS("After create: dummy->parent points back to entry");
    else
        FAIL("After create: dummy->parent incorrect");

    // Drop entry
    catalogSet_drop_entry(&set, "versioned");

    // After drop: hmap now points to a new dummy (deleted=true)
    // The chain: new_dummy(deleted) -> e1 -> old_dummy
    // e1->parent should be the new drop dummy
    if (e1->parent != NULL && e1->parent->deleted == true)
        PASS("After drop: entry's parent is drop-dummy (deleted=true)");
    else
        FAIL("After drop: version chain incorrect");

    // Re-create
    CatalogEntry* e2 = test_make_entry(TABLE, "versioned");
    catalogSet_create_entry(&set, "versioned", e2);

    // e2->child should be the drop-dummy
    if (e2->child != NULL && e2->child->deleted == true)
        PASS("After re-create: new entry's child is drop-dummy");
    else
        FAIL("After re-create: version chain incorrect");

    // Full chain: e2 -> drop_dummy(deleted) -> e1 -> old_dummy
    CatalogEntry* drop_dummy = e2->child;
    if (drop_dummy->child == e1 && e1->child != NULL && e1->child->type == INVALID)
        PASS("Full version chain intact: e2 -> drop_dummy -> e1 -> dummy");
    else
        FAIL("Full version chain broken");

    catalogSet_deinit(&set);
}

// ========== Test: Catalog create / destroy ==========
void test_catalog_create_destroy(void)
{
    printf("\n=== Test Catalog create/destroy ===\n");

    Catalog* catalog = catalog_create();
    if (catalog != NULL)
        PASS("catalog_create returns non-NULL");
    else
    {
        FAIL("catalog_create returned NULL");
        return;
    }

    // schemas should be initialized
    if (catalog->schemas.data.nbuckets == HMAP_DEFAULT_NBUCKETS &&
        catalog->schemas.data.len == 0)
        PASS("Catalog schemas CatalogSet initialized");
    else
        FAIL("Catalog schemas not properly initialized");

    catalog_destroy(catalog);
    PASS("catalog_destroy completed");

    // NULL safety
    catalog_destroy(NULL);
    PASS("catalog_destroy(NULL) is safe");
}

// ========== Test: Catalog create_schema ==========
void test_catalog_create_schema(void)
{
    printf("\n=== Test Catalog create_schema ===\n");

    Catalog* catalog = catalog_create();
    if (!catalog) { FAIL("catalog_create failed"); return; }

    CreateSchemaInfo info = { .schema_name = "test_db", .if_not_exists = false };
    int ret = catalog_create_schema(catalog, &info);

    if (ret == 0)
        PASS("catalog_create_schema returns 0 on success");
    else
        FAIL("catalog_create_schema returned %d", ret);

    // Verify schema exists
    SchemaCatalogEntry* schema = catalog_get_schema(catalog, "test_db");
    if (schema != NULL)
        PASS("Created schema is retrievable");
    else
        FAIL("Created schema should be retrievable");

    // Verify base fields
    if (schema && schema->base.type == SCHEMA)
        PASS("Schema entry type is SCHEMA");
    else
        FAIL("Schema entry type incorrect");

    if (schema && strcmp(schema->base.name, "test_db") == 0)
        PASS("Schema entry name is 'test_db'");
    else
        FAIL("Schema entry name incorrect");

    if (schema && schema->base.deleted == false)
        PASS("Schema entry is not deleted");
    else
        FAIL("Schema entry should not be deleted");

    catalog_destroy(catalog);
}

// ========== Test: Catalog get_schema ==========
void test_catalog_get_schema(void)
{
    printf("\n=== Test Catalog get_schema ===\n");

    Catalog* catalog = catalog_create();
    if (!catalog) { FAIL("catalog_create failed"); return; }

    // Get from empty catalog
    SchemaCatalogEntry* empty = catalog_get_schema(catalog, "nope");
    if (empty == NULL)
        PASS("Get non-existent schema returns NULL");
    else
        FAIL("Get non-existent schema should return NULL");

    // Create and get
    CreateSchemaInfo info = { .schema_name = "mydb", .if_not_exists = false };
    catalog_create_schema(catalog, &info);

    SchemaCatalogEntry* schema = catalog_get_schema(catalog, "mydb");
    if (schema != NULL)
        PASS("Get existing schema returns non-NULL");
    else
        FAIL("Get existing schema should return non-NULL");

    // Get with wrong name
    SchemaCatalogEntry* wrong = catalog_get_schema(catalog, "other");
    if (wrong == NULL)
        PASS("Get wrong schema name returns NULL");
    else
        FAIL("Get wrong schema name should return NULL");

    catalog_destroy(catalog);
}

// ========== Test: Catalog drop_schema ==========
void test_catalog_drop_schema(void)
{
    printf("\n=== Test Catalog drop_schema ===\n");

    Catalog* catalog = catalog_create();
    if (!catalog) { FAIL("catalog_create failed"); return; }

    CreateSchemaInfo info = { .schema_name = "dropme", .if_not_exists = false };
    catalog_create_schema(catalog, &info);

    // Drop
    int ret = catalog_drop_schema(catalog, "dropme");
    if (ret == 0)
        PASS("catalog_drop_schema returns 0");
    else
        FAIL("catalog_drop_schema returned %d", ret);

    // Verify not accessible
    SchemaCatalogEntry* after = catalog_get_schema(catalog, "dropme");
    if (after == NULL)
        PASS("Dropped schema no longer accessible");
    else
        FAIL("Dropped schema should not be accessible");

    catalog_destroy(catalog);
}

// ========== Test: Catalog cannot drop default schema ==========
void test_catalog_drop_default_schema(void)
{
    printf("\n=== Test Catalog drop default schema ===\n");

    Catalog* catalog = catalog_create();
    if (!catalog) { FAIL("catalog_create failed"); return; }

    // Create "main" schema first
    CreateSchemaInfo info = { .schema_name = DEFAULT_SCHEMA, .if_not_exists = false };
    catalog_create_schema(catalog, &info);

    // Try to drop "main"
    int ret = catalog_drop_schema(catalog, DEFAULT_SCHEMA);
    if (ret == -1)
        PASS("Cannot drop default schema '%s' (returns -1)", DEFAULT_SCHEMA);
    else
        FAIL("Drop default schema should return -1, got %d", ret);

    // "main" should still be accessible
    SchemaCatalogEntry* schema = catalog_get_schema(catalog, DEFAULT_SCHEMA);
    if (schema != NULL)
        PASS("Default schema still accessible after failed drop");
    else
        FAIL("Default schema should still be accessible");

    catalog_destroy(catalog);
}

// ========== Test: if_not_exists flag ==========
void test_catalog_if_not_exists(void)
{
    printf("\n=== Test Catalog if_not_exists ===\n");

    Catalog* catalog = catalog_create();
    if (!catalog) { FAIL("catalog_create failed"); return; }

    // Create schema
    CreateSchemaInfo info1 = { .schema_name = "dup_schema", .if_not_exists = false };
    int ret1 = catalog_create_schema(catalog, &info1);
    if (ret1 == 0)
        PASS("First create succeeds");
    else
        FAIL("First create should succeed");

    // Duplicate without if_not_exists -> error (-2)
    CreateSchemaInfo info2 = { .schema_name = "dup_schema", .if_not_exists = false };
    int ret2 = catalog_create_schema(catalog, &info2);
    if (ret2 == -2)
        PASS("Duplicate without if_not_exists returns -2");
    else
        FAIL("Duplicate without if_not_exists should return -2, got %d", ret2);

    // Duplicate with if_not_exists -> success (0)
    CreateSchemaInfo info3 = { .schema_name = "dup_schema", .if_not_exists = true };
    int ret3 = catalog_create_schema(catalog, &info3);
    if (ret3 == 0)
        PASS("Duplicate with if_not_exists returns 0 (no error)");
    else
        FAIL("Duplicate with if_not_exists should return 0, got %d", ret3);

    catalog_destroy(catalog);
}

// ========== Test: SchemaCatalogEntry fields ==========
void test_schema_entry_fields(void)
{
    printf("\n=== Test SchemaCatalogEntry fields ===\n");

    Catalog* catalog = catalog_create();
    if (!catalog) { FAIL("catalog_create failed"); return; }

    CreateSchemaInfo info = { .schema_name = "fieldtest", .if_not_exists = false };
    catalog_create_schema(catalog, &info);

    SchemaCatalogEntry* schema = catalog_get_schema(catalog, "fieldtest");
    if (!schema) { FAIL("Schema not found"); catalog_destroy(catalog); return; }

    // EXTENDS(CatalogEntry) -> base field
    if (schema->base.type == SCHEMA)
        PASS("SchemaCatalogEntry.base.type == SCHEMA");
    else
        FAIL("base.type should be SCHEMA");

    if (schema->base.deleted == false)
        PASS("SchemaCatalogEntry.base.deleted == false");
    else
        FAIL("base.deleted should be false");

    // tables CatalogSet should be initialized
    if (schema->tables.data.nbuckets == HMAP_DEFAULT_NBUCKETS &&
        schema->tables.data.len == 0)
        PASS("SchemaCatalogEntry.tables initialized (empty)");
    else
        FAIL("tables CatalogSet not initialized");

    // indexes CatalogSet should be initialized
    if (schema->indexes.data.nbuckets == HMAP_DEFAULT_NBUCKETS &&
        schema->indexes.data.len == 0)
        PASS("SchemaCatalogEntry.indexes initialized (empty)");
    else
        FAIL("indexes CatalogSet not initialized");

    catalog_destroy(catalog);
}

// ========== Test: Multiple schemas ==========
void test_catalog_multiple_schemas(void)
{
    printf("\n=== Test Catalog multiple schemas ===\n");

    Catalog* catalog = catalog_create();
    if (!catalog) { FAIL("catalog_create failed"); return; }

    const char* names[] = {"alpha", "beta", "gamma", "delta", "epsilon"};
    int count = 5;

    for (int i = 0; i < count; i++)
    {
        CreateSchemaInfo info = { .schema_name = (char*)names[i], .if_not_exists = false };
        int ret = catalog_create_schema(catalog, &info);
        if (ret != 0)
        {
            FAIL("Failed to create schema '%s'", names[i]);
            catalog_destroy(catalog);
            return;
        }
    }
    PASS("Created %d schemas", count);

    // Verify all accessible
    bool all_ok = true;
    for (int i = 0; i < count; i++)
    {
        SchemaCatalogEntry* s = catalog_get_schema(catalog, names[i]);
        if (!s || s->base.type != SCHEMA || strcmp(s->base.name, names[i]) != 0)
        {
            all_ok = false;
            FAIL("Schema '%s' not found or has incorrect fields", names[i]);
            break;
        }
    }
    if (all_ok)
        PASS("All %d schemas retrievable with correct fields", count);

    // Drop one in the middle
    catalog_drop_schema(catalog, "gamma");
    SchemaCatalogEntry* dropped = catalog_get_schema(catalog, "gamma");
    if (dropped == NULL)
        PASS("Dropped schema 'gamma' not accessible");
    else
        FAIL("Dropped schema should not be accessible");

    // Others still intact
    bool rest_ok = true;
    for (int i = 0; i < count; i++)
    {
        if (strcmp(names[i], "gamma") == 0) continue;
        SchemaCatalogEntry* s = catalog_get_schema(catalog, names[i]);
        if (!s)
        {
            rest_ok = false;
            FAIL("Schema '%s' should still exist", names[i]);
            break;
        }
    }
    if (rest_ok)
        PASS("Remaining schemas intact after drop");

    catalog_destroy(catalog);
}

// ========== Test: Schema drop and re-create ==========
void test_catalog_schema_drop_recreate(void)
{
    printf("\n=== Test Catalog schema drop and re-create ===\n");

    Catalog* catalog = catalog_create();
    if (!catalog) { FAIL("catalog_create failed"); return; }

    // Create
    CreateSchemaInfo info1 = { .schema_name = "temp", .if_not_exists = false };
    catalog_create_schema(catalog, &info1);
    SchemaCatalogEntry* s1 = catalog_get_schema(catalog, "temp");
    if (s1 != NULL)
        PASS("Schema 'temp' created");
    else
    {
        FAIL("Schema creation failed");
        catalog_destroy(catalog);
        return;
    }

    // Drop
    catalog_drop_schema(catalog, "temp");
    SchemaCatalogEntry* dropped = catalog_get_schema(catalog, "temp");
    if (dropped == NULL)
        PASS("Schema 'temp' dropped");
    else
        FAIL("Schema should be dropped");

    // Re-create
    CreateSchemaInfo info2 = { .schema_name = "temp", .if_not_exists = false };
    int ret = catalog_create_schema(catalog, &info2);
    if (ret == 0)
        PASS("Re-create schema 'temp' succeeds");
    else
        FAIL("Re-create should succeed, got %d", ret);

    SchemaCatalogEntry* s2 = catalog_get_schema(catalog, "temp");
    if (s2 != NULL && s2 != s1)
        PASS("Re-created schema is a new entry (different pointer)");
    else if (s2 == s1)
        FAIL("Re-created schema should be a new allocation");
    else
        FAIL("Re-created schema not found");

    catalog_destroy(catalog);
}

// ========== Test: CatalogEntry type enum values ==========
void test_catalog_type_enum(void)
{
    printf("\n=== Test CatalogType enum values ===\n");

    if (INVALID == 0)
        PASS("INVALID == 0");
    else
        FAIL("INVALID should be 0");

    if (TABLE == 1)
        PASS("TABLE == 1");
    else
        FAIL("TABLE should be 1");

    if (SCHEMA == 2)
        PASS("SCHEMA == 2");
    else
        FAIL("SCHEMA should be 2");

    if (INDEX == 3)
        PASS("INDEX == 3");
    else
        FAIL("INDEX should be 3");
}

// ========== Test: CatalogSet with many entries ==========
void test_catalogset_stress(void)
{
    printf("\n=== Test CatalogSet stress ===\n");

    CatalogSet set;
    catalogSet_init(&set);

    const int N = 100;
    CatalogEntry* entries[100];
    char names[100][32];

    for (int i = 0; i < N; i++)
    {
        snprintf(names[i], sizeof(names[i]), "entry_%03d", i);
        entries[i] = test_make_entry(TABLE, names[i]);
        bool ok = catalogSet_create_entry(&set, names[i], entries[i]);
        if (!ok)
        {
            FAIL("Failed to create entry %d", i);
            catalogSet_deinit(&set);
            return;
        }
    }
    PASS("Inserted %d entries", N);

    // Verify all accessible
    bool all_ok = true;
    for (int i = 0; i < N; i++)
    {
        CatalogEntry* got = catalogSet_get_entry(&set, names[i]);
        if (got != entries[i])
        {
            all_ok = false;
            FAIL("Entry '%s' not found or wrong pointer", names[i]);
            break;
        }
    }
    if (all_ok)
        PASS("All %d entries retrievable", N);

    // Drop every other entry
    for (int i = 0; i < N; i += 2)
    {
        catalogSet_drop_entry(&set, names[i]);
    }

    // Verify dropped are gone, remaining are intact
    bool drop_ok = true;
    for (int i = 0; i < N; i++)
    {
        CatalogEntry* got = catalogSet_get_entry(&set, names[i]);
        if (i % 2 == 0)
        {
            if (got != NULL) { drop_ok = false; break; }
        }
        else
        {
            if (got != entries[i]) { drop_ok = false; break; }
        }
    }
    if (drop_ok)
        PASS("Drop pattern correct: 50 dropped, 50 remaining");
    else
        FAIL("Drop pattern incorrect");

    catalogSet_deinit(&set);
}

// ========== Test: Schema tables/indexes CatalogSets usable ==========
void test_schema_nested_catalogsets(void)
{
    printf("\n=== Test Schema nested CatalogSets ===\n");

    Catalog* catalog = catalog_create();
    if (!catalog) { FAIL("catalog_create failed"); return; }

    CreateSchemaInfo info = { .schema_name = "nested_test", .if_not_exists = false };
    catalog_create_schema(catalog, &info);

    SchemaCatalogEntry* schema = catalog_get_schema(catalog, "nested_test");
    if (!schema) { FAIL("Schema not found"); catalog_destroy(catalog); return; }

    // Insert entries into the schema's tables CatalogSet
    CatalogEntry* t1 = test_make_entry(TABLE, "users");
    CatalogEntry* t2 = test_make_entry(TABLE, "orders");
    bool ok1 = catalogSet_create_entry(&schema->tables, "users", t1);
    bool ok2 = catalogSet_create_entry(&schema->tables, "orders", t2);

    if (ok1 && ok2)
        PASS("Inserted 2 table entries into schema.tables");
    else
        FAIL("Failed to insert into schema.tables");

    // Insert entries into the schema's indexes CatalogSet
    CatalogEntry* idx1 = test_make_entry(INDEX, "idx_users_pk");
    bool ok3 = catalogSet_create_entry(&schema->indexes, "idx_users_pk", idx1);

    if (ok3)
        PASS("Inserted 1 index entry into schema.indexes");
    else
        FAIL("Failed to insert into schema.indexes");

    // Retrieve from tables
    CatalogEntry* got_t = catalogSet_get_entry(&schema->tables, "users");
    if (got_t == t1 && got_t->type == TABLE)
        PASS("Retrieved table 'users' from schema.tables");
    else
        FAIL("Table retrieval failed");

    // Retrieve from indexes
    CatalogEntry* got_i = catalogSet_get_entry(&schema->indexes, "idx_users_pk");
    if (got_i == idx1 && got_i->type == INDEX)
        PASS("Retrieved index 'idx_users_pk' from schema.indexes");
    else
        FAIL("Index retrieval failed");

    // Tables and indexes are independent namespaces
    CatalogEntry* cross = catalogSet_get_entry(&schema->tables, "idx_users_pk");
    if (cross == NULL)
        PASS("Index name not found in tables (independent namespaces)");
    else
        FAIL("Tables and indexes should be independent");

    catalog_destroy(catalog);
}

// ========== Test: DEFAULT_SCHEMA constant ==========
void test_default_schema_constant(void)
{
    printf("\n=== Test DEFAULT_SCHEMA constant ===\n");

    if (strcmp(DEFAULT_SCHEMA, "main") == 0)
        PASS("DEFAULT_SCHEMA is \"main\"");
    else
        FAIL("DEFAULT_SCHEMA should be \"main\", got \"%s\"", DEFAULT_SCHEMA);
}

int main(void)
{
    printf("========================================\n");
    printf("   Catalog Test Suite\n");
    printf("========================================\n");

    // CatalogSet tests
    test_catalogset_init_deinit();
    test_catalogset_create_entry();
    test_catalogset_duplicate_entry();
    test_catalogset_get_entry();
    test_catalogset_drop_entry();
    test_catalogset_drop_nonexistent();
    test_catalogset_create_after_drop();
    test_catalogset_version_chain();
    test_catalogset_stress();

    // Catalog tests
    test_catalog_create_destroy();
    test_catalog_create_schema();
    test_catalog_get_schema();
    test_catalog_drop_schema();
    test_catalog_drop_default_schema();
    test_catalog_if_not_exists();
    test_schema_entry_fields();
    test_catalog_multiple_schemas();
    test_catalog_schema_drop_recreate();
    test_catalog_type_enum();
    test_schema_nested_catalogsets();
    test_default_schema_constant();

    printf("\n========================================\n");
    printf("   Results: %d passed, %d failed\n", pass_count, fail_count);
    printf("========================================\n\n");

    return fail_count > 0 ? 1 : 0;
}
