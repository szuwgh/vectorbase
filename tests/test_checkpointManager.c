#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include "storage.h"
#include "catalog.h"

/* ============================================================
 * Test Framework
 * ============================================================ */

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

#define CHECK(cond, pass_msg, fail_msg) \
    do { if (cond) { PASS(pass_msg); } else { FAIL(fail_msg); } } while (0)

static const char* TEST_DB = "/tmp/test_checkpoint_vb.db";
static void cleanup_db(void) { unlink(TEST_DB); }

/* ============================================================
 * Helper: create a fresh database + catalog + checkpoint manager
 * ============================================================ */
typedef struct
{
    SingleFileBlockManager* sfbm;
    Catalog* catalog;
    CheckpointManager* cpm;
    MetaBlockWriter* writer;
} TestEnv;

static void init_writer(MetaBlockWriter* writer, BlockManager* manager)
{
    writer->manager = manager;
    writer->block = manager->vtable->create_block(manager);
    writer->offset = sizeof(block_id_t);
}

static void free_writer(MetaBlockWriter* writer)
{
    if (writer)
    {
        if (writer->block)
        {
            fileBuffer_destroy(writer->block->fb);
            free(writer->block);
        }
        free(writer);
    }
}

static MetaBlockWriter* new_writer(BlockManager* manager)
{
    MetaBlockWriter* w = malloc(sizeof(MetaBlockWriter));
    memset(w, 0, sizeof(MetaBlockWriter));
    init_writer(w, manager);
    return w;
}

static TestEnv create_test_env(void)
{
    TestEnv env = {0};
    cleanup_db();
    env.sfbm = create_new_database(TEST_DB, true);
    env.catalog = catalog_create();

    CreateSchemaInfo main_info = {.schema_name = "main", .if_not_exists = true};
    catalog_create_schema(env.catalog, &main_info);

    env.writer = new_writer((BlockManager*)env.sfbm);

    env.cpm = malloc(sizeof(CheckpointManager));
    env.cpm->block_manager = (BlockManager*)env.sfbm;
    env.cpm->catalog = env.catalog;
    env.cpm->meta_block_writer = env.writer;

    return env;
}

static void reset_writer(TestEnv* env)
{
    free_writer(env->writer);
    env->writer = new_writer((BlockManager*)env->sfbm);
    env->cpm->meta_block_writer = env->writer;
}

static void destroy_test_env(TestEnv* env)
{
    if (env->cpm) free(env->cpm);
    free_writer(env->writer);
    if (env->catalog) catalog_destroy(env->catalog);
    if (env->sfbm) destory_single_manager(env->sfbm);
    env->cpm = NULL;
    env->writer = NULL;
    env->catalog = NULL;
    env->sfbm = NULL;
    cleanup_db();
}

/* ============================================================
 * Test: basic createpoint (only "main" schema)
 * ============================================================ */
void test_createpoint_single_schema(void)
{
    printf("\n=== Test createpoint_single_schema ===\n");

    TestEnv env = create_test_env();
    checkpointManager_createpoint(env.cpm);

    SingleFileBlockManager* sfbm = env.sfbm;

    CHECK(sfbm->meta_block != (block_id_t)INVALID_BLOCK,
          "meta_block updated after createpoint",
          "meta_block should be set after createpoint");

    if (sfbm->iteration_count > 0)
        PASS("iteration_count incremented (%lu)", (unsigned long)sfbm->iteration_count);
    else
        FAIL("iteration_count should be > 0");

    if (sfbm->max_block > 0)
        PASS("max_block > 0 (%lu)", (unsigned long)sfbm->max_block);
    else
        FAIL("max_block should be > 0 after createpoint");

    destroy_test_env(&env);
}

/* ============================================================
 * Test: createpoint with multiple schemas
 * ============================================================ */
void test_createpoint_multiple_schemas(void)
{
    printf("\n=== Test createpoint_multiple_schemas ===\n");

    TestEnv env = create_test_env();

    CreateSchemaInfo info2 = {.schema_name = "analytics", .if_not_exists = false};
    CreateSchemaInfo info3 = {.schema_name = "staging", .if_not_exists = false};
    catalog_create_schema(env.catalog, &info2);
    catalog_create_schema(env.catalog, &info3);

    SchemaCatalogEntry* s1 = catalog_get_schema(env.catalog, "main");
    SchemaCatalogEntry* s2 = catalog_get_schema(env.catalog, "analytics");
    SchemaCatalogEntry* s3 = catalog_get_schema(env.catalog, "staging");
    CHECK(s1 && s2 && s3,
          "All 3 schemas exist before checkpoint",
          "Schemas should exist before checkpoint");

    checkpointManager_createpoint(env.cpm);

    CHECK(env.sfbm->meta_block != (block_id_t)INVALID_BLOCK,
          "meta_block updated after multi-schema createpoint",
          "meta_block should be set");

    PASS("createpoint with 3 schemas completed without crash");
    destroy_test_env(&env);
}

/* ============================================================
 * Test: loadfromstorage on empty database (no checkpoint done)
 * ============================================================ */
void test_loadfromstorage_empty(void)
{
    printf("\n=== Test loadfromstorage_empty ===\n");

    TestEnv env = create_test_env();

    CHECK(env.sfbm->meta_block == (block_id_t)INVALID_BLOCK,
          "Fresh database has meta_block=INVALID_BLOCK",
          "Fresh database should have INVALID_BLOCK");

    Catalog* empty_catalog = catalog_create();
    CheckpointManager load_cpm = {
        .block_manager = (BlockManager*)env.sfbm,
        .catalog = empty_catalog,
        .meta_block_writer = NULL
    };
    checkpointManager_loadfromstorage(&load_cpm);
    PASS("loadfromstorage on empty database does not crash");

    SchemaCatalogEntry* s = catalog_get_schema(empty_catalog, "main");
    CHECK(s == NULL,
          "No schemas in empty catalog after load",
          "Should have no schemas loaded from empty database");

    catalog_destroy(empty_catalog);
    destroy_test_env(&env);
}

/* ============================================================
 * Test: double checkpoint
 * ============================================================ */
void test_double_checkpoint(void)
{
    printf("\n=== Test double_checkpoint ===\n");

    TestEnv env = create_test_env();

    checkpointManager_createpoint(env.cpm);
    u64 iter1 = env.sfbm->iteration_count;
    PASS("1st checkpoint: iteration=%lu", (unsigned long)iter1);

    CreateSchemaInfo info = {.schema_name = "second_pass", .if_not_exists = false};
    catalog_create_schema(env.catalog, &info);

    reset_writer(&env);
    checkpointManager_createpoint(env.cpm);
    u64 iter2 = env.sfbm->iteration_count;
    PASS("2nd checkpoint: iteration=%lu", (unsigned long)iter2);

    if (iter2 > iter1)
        PASS("Iteration increased (%lu > %lu)", (unsigned long)iter2, (unsigned long)iter1);
    else
        FAIL("Iteration should increase after second checkpoint");

    PASS("Double checkpoint completed without crash");
    destroy_test_env(&env);
}

/* ============================================================
 * Test: free list management across checkpoints
 * ============================================================ */
void test_free_list_management(void)
{
    printf("\n=== Test free_list_management ===\n");

    TestEnv env = create_test_env();

    checkpointManager_createpoint(env.cpm);
    usize used_after_1st = env.sfbm->used_blocks.size;

    if (used_after_1st == 0)
        PASS("used_blocks empty after 1st checkpoint (size=%zu)", used_after_1st);
    else
        FAIL("used_blocks should be reset after write_header (size=%zu)", used_after_1st);

    PASS("After 1st: free_list=%zu, used_blocks=%zu",
         env.sfbm->free_list.size, env.sfbm->used_blocks.size);

    reset_writer(&env);

    // Load from storage — reads blocks, adding to used_blocks
    Catalog* tmp = catalog_create();
    CheckpointManager load_cpm = {
        .block_manager = (BlockManager*)env.sfbm,
        .catalog = tmp,
        .meta_block_writer = NULL
    };
    checkpointManager_loadfromstorage(&load_cpm);
    catalog_destroy(tmp);

    usize used_after_load = env.sfbm->used_blocks.size;
    PASS("After load: used_blocks=%zu (blocks read during load)", used_after_load);

    checkpointManager_createpoint(env.cpm);
    usize free_after_2nd = env.sfbm->free_list.size;

    if (free_after_2nd >= used_after_load)
        PASS("free_list grew after 2nd checkpoint (%zu >= %zu)", free_after_2nd, used_after_load);
    else
        FAIL("free_list should contain old used_blocks (%zu < %zu)", free_after_2nd, used_after_load);

    CHECK(env.sfbm->used_blocks.size == 0,
          "used_blocks reset after 2nd checkpoint",
          "used_blocks should be empty after checkpoint");

    destroy_test_env(&env);
}

/* ============================================================
 * Test: header alternation (H1/H2 double-buffering)
 * ============================================================ */
void test_header_alternation(void)
{
    printf("\n=== Test header_alternation ===\n");

    TestEnv env = create_test_env();

    CHECK(env.sfbm->active_header == 1,
          "New database: active_header=1",
          "New database should start with active_header=1");

    checkpointManager_createpoint(env.cpm);
    u8 after_1st = env.sfbm->active_header;
    PASS("After 1st checkpoint: active_header=%u", (unsigned)after_1st);

    reset_writer(&env);
    checkpointManager_createpoint(env.cpm);
    u8 after_2nd = env.sfbm->active_header;
    PASS("After 2nd checkpoint: active_header=%u", (unsigned)after_2nd);

    if (after_1st != after_2nd)
        PASS("Header toggled (%u -> %u)", (unsigned)after_1st, (unsigned)after_2nd);
    else
        FAIL("active_header should alternate between checkpoints");

    destroy_test_env(&env);
}

/* ============================================================
 * Test: MetaBlockWriter lifecycle
 * ============================================================ */
void test_metaBlockWriter_lifecycle(void)
{
    printf("\n=== Test MetaBlockWriter lifecycle ===\n");

    TestEnv env = create_test_env();

    CHECK(env.writer->block != NULL,
          "Writer has allocated block",
          "Writer block should not be NULL");

    if (env.writer->offset == sizeof(block_id_t))
        PASS("Writer offset starts at %zu", sizeof(block_id_t));
    else
        FAIL("Writer offset should start at sizeof(block_id_t)");

    CHECK(env.writer->manager == (BlockManager*)env.sfbm,
          "Writer manager points to block_manager",
          "Writer manager should point to sfbm");

    destroy_test_env(&env);
}

/* ============================================================
 * Test: roundtrip single schema (createpoint + loadfromstorage)
 * ============================================================ */
void test_roundtrip_single_schema(void)
{
    printf("\n=== Test roundtrip single schema ===\n");

    TestEnv env = create_test_env();
    checkpointManager_createpoint(env.cpm);

    Catalog* loaded = catalog_create();
    CheckpointManager load_cpm = {
        .block_manager = (BlockManager*)env.sfbm,
        .catalog = loaded,
        .meta_block_writer = NULL
    };
    checkpointManager_loadfromstorage(&load_cpm);

    SchemaCatalogEntry* s = catalog_get_schema(loaded, "main");
    if (s)
    {
        PASS("Roundtrip: 'main' schema restored");
        CHECK(s->base.type == SCHEMA,
              "Restored schema type is SCHEMA",
              "Schema type should be SCHEMA");
    }
    else
    {
        FAIL("Roundtrip: 'main' schema NOT found");
    }

    catalog_destroy(loaded);
    destroy_test_env(&env);
}

/* ============================================================
 * Test: roundtrip with multiple schemas
 * ============================================================ */
void test_roundtrip_multi_schema(void)
{
    printf("\n=== Test roundtrip multi-schema ===\n");

    TestEnv env = create_test_env();

    CreateSchemaInfo info1 = {.schema_name = "alpha", .if_not_exists = false};
    CreateSchemaInfo info2 = {.schema_name = "beta", .if_not_exists = false};
    catalog_create_schema(env.catalog, &info1);
    catalog_create_schema(env.catalog, &info2);

    checkpointManager_createpoint(env.cpm);

    CHECK(env.sfbm->meta_block != (block_id_t)INVALID_BLOCK,
          "meta_block valid after checkpoint",
          "meta_block should be valid");

    Catalog* loaded = catalog_create();
    CheckpointManager load_cpm = {
        .block_manager = (BlockManager*)env.sfbm,
        .catalog = loaded,
        .meta_block_writer = NULL
    };
    checkpointManager_loadfromstorage(&load_cpm);

    SchemaCatalogEntry* sm = catalog_get_schema(loaded, "main");
    SchemaCatalogEntry* sa = catalog_get_schema(loaded, "alpha");
    SchemaCatalogEntry* sb = catalog_get_schema(loaded, "beta");

    CHECK(sm != NULL, "Roundtrip: 'main' restored", "Roundtrip: 'main' NOT found");
    CHECK(sa != NULL, "Roundtrip: 'alpha' restored", "Roundtrip: 'alpha' NOT found");
    CHECK(sb != NULL, "Roundtrip: 'beta' restored", "Roundtrip: 'beta' NOT found");

    catalog_destroy(loaded);
    destroy_test_env(&env);
}

/* ============================================================
 * Test: roundtrip with many schemas
 * ============================================================ */
void test_roundtrip_many_schemas(void)
{
    printf("\n=== Test roundtrip many schemas ===\n");

    TestEnv env = create_test_env();

    char names[10][32];
    for (int i = 0; i < 10; i++)
    {
        snprintf(names[i], sizeof(names[i]), "schema_%d", i);
        CreateSchemaInfo info = {.schema_name = names[i], .if_not_exists = false};
        catalog_create_schema(env.catalog, &info);
    }

    checkpointManager_createpoint(env.cpm);

    Catalog* loaded = catalog_create();
    CheckpointManager load_cpm = {
        .block_manager = (BlockManager*)env.sfbm,
        .catalog = loaded,
        .meta_block_writer = NULL
    };
    checkpointManager_loadfromstorage(&load_cpm);

    // Check "main" + 10 schemas = 11 total
    CHECK(catalog_get_schema(loaded, "main") != NULL,
          "Roundtrip many: 'main' restored",
          "Roundtrip many: 'main' NOT found");

    bool all_ok = true;
    for (int i = 0; i < 10; i++)
    {
        if (!catalog_get_schema(loaded, names[i]))
        {
            FAIL("Roundtrip many: '%s' NOT found", names[i]);
            all_ok = false;
        }
    }
    if (all_ok)
        PASS("Roundtrip many: all 10 extra schemas restored");

    catalog_destroy(loaded);
    destroy_test_env(&env);
}

/* ============================================================
 * Test: checkpoint + load + checkpoint + load (two full cycles)
 * ============================================================ */
void test_two_full_cycles(void)
{
    printf("\n=== Test two full checkpoint/load cycles ===\n");

    TestEnv env = create_test_env();

    // Cycle 1: checkpoint with "main"
    checkpointManager_createpoint(env.cpm);
    block_id_t meta1 = env.sfbm->meta_block;
    PASS("Cycle 1 checkpoint: meta_block=%lu", (unsigned long)meta1);

    // Load cycle 1
    Catalog* loaded1 = catalog_create();
    CheckpointManager load1 = {
        .block_manager = (BlockManager*)env.sfbm,
        .catalog = loaded1,
        .meta_block_writer = NULL
    };
    checkpointManager_loadfromstorage(&load1);
    CHECK(catalog_get_schema(loaded1, "main") != NULL,
          "Cycle 1 load: 'main' restored",
          "Cycle 1 load: 'main' NOT found");
    catalog_destroy(loaded1);

    // Cycle 2: add schema, checkpoint again
    CreateSchemaInfo info = {.schema_name = "cycle2_db", .if_not_exists = false};
    catalog_create_schema(env.catalog, &info);

    reset_writer(&env);
    checkpointManager_createpoint(env.cpm);
    block_id_t meta2 = env.sfbm->meta_block;
    PASS("Cycle 2 checkpoint: meta_block=%lu", (unsigned long)meta2);

    // Load cycle 2
    Catalog* loaded2 = catalog_create();
    CheckpointManager load2 = {
        .block_manager = (BlockManager*)env.sfbm,
        .catalog = loaded2,
        .meta_block_writer = NULL
    };
    checkpointManager_loadfromstorage(&load2);
    CHECK(catalog_get_schema(loaded2, "main") != NULL,
          "Cycle 2 load: 'main' restored",
          "Cycle 2 load: 'main' NOT found");
    CHECK(catalog_get_schema(loaded2, "cycle2_db") != NULL,
          "Cycle 2 load: 'cycle2_db' restored",
          "Cycle 2 load: 'cycle2_db' NOT found");
    catalog_destroy(loaded2);

    destroy_test_env(&env);
}

/* ============================================================
 * Test: reopen database from disk (create_new=false)
 * Verifies Bug #6 and #7 are fixed
 * ============================================================ */
void test_reopen_database(void)
{
    printf("\n=== Test reopen database from disk ===\n");

    cleanup_db();

    // Phase 1: create, checkpoint, close
    {
        SingleFileBlockManager* sfbm = create_new_database(TEST_DB, true);
        CHECK(sfbm != NULL, "Phase 1: created new database", "Failed to create database");

        Catalog* cat = catalog_create();
        CreateSchemaInfo info1 = {.schema_name = "main", .if_not_exists = true};
        CreateSchemaInfo info2 = {.schema_name = "persist_me", .if_not_exists = false};
        catalog_create_schema(cat, &info1);
        catalog_create_schema(cat, &info2);

        MetaBlockWriter* w = new_writer((BlockManager*)sfbm);
        CheckpointManager cpm = {
            .block_manager = (BlockManager*)sfbm,
            .catalog = cat,
            .meta_block_writer = w
        };
        checkpointManager_createpoint(&cpm);
        PASS("Phase 1: checkpoint with [main, persist_me]");

        free_writer(w);
        catalog_destroy(cat);
        destory_single_manager(sfbm);
    }

    // Phase 2: reopen with create_new=false
    {
        SingleFileBlockManager* sfbm = create_new_database(TEST_DB, false);
        if (!sfbm)
        {
            FAIL("Phase 2: create_new_database(false) returned NULL");
            cleanup_db();
            return;
        }
        PASS("Phase 2: reopened existing database");

        CHECK(sfbm->meta_block != (block_id_t)INVALID_BLOCK,
              "Phase 2: meta_block loaded from disk",
              "Phase 2: meta_block should be set from disk header");

        if (sfbm->iteration_count > 0)
            PASS("Phase 2: iteration_count=%lu", (unsigned long)sfbm->iteration_count);
        else
            FAIL("Phase 2: iteration_count should be > 0");

        // Load schemas from disk
        Catalog* cat = catalog_create();
        CheckpointManager load = {
            .block_manager = (BlockManager*)sfbm,
            .catalog = cat,
            .meta_block_writer = NULL
        };
        checkpointManager_loadfromstorage(&load);

        SchemaCatalogEntry* sm = catalog_get_schema(cat, "main");
        SchemaCatalogEntry* sp = catalog_get_schema(cat, "persist_me");
        CHECK(sm != NULL, "Phase 2: 'main' schema loaded from disk",
              "Phase 2: 'main' NOT found from disk");
        CHECK(sp != NULL, "Phase 2: 'persist_me' schema loaded from disk",
              "Phase 2: 'persist_me' NOT found from disk");

        catalog_destroy(cat);
        destory_single_manager(sfbm);
    }

    cleanup_db();
}

/* ============================================================
 * main
 * ============================================================ */
int main(void)
{
    printf("========================================\n");
    printf("   CheckpointManager Test Suite\n");
    printf("========================================\n");

    // Functional tests
    test_createpoint_single_schema();
    test_createpoint_multiple_schemas();
    test_loadfromstorage_empty();
    test_double_checkpoint();
    test_free_list_management();
    test_header_alternation();
    test_metaBlockWriter_lifecycle();

    // Roundtrip tests
    test_roundtrip_single_schema();
    test_roundtrip_multi_schema();
    test_roundtrip_many_schemas();
    test_two_full_cycles();

    // Disk persistence test
    test_reopen_database();

    printf("\n========================================\n");
    printf("   Results: %d passed, %d failed\n", pass_count, fail_count);
    printf("========================================\n\n");

    return fail_count > 0 ? 1 : 0;
}
