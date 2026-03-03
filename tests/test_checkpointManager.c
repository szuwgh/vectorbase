#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include "storage.h"
#include "catalog.h"
#include "datatable.h"

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
    StorageManager* sm;
    Catalog* catalog;
    CheckpointManager* cpm;
} TestEnv;

static TestEnv create_test_env(void)
{
    TestEnv env = {0};
    cleanup_db();
    env.sfbm = create_new_database(TEST_DB, true);
    env.catalog = catalog_create();

    CreateSchemaInfo main_info = {.schema_name = "main", .if_not_exists = true};
    catalog_create_schema(env.catalog, &main_info);

    env.sm = malloc(sizeof(StorageManager));
    env.sm->block_manager = (BlockManager*)env.sfbm;
    env.sm->wal_manager = NULL;

    env.catalog->storage = env.sm;
    env.cpm = CheckpointManager_create((BlockManager*)env.sfbm, env.catalog);

    return env;
}

static void destroy_test_env(TestEnv* env)
{
    if (env->cpm) free(env->cpm);
    if (env->catalog) catalog_destroy(env->catalog);
    if (env->sfbm) destory_single_manager(env->sfbm);
    if (env->sm) free(env->sm);
    env->cpm = NULL;
    env->catalog = NULL;
    env->sfbm = NULL;
    env->sm = NULL;
    cleanup_db();
}

/* Helper: create a CheckpointManager for loading into a fresh catalog */
static CheckpointManager* make_load_cpm(TestEnv* env, Catalog* cat)
{
    cat->storage = env->sm;
    return CheckpointManager_create((BlockManager*)env->sfbm, cat);
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
    CheckpointManager* load_cpm = make_load_cpm(&env, empty_catalog);
    checkpointManager_loadfromstorage(load_cpm);
    PASS("loadfromstorage on empty database does not crash");

    SchemaCatalogEntry* s = catalog_get_schema(empty_catalog, "main");
    CHECK(s == NULL,
          "No schemas in empty catalog after load",
          "Should have no schemas loaded from empty database");

    free(load_cpm);
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

    // Load from storage — reads blocks, adding to used_blocks
    Catalog* tmp = catalog_create();
    CheckpointManager* load_cpm = make_load_cpm(&env, tmp);
    checkpointManager_loadfromstorage(load_cpm);
    free(load_cpm);
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
 * Test: roundtrip single schema (createpoint + loadfromstorage)
 * ============================================================ */
void test_roundtrip_single_schema(void)
{
    printf("\n=== Test roundtrip single schema ===\n");

    TestEnv env = create_test_env();
    checkpointManager_createpoint(env.cpm);

    Catalog* loaded = catalog_create();
    CheckpointManager* load_cpm = make_load_cpm(&env, loaded);
    checkpointManager_loadfromstorage(load_cpm);

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

    free(load_cpm);
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
    CheckpointManager* load_cpm = make_load_cpm(&env, loaded);
    checkpointManager_loadfromstorage(load_cpm);

    SchemaCatalogEntry* sm = catalog_get_schema(loaded, "main");
    SchemaCatalogEntry* sa = catalog_get_schema(loaded, "alpha");
    SchemaCatalogEntry* sb = catalog_get_schema(loaded, "beta");

    CHECK(sm != NULL, "Roundtrip: 'main' restored", "Roundtrip: 'main' NOT found");
    CHECK(sa != NULL, "Roundtrip: 'alpha' restored", "Roundtrip: 'alpha' NOT found");
    CHECK(sb != NULL, "Roundtrip: 'beta' restored", "Roundtrip: 'beta' NOT found");

    free(load_cpm);
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
    CheckpointManager* load_cpm = make_load_cpm(&env, loaded);
    checkpointManager_loadfromstorage(load_cpm);

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

    free(load_cpm);
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
    CheckpointManager* load1 = make_load_cpm(&env, loaded1);
    checkpointManager_loadfromstorage(load1);
    CHECK(catalog_get_schema(loaded1, "main") != NULL,
          "Cycle 1 load: 'main' restored",
          "Cycle 1 load: 'main' NOT found");
    free(load1);
    catalog_destroy(loaded1);

    // Cycle 2: add schema, checkpoint again
    CreateSchemaInfo info = {.schema_name = "cycle2_db", .if_not_exists = false};
    catalog_create_schema(env.catalog, &info);

    checkpointManager_createpoint(env.cpm);
    block_id_t meta2 = env.sfbm->meta_block;
    PASS("Cycle 2 checkpoint: meta_block=%lu", (unsigned long)meta2);

    // Load cycle 2
    Catalog* loaded2 = catalog_create();
    CheckpointManager* load2 = make_load_cpm(&env, loaded2);
    checkpointManager_loadfromstorage(load2);
    CHECK(catalog_get_schema(loaded2, "main") != NULL,
          "Cycle 2 load: 'main' restored",
          "Cycle 2 load: 'main' NOT found");
    CHECK(catalog_get_schema(loaded2, "cycle2_db") != NULL,
          "Cycle 2 load: 'cycle2_db' restored",
          "Cycle 2 load: 'cycle2_db' NOT found");
    free(load2);
    catalog_destroy(loaded2);

    destroy_test_env(&env);
}

/* ============================================================
 * Test: reopen database from disk (create_new=false)
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
        StorageManager p1_sm = { .block_manager = (BlockManager*)sfbm, .wal_manager = NULL };
        cat->storage = &p1_sm;

        CreateSchemaInfo info1 = {.schema_name = "main", .if_not_exists = true};
        CreateSchemaInfo info2 = {.schema_name = "persist_me", .if_not_exists = false};
        catalog_create_schema(cat, &info1);
        catalog_create_schema(cat, &info2);

        CheckpointManager* cpm = CheckpointManager_create((BlockManager*)sfbm, cat);
        checkpointManager_createpoint(cpm);
        PASS("Phase 1: checkpoint with [main, persist_me]");

        free(cpm);
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
        StorageManager p2_sm = { .block_manager = (BlockManager*)sfbm, .wal_manager = NULL };
        cat->storage = &p2_sm;

        CheckpointManager* load = CheckpointManager_create((BlockManager*)sfbm, cat);
        checkpointManager_loadfromstorage(load);

        SchemaCatalogEntry* sm = catalog_get_schema(cat, "main");
        SchemaCatalogEntry* sp = catalog_get_schema(cat, "persist_me");
        CHECK(sm != NULL, "Phase 2: 'main' schema loaded from disk",
              "Phase 2: 'main' NOT found from disk");
        CHECK(sp != NULL, "Phase 2: 'persist_me' schema loaded from disk",
              "Phase 2: 'persist_me' NOT found from disk");

        free(load);
        catalog_destroy(cat);
        destory_single_manager(sfbm);
    }

    cleanup_db();
}

/* ============================================================
 * Test: Table data roundtrip (checkpoint + load with INT32 data)
 *
 * This tests the full pipeline:
 *   1. Create schema + table with 2 INT32 columns
 *   2. Append 100 rows of data
 *   3. checkpointManager_createpoint (writes to disk)
 *   4. checkpointManager_loadfromstorage (reads from disk into
 *      a fresh catalog)
 *   5. Scan the loaded table and verify all 100 rows match
 * ============================================================ */
void test_table_data_roundtrip(void)
{
    printf("\n=== Test table data roundtrip ===\n");

    TestEnv env = create_test_env();

    /* --- 1. Create table "test_tbl" (col_a INT32, col_b INT32) --- */
    ColumnDefinition cols[2] = {
        {.name = "col_a", .oid = 0, .type = SQLT_INTEGER},
        {.name = "col_b", .oid = 1, .type = SQLT_INTEGER},
    };
    CreateTableInfo tinfo = {
        .schema_name = "main",
        .table_name = "test_tbl",
        .if_not_exists = false,
        .columns = cols,
        .col_count = 2,
    };
    int rc = catalog_create_table(env.catalog, &tinfo);
    CHECK(rc == 0, "Created table 'test_tbl'", "Failed to create table");

    TableCatalogEntry* tbl = catalog_get_table(env.catalog, "main", "test_tbl");
    CHECK(tbl != NULL, "Table entry found in catalog", "Table entry NOT found");
    if (!tbl) { destroy_test_env(&env); return; }

    /* --- 2. Append 100 rows --- */
    #define N_ROWS 100
    Vector types = VEC(TypeID, 2);
    TypeID t = TYPE_INT32;
    vector_push_back(&types, &t);
    vector_push_back(&types, &t);

    DataChunk chunk = MAKE(DataChunk, types);
    i32* col_a = (i32*)chunk.columns[0].data;
    i32* col_b = (i32*)chunk.columns[1].data;
    usize batch_size = MIN(N_ROWS, STANDARD_VECTOR_SIZE);
    for (usize i = 0; i < batch_size; i++)
    {
        col_a[i] = (i32)(i * 10);
        col_b[i] = (i32)(i * 10 + 1);
    }
    chunk.columns[0].count = batch_size;
    chunk.columns[1].count = batch_size;
    datatable_append(tbl->datatable, &chunk);

    if (N_ROWS > STANDARD_VECTOR_SIZE)
    {
        dataChunk_reset(&chunk);
        col_a = (i32*)chunk.columns[0].data;
        col_b = (i32*)chunk.columns[1].data;
        usize remaining = N_ROWS - STANDARD_VECTOR_SIZE;
        for (usize i = 0; i < remaining; i++)
        {
            col_a[i] = (i32)((STANDARD_VECTOR_SIZE + i) * 10);
            col_b[i] = (i32)((STANDARD_VECTOR_SIZE + i) * 10 + 1);
        }
        chunk.columns[0].count = remaining;
        chunk.columns[1].count = remaining;
        datatable_append(tbl->datatable, &chunk);
    }
    dataChunk_deinit(&chunk);
    vector_deinit(&types);
    PASS("Appended %d rows to table", N_ROWS);

    /* --- 3. Checkpoint --- */
    checkpointManager_createpoint(env.cpm);
    PASS("Checkpoint created with table data");

    /* --- 4. Load from storage --- */
    Catalog* loaded = catalog_create();
    CheckpointManager* load_cpm = make_load_cpm(&env, loaded);
    checkpointManager_loadfromstorage(load_cpm);

    SchemaCatalogEntry* ls = catalog_get_schema(loaded, "main");
    CHECK(ls != NULL, "Loaded schema 'main'", "Schema 'main' NOT loaded");

    TableCatalogEntry* lt = catalog_get_table(loaded, "main", "test_tbl");
    CHECK(lt != NULL, "Loaded table 'test_tbl'", "Table 'test_tbl' NOT loaded");
    if (!lt) { free(load_cpm); catalog_destroy(loaded); destroy_test_env(&env); return; }

    CHECK(lt->column_count == 2,
          "Loaded table has 2 columns",
          "Loaded table column_count mismatch");
    CHECK(lt->datatable != NULL,
          "Loaded table has DataTable",
          "Loaded table DataTable is NULL");

    /* --- 5. Scan and verify --- */
    Vector types2 = VEC(TypeID, 2);
    vector_push_back(&types2, &t);
    vector_push_back(&types2, &t);
    DataChunk output = MAKE(DataChunk, types2);
    ScanState state;
    datatable_init_scan(lt->datatable, &state);

    usize col_ids[2] = {0, 1};
    usize total_rows = 0;
    bool data_ok = true;

    while (true)
    {
        dataChunk_reset(&output);
        bool has_data = datatable_scan(lt->datatable, &state, &output, col_ids, 2);
        if (!has_data || dataChunk_size(&output) == 0) break;

        i32* ra = (i32*)output.columns[0].data;
        i32* rb = (i32*)output.columns[1].data;
        for (usize i = 0; i < dataChunk_size(&output); i++)
        {
            i32 expected_a = (i32)((total_rows + i) * 10);
            i32 expected_b = (i32)((total_rows + i) * 10 + 1);
            if (ra[i] != expected_a || rb[i] != expected_b)
            {
                FAIL("Row %zu: col_a=%d (expected %d), col_b=%d (expected %d)",
                     total_rows + i, ra[i], expected_a, rb[i], expected_b);
                data_ok = false;
            }
        }
        total_rows += dataChunk_size(&output);
    }

    if (total_rows == N_ROWS)
        PASS("Scan returned correct row count (%d)", N_ROWS);
    else
        FAIL("Scan row count mismatch: got %zu, expected %d", total_rows, N_ROWS);
    if (data_ok && total_rows == N_ROWS)
        PASS("All %d rows verified correct after roundtrip", N_ROWS);

    scanstate_deinit(&state);
    dataChunk_deinit(&output);
    vector_deinit(&types2);
    free(load_cpm);
    catalog_destroy(loaded);
    destroy_test_env(&env);
    #undef N_ROWS
}

/* ============================================================
 * Test: Table data roundtrip with large dataset (cross-segment)
 *
 * Writes 500 rows to test RowSegment boundary crossing
 * (STORAGE_CHUNK_SIZE = 250)
 * ============================================================ */
void test_table_data_roundtrip_large(void)
{
    printf("\n=== Test table data roundtrip large (500 rows) ===\n");

    TestEnv env = create_test_env();

    ColumnDefinition cols[2] = {
        {.name = "id",    .oid = 0, .type = SQLT_INTEGER},
        {.name = "value", .oid = 1, .type = SQLT_DOUBLE},
    };
    CreateTableInfo tinfo = {
        .schema_name = "main",
        .table_name = "big_tbl",
        .if_not_exists = false,
        .columns = cols,
        .col_count = 2,
    };
    catalog_create_table(env.catalog, &tinfo);
    TableCatalogEntry* tbl = catalog_get_table(env.catalog, "main", "big_tbl");
    CHECK(tbl != NULL, "Created big_tbl", "Failed to create big_tbl");
    if (!tbl) { destroy_test_env(&env); return; }

    #define TOTAL_ROWS 500
    Vector types = VEC(TypeID, 2);
    TypeID ti32 = TYPE_INT32;
    TypeID tf64 = TYPE_FLOAT64;
    vector_push_back(&types, &ti32);
    vector_push_back(&types, &tf64);

    DataChunk chunk = MAKE(DataChunk, types);
    usize rows_written = 0;
    while (rows_written < TOTAL_ROWS)
    {
        dataChunk_reset(&chunk);
        usize batch = MIN(STANDARD_VECTOR_SIZE, TOTAL_ROWS - rows_written);
        i32* ids = (i32*)chunk.columns[0].data;
        f64* vals = (f64*)chunk.columns[1].data;
        for (usize i = 0; i < batch; i++)
        {
            ids[i] = (i32)(rows_written + i);
            vals[i] = (f64)(rows_written + i) * 1.5;
        }
        chunk.columns[0].count = batch;
        chunk.columns[1].count = batch;
        datatable_append(tbl->datatable, &chunk);
        rows_written += batch;
    }
    dataChunk_deinit(&chunk);
    PASS("Appended %d rows (crosses RowSegment boundary)", TOTAL_ROWS);

    /* Checkpoint */
    checkpointManager_createpoint(env.cpm);
    PASS("Checkpoint with %d rows", TOTAL_ROWS);

    /* Load */
    Catalog* loaded = catalog_create();
    CheckpointManager* load_cpm = make_load_cpm(&env, loaded);
    checkpointManager_loadfromstorage(load_cpm);

    TableCatalogEntry* lt = catalog_get_table(loaded, "main", "big_tbl");
    CHECK(lt != NULL, "Loaded big_tbl", "big_tbl NOT loaded");
    if (!lt) { free(load_cpm); catalog_destroy(loaded); destroy_test_env(&env); return; }

    /* Scan and verify */
    DataChunk output = MAKE(DataChunk, types);
    ScanState state;
    datatable_init_scan(lt->datatable, &state);
    usize col_ids[2] = {0, 1};
    usize total = 0;
    bool ok = true;
    while (true)
    {
        dataChunk_reset(&output);
        if (!datatable_scan(lt->datatable, &state, &output, col_ids, 2)) break;
        if (dataChunk_size(&output) == 0) break;
        i32* ids = (i32*)output.columns[0].data;
        f64* vals = (f64*)output.columns[1].data;
        for (usize i = 0; i < dataChunk_size(&output); i++)
        {
            i32 eid = (i32)(total + i);
            f64 eval = (f64)(total + i) * 1.5;
            if (ids[i] != eid) { ok = false; FAIL("Row %zu: id=%d expected=%d", total+i, ids[i], eid); }
            if (vals[i] != eval) { ok = false; FAIL("Row %zu: val=%.2f expected=%.2f", total+i, vals[i], eval); }
        }
        total += dataChunk_size(&output);
    }
    if (total == TOTAL_ROWS)
        PASS("Scan returned correct row count (%d)", TOTAL_ROWS);
    else
        FAIL("Scan row count mismatch: got %zu, expected %d", total, TOTAL_ROWS);
    if (ok && total == TOTAL_ROWS)
        PASS("All %d rows verified after large roundtrip", TOTAL_ROWS);

    scanstate_deinit(&state);
    dataChunk_deinit(&output);
    vector_deinit(&types);
    free(load_cpm);
    catalog_destroy(loaded);
    destroy_test_env(&env);
    #undef TOTAL_ROWS
}

/* ============================================================
 * Test: Multiple tables roundtrip
 * ============================================================ */
void test_multi_table_roundtrip(void)
{
    printf("\n=== Test multi-table roundtrip ===\n");

    TestEnv env = create_test_env();

    /* Create table1 (2 INT32 cols, 20 rows) */
    ColumnDefinition cols1[2] = {
        {.name = "a", .oid = 0, .type = SQLT_INTEGER},
        {.name = "b", .oid = 1, .type = SQLT_INTEGER},
    };
    CreateTableInfo ti1 = {
        .schema_name = "main", .table_name = "t1",
        .if_not_exists = false, .columns = cols1, .col_count = 2
    };
    catalog_create_table(env.catalog, &ti1);

    /* Create table2 (1 DOUBLE col, 30 rows) */
    ColumnDefinition cols2[1] = {
        {.name = "x", .oid = 0, .type = SQLT_DOUBLE},
    };
    CreateTableInfo ti2 = {
        .schema_name = "main", .table_name = "t2",
        .if_not_exists = false, .columns = cols2, .col_count = 1
    };
    catalog_create_table(env.catalog, &ti2);

    /* Append data to t1 */
    TableCatalogEntry* tbl1 = catalog_get_table(env.catalog, "main", "t1");
    {
        Vector types = VEC(TypeID, 2);
        TypeID t32 = TYPE_INT32;
        vector_push_back(&types, &t32);
        vector_push_back(&types, &t32);
        DataChunk chunk = MAKE(DataChunk, types);
        i32* ca = (i32*)chunk.columns[0].data;
        i32* cb = (i32*)chunk.columns[1].data;
        for (int i = 0; i < 20; i++) { ca[i] = i; cb[i] = i * 100; }
        chunk.columns[0].count = 20;
        chunk.columns[1].count = 20;
        datatable_append(tbl1->datatable, &chunk);
        dataChunk_deinit(&chunk);
        vector_deinit(&types);
    }

    /* Append data to t2 */
    TableCatalogEntry* tbl2 = catalog_get_table(env.catalog, "main", "t2");
    {
        Vector types = VEC(TypeID, 1);
        TypeID t64 = TYPE_FLOAT64;
        vector_push_back(&types, &t64);
        DataChunk chunk = MAKE(DataChunk, types);
        f64* cx = (f64*)chunk.columns[0].data;
        for (int i = 0; i < 30; i++) cx[i] = i * 3.14;
        chunk.columns[0].count = 30;
        datatable_append(tbl2->datatable, &chunk);
        dataChunk_deinit(&chunk);
        vector_deinit(&types);
    }

    PASS("Populated t1(20 rows) and t2(30 rows)");

    /* Checkpoint */
    checkpointManager_createpoint(env.cpm);
    PASS("Checkpoint with 2 tables");

    /* Load */
    Catalog* loaded = catalog_create();
    CheckpointManager* load_cpm = make_load_cpm(&env, loaded);
    checkpointManager_loadfromstorage(load_cpm);

    TableCatalogEntry* lt1 = catalog_get_table(loaded, "main", "t1");
    TableCatalogEntry* lt2 = catalog_get_table(loaded, "main", "t2");
    CHECK(lt1 != NULL, "Loaded t1", "t1 NOT loaded");
    CHECK(lt2 != NULL, "Loaded t2", "t2 NOT loaded");

    /* Verify t1 */
    if (lt1)
    {
        Vector types = VEC(TypeID, 2);
        TypeID t32 = TYPE_INT32;
        vector_push_back(&types, &t32);
        vector_push_back(&types, &t32);
        DataChunk output = MAKE(DataChunk, types);
        ScanState state;
        datatable_init_scan(lt1->datatable, &state);
        usize col_ids[2] = {0, 1};
        usize total = 0;
        while (true)
        {
            dataChunk_reset(&output);
            if (!datatable_scan(lt1->datatable, &state, &output, col_ids, 2)) break;
            if (dataChunk_size(&output) == 0) break;
            total += dataChunk_size(&output);
        }
        CHECK(total == 20, "t1 has 20 rows after load", "t1 row count mismatch");
        scanstate_deinit(&state);
        dataChunk_deinit(&output);
        vector_deinit(&types);
    }

    /* Verify t2 */
    if (lt2)
    {
        Vector types = VEC(TypeID, 1);
        TypeID t64 = TYPE_FLOAT64;
        vector_push_back(&types, &t64);
        DataChunk output = MAKE(DataChunk, types);
        ScanState state;
        datatable_init_scan(lt2->datatable, &state);
        usize col_ids[1] = {0};
        usize total = 0;
        while (true)
        {
            dataChunk_reset(&output);
            if (!datatable_scan(lt2->datatable, &state, &output, col_ids, 1)) break;
            if (dataChunk_size(&output) == 0) break;
            total += dataChunk_size(&output);
        }
        CHECK(total == 30, "t2 has 30 rows after load", "t2 row count mismatch");
        scanstate_deinit(&state);
        dataChunk_deinit(&output);
        vector_deinit(&types);
    }

    free(load_cpm);
    catalog_destroy(loaded);
    destroy_test_env(&env);
}

/* ============================================================
 * Test: Empty table roundtrip
 * ============================================================ */
void test_empty_table_roundtrip(void)
{
    printf("\n=== Test empty table roundtrip ===\n");

    TestEnv env = create_test_env();

    ColumnDefinition cols[1] = {
        {.name = "x", .oid = 0, .type = SQLT_INTEGER},
    };
    CreateTableInfo tinfo = {
        .schema_name = "main", .table_name = "empty_tbl",
        .if_not_exists = false, .columns = cols, .col_count = 1
    };
    catalog_create_table(env.catalog, &tinfo);
    PASS("Created empty table 'empty_tbl'");

    checkpointManager_createpoint(env.cpm);
    PASS("Checkpoint with empty table");

    Catalog* loaded = catalog_create();
    CheckpointManager* load_cpm = make_load_cpm(&env, loaded);
    checkpointManager_loadfromstorage(load_cpm);

    TableCatalogEntry* lt = catalog_get_table(loaded, "main", "empty_tbl");
    CHECK(lt != NULL, "Loaded empty_tbl", "empty_tbl NOT loaded");
    if (lt)
    {
        CHECK(lt->column_count == 1, "column_count == 1", "column_count wrong");
        ScanState state;
        datatable_init_scan(lt->datatable, &state);
        Vector types = VEC(TypeID, 1);
        TypeID t32 = TYPE_INT32;
        vector_push_back(&types, &t32);
        DataChunk output = MAKE(DataChunk, types);
        dataChunk_reset(&output);
        usize col_ids[1] = {0};
        bool has = datatable_scan(lt->datatable, &state, &output, col_ids, 1);
        usize rows = dataChunk_size(&output);
        CHECK(rows == 0, "Empty table scan returns 0 rows", "Expected 0 rows from empty table");
        scanstate_deinit(&state);
        dataChunk_deinit(&output);
        vector_deinit(&types);
        (void)has;
    }

    free(load_cpm);
    catalog_destroy(loaded);
    destroy_test_env(&env);
}

/* ============================================================
 * main
 * ============================================================ */
int main(void)
{
    printf("========================================\n");
    printf("   CheckpointManager Test Suite\n");
    printf("========================================\n");

    // Schema-only tests
    test_createpoint_single_schema();
    test_createpoint_multiple_schemas();
    test_loadfromstorage_empty();
    test_double_checkpoint();
    test_free_list_management();
    test_header_alternation();

    // Schema roundtrip tests
    test_roundtrip_single_schema();
    test_roundtrip_multi_schema();
    test_roundtrip_many_schemas();
    test_two_full_cycles();

    // Disk persistence test
    test_reopen_database();

    // Table data roundtrip tests
    test_empty_table_roundtrip();
    test_table_data_roundtrip();
    test_table_data_roundtrip_large();
    test_multi_table_roundtrip();

    printf("\n========================================\n");
    printf("   Results: %d passed, %d failed\n", pass_count, fail_count);
    printf("========================================\n\n");

    return fail_count > 0 ? 1 : 0;
}
