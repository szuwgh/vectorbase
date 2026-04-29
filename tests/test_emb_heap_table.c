/**
 * test_emb_heap_table.c
 *
 * Tests for EmbeddingHeapTable — specifically:
 *   embeddingHeapTable_append_chunk  (batch insert via vtable .append_chunk)
 *   embeddingHeapTable_scan_chunk    (top-K ANN scan via vtable .scan)
 *
 * Compile & run:
 *   cd tests && make test_emb_heap_table && ./test_emb_heap_table
 *   -- or --
 *   cd tests && make test
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>

#include "../src/table.h"    /* EmbeddingHeapTable, DataChunk, TableQueryResult */
#include "../src/operator.h" /* DistanceType: L2, L2_SQUARED, COSINE, ... */

/* ---- test harness ---- */
static int pass_count = 0, fail_count = 0;

#define CHECK(cond, msg)                                               \
    do                                                                 \
    {                                                                  \
        if (cond)                                                      \
        {                                                              \
            pass_count++;                                              \
            printf("[PASS] %s\n", msg);                               \
        }                                                              \
        else                                                           \
        {                                                              \
            fail_count++;                                              \
            printf("[FAIL] %s  (line %d)\n", msg, __LINE__);         \
        }                                                              \
    } while (0)

/* ---- schema with zero user payload columns (pure-vector table) ---- */
static const TupleColType no_cols[1] = {0};
static const TableSchema empty_schema = {.cols = no_cols, .ncols = 0};

/* ================================================================
 * Helper: fill a DataChunk for EMBED mode (no payload columns)
 * ================================================================ */
static void make_chunk(DataChunk* c, VectorBase* vecs, usize n)
{
    memset(c, 0, sizeof(*c));
    c->mode       = CHUNK_EMBED;
    c->count      = n;
    c->arrays     = vecs;
    c->payloads   = NULL;
    c->n_payloads = 0;
}

/* ================================================================
 * Test 1: append_chunk stores vectors; emb_ctids are filled & valid
 * ================================================================ */
static void test_append_chunk_fills_emb_ctids(void)
{
    printf("\n--- test_append_chunk_fills_emb_ctids ---\n");

    EmbeddingHeapTable table;
    EmbeddingHeapTable_init(&table, 4, &empty_schema, NULL);

    f32 d0[4] = {1.0f, 0.0f, 0.0f, 0.0f};
    f32 d1[4] = {0.0f, 1.0f, 0.0f, 0.0f};
    f32 d2[4] = {0.0f, 0.0f, 1.0f, 0.0f};

    VectorBase vecs[3] = {
        {TYPE_FLOAT32, 4, (data_ptr_t)d0},
        {TYPE_FLOAT32, 4, (data_ptr_t)d1},
        {TYPE_FLOAT32, 4, (data_ptr_t)d2},
    };
    DataChunk chunk;
    make_chunk(&chunk, vecs, 3);

    ItemPtr emb_buf[3];
    TamInsertCtx ctx = {.emb_ctids = emb_buf, .count = 3, .xid = 1};

    /* Insert via vtable — exercises embeddingHeapTable_append_chunk */
    VCALL(&table.base, append_chunk, &chunk, &ctx);

    CHECK(item_ptr_is_valid(emb_buf[0]), "emb_ctid[0] valid");
    CHECK(item_ptr_is_valid(emb_buf[1]), "emb_ctid[1] valid");
    CHECK(item_ptr_is_valid(emb_buf[2]), "emb_ctid[2] valid");

    /* All three ctids must be distinct */
    u64 p0 = ((u64)emb_buf[0].ip_blkid_hi << 32) | ((u64)emb_buf[0].ip_blkid_lo << 16) | emb_buf[0].ip_posid;
    u64 p1 = ((u64)emb_buf[1].ip_blkid_hi << 32) | ((u64)emb_buf[1].ip_blkid_lo << 16) | emb_buf[1].ip_posid;
    u64 p2 = ((u64)emb_buf[2].ip_blkid_hi << 32) | ((u64)emb_buf[2].ip_blkid_lo << 16) | emb_buf[2].ip_posid;

    CHECK(p0 != p1, "emb_ctid[0] != emb_ctid[1]");
    CHECK(p1 != p2, "emb_ctid[1] != emb_ctid[2]");
    CHECK(p0 != p2, "emb_ctid[0] != emb_ctid[2]");

    EmbeddingHeapTable_deinit(&table);
}

/* ================================================================
 * Test 2: scan_chunk returns the correct nearest neighbour (L2)
 * ================================================================ */
static void test_scan_returns_nearest_neighbour(void)
{
    printf("\n--- test_scan_returns_nearest_neighbour ---\n");

    EmbeddingHeapTable table;
    EmbeddingHeapTable_init(&table, 4, &empty_schema, NULL);

    /* Three orthogonal unit vectors; query == d0 exactly */
    f32 d0[4] = {1.0f, 0.0f, 0.0f, 0.0f};
    f32 d1[4] = {0.0f, 1.0f, 0.0f, 0.0f};
    f32 d2[4] = {0.0f, 0.0f, 1.0f, 0.0f};

    VectorBase vecs[3] = {
        {TYPE_FLOAT32, 4, (data_ptr_t)d0},
        {TYPE_FLOAT32, 4, (data_ptr_t)d1},
        {TYPE_FLOAT32, 4, (data_ptr_t)d2},
    };
    DataChunk chunk;
    make_chunk(&chunk, vecs, 3);

    ItemPtr emb_buf[3];
    TamInsertCtx ctx = {.emb_ctids = emb_buf, .count = 3, .xid = 1};
    VCALL(&table.base, append_chunk, &chunk, &ctx);

    /* Query vector == d0 → distance to d0 should be ~0 */
    f32 q[4] = {1.0f, 0.0f, 0.0f, 0.0f};
    VectorCondition vc = {
        .query  = {TYPE_FLOAT32, 4, (data_ptr_t)q},
        .metric = L2,
    };
    TamScanCtx scan_ctx = {.vec_cond = &vc, .k = 1};
    TableQueryResult results[1];

    /* Exercises embeddingHeapTable_scan_chunk */
    int nout = VCALL(&table.base, scan, &scan_ctx, results, 1);

    CHECK(nout == 1, "scan k=1 returns exactly 1 result");
    if (nout > 0)
    {
        CHECK(results[0].distance < 0.001f,
              "nearest result L2 distance to exact query ≈ 0");
        CHECK(item_ptr_is_valid(results[0].heap_ctid), "result heap_ctid is valid");
        free(results[0].payloads);
    }

    EmbeddingHeapTable_deinit(&table);
}

/* ================================================================
 * Test 3: scan_chunk results are sorted nearest-first (ascending dist)
 * ================================================================ */
static void test_scan_sorted_nearest_first(void)
{
    printf("\n--- test_scan_sorted_nearest_first ---\n");

    EmbeddingHeapTable table;
    EmbeddingHeapTable_init(&table, 2, &empty_schema, NULL);

    /*
     * Query = [1, 0].  Expected L2 distances (exact):
     *   d0 = [1, 0]   → 0.00
     *   d2 = [0, 0]   → 1.00
     *   d1 = [0, 1]   → sqrt(2) ≈ 1.414
     *   d3 = [-1, 0]  → 2.00
     */
    f32 d0[2] = { 1.0f,  0.0f};
    f32 d1[2] = { 0.0f,  1.0f};
    f32 d2[2] = { 0.0f,  0.0f};
    f32 d3[2] = {-1.0f,  0.0f};

    VectorBase vecs[4] = {
        {TYPE_FLOAT32, 2, (data_ptr_t)d0},
        {TYPE_FLOAT32, 2, (data_ptr_t)d1},
        {TYPE_FLOAT32, 2, (data_ptr_t)d2},
        {TYPE_FLOAT32, 2, (data_ptr_t)d3},
    };
    DataChunk chunk;
    make_chunk(&chunk, vecs, 4);

    ItemPtr emb_buf[4];
    TamInsertCtx ctx = {.emb_ctids = emb_buf, .count = 4, .xid = 1};
    VCALL(&table.base, append_chunk, &chunk, &ctx);

    f32 q[2] = {1.0f, 0.0f};
    VectorCondition vc = {.query = {TYPE_FLOAT32, 2, (data_ptr_t)q}, .metric = L2};
    TamScanCtx scan_ctx = {.vec_cond = &vc, .k = 4};
    TableQueryResult results[4];

    int nout = VCALL(&table.base, scan, &scan_ctx, results, 4);
    CHECK(nout == 4, "scan returns all 4 vectors");

    /* distances must be non-decreasing */
    int sorted = 1;
    for (int i = 1; i < nout; i++)
        if (results[i].distance < results[i - 1].distance) sorted = 0;
    CHECK(sorted, "results are sorted nearest-first");

    /* nearest result: d0, distance ≈ 0 */
    if (nout > 0)
        CHECK(results[0].distance < 0.001f, "nearest result dist ≈ 0.0 (exact match)");

    /* farthest result: d3=[-1,0], distance = 2.0 */
    if (nout == 4)
        CHECK(fabsf(results[3].distance - 2.0f) < 0.001f,
              "farthest result dist ≈ 2.0");

    for (int i = 0; i < nout; i++)
        free(results[i].payloads);

    EmbeddingHeapTable_deinit(&table);
}

/* ================================================================
 * Test 4: scan_chunk with k < total returns only k results
 * ================================================================ */
static void test_scan_respects_k_limit(void)
{
    printf("\n--- test_scan_respects_k_limit ---\n");

    EmbeddingHeapTable table;
    EmbeddingHeapTable_init(&table, 2, &empty_schema, NULL);

    /* Insert 5 vectors */
    f32 data[5][2] = {
        { 1.0f,  0.0f},
        { 0.0f,  1.0f},
        {-1.0f,  0.0f},
        { 0.0f, -1.0f},
        { 0.5f,  0.5f},
    };
    VectorBase vecs[5];
    for (int i = 0; i < 5; i++)
        vecs[i] = (VectorBase){TYPE_FLOAT32, 2, (data_ptr_t)data[i]};

    DataChunk chunk;
    make_chunk(&chunk, vecs, 5);
    ItemPtr emb_buf[5];
    TamInsertCtx ctx = {.emb_ctids = emb_buf, .count = 5, .xid = 1};
    VCALL(&table.base, append_chunk, &chunk, &ctx);

    /* Request only top-2 */
    f32 q[2] = {1.0f, 0.0f};
    VectorCondition vc = {.query = {TYPE_FLOAT32, 2, (data_ptr_t)q}, .metric = L2};
    TamScanCtx scan_ctx = {.vec_cond = &vc, .k = 2};
    TableQueryResult results[2];

    int nout = VCALL(&table.base, scan, &scan_ctx, results, 2);
    CHECK(nout == 2, "k=2 scan returns exactly 2 results out of 5 inserted");

    if (nout == 2)
        CHECK(results[0].distance <= results[1].distance,
              "top-2 sorted nearest-first");

    for (int i = 0; i < nout; i++)
        free(results[i].payloads);

    EmbeddingHeapTable_deinit(&table);
}

/* ================================================================
 * Test 5: scan_chunk with COSINE metric
 * ================================================================ */
static void test_scan_cosine_metric(void)
{
    printf("\n--- test_scan_cosine_metric ---\n");

    EmbeddingHeapTable table;
    EmbeddingHeapTable_init(&table, 3, &empty_schema, NULL);

    /* Query = [1,1,0] (normalised direction). Nearest by cosine: [1,1,0]. */
    f32 d0[3] = {1.0f, 1.0f, 0.0f};   /* cosine dist to q: 0    */
    f32 d1[3] = {1.0f, 0.0f, 0.0f};   /* cosine dist to q: ~0.29 */
    f32 d2[3] = {0.0f, 0.0f, 1.0f};   /* cosine dist to q: 1.0  (orthogonal) */

    VectorBase vecs[3] = {
        {TYPE_FLOAT32, 3, (data_ptr_t)d0},
        {TYPE_FLOAT32, 3, (data_ptr_t)d1},
        {TYPE_FLOAT32, 3, (data_ptr_t)d2},
    };
    DataChunk chunk;
    make_chunk(&chunk, vecs, 3);
    ItemPtr emb_buf[3];
    TamInsertCtx ctx = {.emb_ctids = emb_buf, .count = 3, .xid = 1};
    VCALL(&table.base, append_chunk, &chunk, &ctx);

    f32 q[3] = {1.0f, 1.0f, 0.0f};
    VectorCondition vc = {.query = {TYPE_FLOAT32, 3, (data_ptr_t)q}, .metric = COSINE};
    TamScanCtx scan_ctx = {.vec_cond = &vc, .k = 1};
    TableQueryResult results[1];

    int nout = VCALL(&table.base, scan, &scan_ctx, results, 1);
    CHECK(nout == 1, "cosine scan returns 1 result");
    if (nout > 0)
    {
        CHECK(results[0].distance < 0.001f,
              "nearest by cosine has cosine distance ≈ 0 (same direction)");
        free(results[0].payloads);
    }

    EmbeddingHeapTable_deinit(&table);
}

/* ================================================================
 * Test 6: append two batches; total scan finds all inserted vectors
 * ================================================================ */
static void test_two_append_chunks(void)
{
    printf("\n--- test_two_append_chunks ---\n");

    EmbeddingHeapTable table;
    EmbeddingHeapTable_init(&table, 2, &empty_schema, NULL);

    /* Batch 1: 2 vectors */
    f32 b1d0[2] = {1.0f, 0.0f};
    f32 b1d1[2] = {0.0f, 1.0f};
    VectorBase vecs1[2] = {
        {TYPE_FLOAT32, 2, (data_ptr_t)b1d0},
        {TYPE_FLOAT32, 2, (data_ptr_t)b1d1},
    };
    DataChunk chunk1;
    make_chunk(&chunk1, vecs1, 2);
    ItemPtr emb_buf1[2];
    TamInsertCtx ctx1 = {.emb_ctids = emb_buf1, .count = 2, .xid = 1};
    VCALL(&table.base, append_chunk, &chunk1, &ctx1);

    /* Batch 2: 2 more vectors */
    f32 b2d0[2] = {-1.0f,  0.0f};
    f32 b2d1[2] = { 0.0f, -1.0f};
    VectorBase vecs2[2] = {
        {TYPE_FLOAT32, 2, (data_ptr_t)b2d0},
        {TYPE_FLOAT32, 2, (data_ptr_t)b2d1},
    };
    DataChunk chunk2;
    make_chunk(&chunk2, vecs2, 2);
    ItemPtr emb_buf2[2];
    TamInsertCtx ctx2 = {.emb_ctids = emb_buf2, .count = 2, .xid = 2};
    VCALL(&table.base, append_chunk, &chunk2, &ctx2);

    /* Scan top-4: should see all 4 vectors */
    f32 q[2] = {1.0f, 0.0f};
    VectorCondition vc = {.query = {TYPE_FLOAT32, 2, (data_ptr_t)q}, .metric = L2};
    TamScanCtx scan_ctx = {.vec_cond = &vc, .k = 4};
    TableQueryResult results[4];

    int nout = VCALL(&table.base, scan, &scan_ctx, results, 4);
    CHECK(nout == 4, "scan after two batches finds all 4 vectors");

    for (int i = 0; i < nout; i++)
        free(results[i].payloads);

    EmbeddingHeapTable_deinit(&table);
}

/* ================================================================
 * main
 * ================================================================ */
int main(void)
{
    printf("=== test_emb_heap_table ===\n");

    test_append_chunk_fills_emb_ctids();
    test_scan_returns_nearest_neighbour();
    test_scan_sorted_nearest_first();
    test_scan_respects_k_limit();
    test_scan_cosine_metric();
    test_two_append_chunks();

    printf("\n=== Results: %d passed, %d failed ===\n", pass_count, fail_count);
    return fail_count > 0 ? 1 : 0;
}
