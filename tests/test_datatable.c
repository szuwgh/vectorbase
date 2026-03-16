/*
 * Comprehensive test suite for datatable_append / datatable_scan
 *
 * Coverage targets:
 *   A. Datatable_create — initial state
 *   B. datatable_append — edge: empty, 1 row, exact chunk, chunk+1
 *   C. datatable_append — multiple appends / RowSegment boundary
 *   D. datatable_append — ColumnSegment boundary crossing
 *   E. datatable_scan — empty, single row
 *   F. datatable_scan — batch sizes / multi-chunk
 *   G. datatable_scan — all four TypeIDs
 *   H. datatable_scan — column projection
 *   I. datatable_scan — ColumnSegment boundary crossing
 *   J. datatable_scan — re-scan / exhaustion / multiple passes
 *   K. DataChunk / VectorBase helpers
 *   L. Combined append + scan workflows
 *
 * Key constants:
 *   STORAGE_CHUNK_SIZE  = 250  rows per RowSegment
 *   BLOCK_SIZE          = 8192 bytes per ColumnSegment block
 *   INT32  → 2048 elems/segment   INT64  → 1024 elems/segment
 *   FLOAT32→ 2048 elems/segment   FLOAT64→ 1024 elems/segment
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "../src/datatable.h"
#include "../src/segment.h"
#include "../src/vb_type.h"
#include "../src/vector.h"
#include "../src/types.h"

/* Compatibility wrapper: old API DataChunk_init_compat(&chunk, col_count) used TYPE_INT32
 * placeholders. The new API requires a Vector of TypeID. This wrapper preserves the old call-site
 * semantics. */
static void DataChunk_init_compat(DataChunk* chunk, usize col_count)
{
    Vector types = VEC(TypeID, col_count);
    TypeID placeholder = TYPE_INT32;
    for (usize i = 0; i < col_count; i++) vector_push_back(&types, &placeholder);
    DataChunk_init(chunk, types);
    vector_deinit(&types);
}

/* ============================================================
 * Test Framework
 * ============================================================ */

static int g_pass = 0;
static int g_fail = 0;

#define ASSERT_TRUE(cond, msg)                                           \
    do {                                                                 \
        if (cond)                                                        \
        {                                                                \
            printf("  [PASS] %s\n", (msg));                              \
            g_pass++;                                                    \
        }                                                                \
        else                                                             \
        {                                                                \
            printf("  [FAIL] %s  (%s:%d)\n", (msg), __FILE__, __LINE__); \
            g_fail++;                                                    \
        }                                                                \
    } while (0)

#define ASSERT_EQ_U64(actual, expected, msg)                                                   \
    do {                                                                                       \
        u64 _a = (u64)(actual), _e = (u64)(expected);                                          \
        if (_a == _e)                                                                          \
        {                                                                                      \
            printf("  [PASS] %s\n", (msg));                                                    \
            g_pass++;                                                                          \
        }                                                                                      \
        else                                                                                   \
        {                                                                                      \
            printf("  [FAIL] %s (expected %lu, got %lu)  (%s:%d)\n", (msg), (unsigned long)_e, \
                   (unsigned long)_a, __FILE__, __LINE__);                                     \
            g_fail++;                                                                          \
        }                                                                                      \
    } while (0)

/* ============================================================
 * Helpers
 * ============================================================ */

/* Per-column capacity in elements for one ColumnSegment block */
#define COL_SEG_CAP_I32 (BLOCK_SIZE / (usize)sizeof(i32))   /* 2048 */
#define COL_SEG_CAP_I64 (BLOCK_SIZE / (usize)sizeof(i64))   /* 1024 */

/* Allocate scan output DataChunk with pre-allocated column buffers.
 * Each buffer sized for STORAGE_CHUNK_SIZE elements of the widest type (i64). */
static DataChunk alloc_output(usize col_count, usize* type_sizes)
{
    DataChunk out;
    DataChunk_init_compat(&out, col_count);
    for (usize i = 0; i < col_count; i++)
        out.columns[i].data = calloc(STORAGE_CHUNK_SIZE, type_sizes[i]);
    return out;
}

static void free_output(DataChunk* out)
{
    for (usize i = 0; i < out->column_count; i++) free(out->columns[i].data);
    free(out->columns);
}

/* Full table scan: collect every element of each projected column into
 * caller-supplied flat arrays.  Returns total row count.  */
static usize scan_all(DataTable* table, usize* column_ids, usize col_count, void** col_bufs,
                      usize* type_sizes)
{
    ScanState st;
    datatable_init_scan(table, &st);
    DataChunk out = alloc_output(col_count, type_sizes);

    usize total = 0;
    while (datatable_scan(table, &st, &out, column_ids, col_count))
    {
        usize n = out.columns[0].count;
        for (usize c = 0; c < col_count; c++)
        {
            memcpy((u8*)col_bufs[c] + total * type_sizes[c], out.columns[c].data,
                   n * type_sizes[c]);
        }
        total += n;
    }
    free_output(&out);
    free(st.columns);
    return total;
}

/* ============================================================
 * A. Datatable_create — initial state
 * ============================================================ */

static void test_create_initial_state(void)
{
    printf("\n--- A. test_create_initial_state ---\n");

    TypeID types[] = {TYPE_INT32, TYPE_INT64, TYPE_FLOAT32};
    DataTable* table = Datatable_create(NULL, "s", "t", 3, types);

    ASSERT_EQ_U64(table->column_count, 3, "column_count == 3");
    ASSERT_TRUE(table->column_types[0] == TYPE_INT32, "type[0] INT32");
    ASSERT_TRUE(table->column_types[1] == TYPE_INT64, "type[1] INT64");
    ASSERT_TRUE(table->column_types[2] == TYPE_FLOAT32, "type[2] FLOAT32");

    /* row_storage_tree has exactly one (empty) RowSegment */
    RowSegment* root = (RowSegment*)segmentTree_get_root_segment(&table->row_storage_tree);
    ASSERT_TRUE(root != NULL, "root RowSegment exists");
    ASSERT_EQ_U64(root->base.count, 0, "initial RowSegment count == 0");
    ASSERT_EQ_U64(root->base.start, 0, "initial RowSegment start == 0");

    /* each column_storage_tree has one ColumnSegment */
    for (usize i = 0; i < 3; i++)
    {
        ColumnSegment* cs =
            (ColumnSegment*)segmentTree_get_root_segment(&table->column_storage_tree[i]);
        char msg[64];
        snprintf(msg, sizeof(msg), "col %lu: initial ColumnSegment exists", (unsigned long)i);
        ASSERT_TRUE(cs != NULL, msg);
        snprintf(msg, sizeof(msg), "col %lu: ColumnSegment count == 0", (unsigned long)i);
        ASSERT_EQ_U64(cs->base.count, 0, msg);
    }
}

/* ============================================================
 * B. datatable_append — edge cases
 * ============================================================ */

/* B1. Append empty chunk — no-op */
static void test_append_empty_chunk(void)
{
    printf("\n--- B1. test_append_empty_chunk ---\n");

    TypeID types[] = {TYPE_INT32};
    DataTable* table = Datatable_create(NULL, "s", "t", 1, types);

    DataChunk empty;
    DataChunk_init_compat(&empty, 1);
    empty.columns[0] = (VectorBase){.type = TYPE_INT32, .count = 0, .data = NULL};
    datatable_append(table, &empty);

    RowSegment* root = (RowSegment*)segmentTree_get_root_segment(&table->row_storage_tree);
    ASSERT_EQ_U64(root->base.count, 0, "after empty append: count still 0");

    free(empty.columns);
}

/* B2. Append single row */
static void test_append_single_row(void)
{
    printf("\n--- B2. test_append_single_row ---\n");

    TypeID types[] = {TYPE_INT32};
    DataTable* table = Datatable_create(NULL, "s", "t", 1, types);

    DataChunk chunk;
    DataChunk_init_compat(&chunk, 1);
    i32 val = 42;
    i32* buf = malloc(sizeof(i32));
    buf[0] = val;
    chunk.arrays[0] = (VectorBase){.type = TYPE_INT32, .count = 1, .data = (data_ptr_t)buf};

    datatable_append(table, &chunk);

    RowSegment* root = (RowSegment*)segmentTree_get_root_segment(&table->row_storage_tree);
    ASSERT_EQ_U64(root->base.count, 1, "RowSegment count == 1");

    /* scan it back */
    usize ids[] = {0};
    usize ts = sizeof(i32);
    i32 out_buf[1];
    void* bufs[] = {out_buf};
    usize total = scan_all(table, ids, 1, bufs, &ts);
    ASSERT_EQ_U64(total, 1, "scanned 1 row");
    ASSERT_TRUE(out_buf[0] == 42, "value == 42");

    free(buf);
    free(chunk.columns);
}

/* B3. Append exactly STORAGE_CHUNK_SIZE rows → 1 full RowSegment */
static void test_append_exact_chunk_size(void)
{
    printf("\n--- B3. test_append_exact_chunk_size (%lu rows) ---\n",
           (unsigned long)STORAGE_CHUNK_SIZE);

    TypeID types[] = {TYPE_INT32};
    DataTable* table = Datatable_create(NULL, "s", "t", 1, types);

    i32* data = malloc(STORAGE_CHUNK_SIZE * sizeof(i32));
    for (usize i = 0; i < STORAGE_CHUNK_SIZE; i++) data[i] = (i32)i;

    DataChunk chunk;
    DataChunk_init_compat(&chunk, 1);
    chunk.arrays[0] =
        (VectorBase){.type = TYPE_INT32, .count = STORAGE_CHUNK_SIZE, .data = (data_ptr_t)data};
    datatable_append(table, &chunk);

    RowSegment* root = (RowSegment*)segmentTree_get_root_segment(&table->row_storage_tree);
    ASSERT_EQ_U64(root->base.count, STORAGE_CHUNK_SIZE, "RowSegment full (250)");
    /* no second segment created */
    ASSERT_TRUE(root->base.next == NULL, "no second RowSegment");

    usize ids[] = {0};
    usize ts = sizeof(i32);
    i32* out = malloc(STORAGE_CHUNK_SIZE * sizeof(i32));
    void* bufs[] = {out};
    usize total = scan_all(table, ids, 1, bufs, &ts);
    ASSERT_EQ_U64(total, STORAGE_CHUNK_SIZE, "scanned 250 rows");
    ASSERT_TRUE(memcmp(out, data, STORAGE_CHUNK_SIZE * sizeof(i32)) == 0,
                "byte-identical to input");

    free(out);
    free(data);
    free(chunk.columns);
}

/* B4. Append STORAGE_CHUNK_SIZE + 1 rows → 2 RowSegments */
static void test_append_chunk_plus_one(void)
{
    printf("\n--- B4. test_append_chunk_plus_one (%lu+1 rows) ---\n",
           (unsigned long)STORAGE_CHUNK_SIZE);

    const usize N = STORAGE_CHUNK_SIZE + 1; /* 251 */
    TypeID types[] = {TYPE_INT32};
    DataTable* table = Datatable_create(NULL, "s", "t", 1, types);

    i32* data = malloc(N * sizeof(i32));
    for (usize i = 0; i < N; i++) data[i] = (i32)(i + 100);

    DataChunk chunk;
    DataChunk_init_compat(&chunk, 1);
    chunk.arrays[0] = (VectorBase){.type = TYPE_INT32, .count = N, .data = (data_ptr_t)data};
    datatable_append(table, &chunk);

    RowSegment* r0 = (RowSegment*)segmentTree_get_root_segment(&table->row_storage_tree);
    ASSERT_EQ_U64(r0->base.count, STORAGE_CHUNK_SIZE, "1st RowSegment count == 250");
    ASSERT_TRUE(r0->base.next != NULL, "2nd RowSegment exists");
    RowSegment* r1 = (RowSegment*)r0->base.next;
    ASSERT_EQ_U64(r1->base.count, 1, "2nd RowSegment count == 1");

    usize ids[] = {0};
    usize ts = sizeof(i32);
    i32* out = malloc(N * sizeof(i32));
    void* bufs[] = {out};
    usize total = scan_all(table, ids, 1, bufs, &ts);
    ASSERT_EQ_U64(total, N, "total scanned == 251");
    ASSERT_TRUE(memcmp(out, data, N * sizeof(i32)) == 0, "byte-identical");

    free(out);
    free(data);
    free(chunk.columns);
}

/* ============================================================
 * C. datatable_append — multiple appends / RowSegment boundary
 * ============================================================ */

/* C1. Multiple small appends within one RowSegment */
static void test_append_multi_small_within_chunk(void)
{
    printf("\n--- C1. test_append_multi_small_within_chunk ---\n");

    TypeID types[] = {TYPE_INT32};
    DataTable* table = Datatable_create(NULL, "s", "t", 1, types);

    /* 5 appends of 40 rows = 200 total, all within one RowSegment (250) */
    i32 expect[200];
    for (int batch = 0; batch < 5; batch++)
    {
        i32* buf = malloc(40 * sizeof(i32));
        for (int j = 0; j < 40; j++)
        {
            buf[j] = batch * 40 + j;
            expect[batch * 40 + j] = buf[j];
        }
        DataChunk c;
        DataChunk_init_compat(&c, 1);
        c.columns[0] = (VectorBase){.type = TYPE_INT32, .count = 40, .data = (data_ptr_t)buf};
        datatable_append(table, &c);
        free(buf);
        free(c.columns);
    }

    RowSegment* root = (RowSegment*)segmentTree_get_root_segment(&table->row_storage_tree);
    ASSERT_EQ_U64(root->base.count, 200, "single RowSegment count == 200");
    ASSERT_TRUE(root->base.next == NULL, "no second RowSegment");

    usize ids[] = {0};
    usize ts = sizeof(i32);
    i32 out[200];
    void* bufs[] = {out};
    usize total = scan_all(table, ids, 1, bufs, &ts);
    ASSERT_EQ_U64(total, 200, "scanned 200 rows");
    ASSERT_TRUE(memcmp(out, expect, sizeof(expect)) == 0, "values correct");
}

/* C2. Two appends crossing RowSegment boundary: 200 + 200 → [250, 150] */
static void test_append_cross_row_segment_boundary(void)
{
    printf("\n--- C2. test_append_cross_row_segment_boundary (200+200) ---\n");

    TypeID types[] = {TYPE_INT64};
    DataTable* table = Datatable_create(NULL, "s", "t", 1, types);

    i64 expect[400];
    for (int batch = 0; batch < 2; batch++)
    {
        i64* buf = malloc(200 * sizeof(i64));
        for (int j = 0; j < 200; j++)
        {
            buf[j] = batch * 200 + j;
            expect[batch * 200 + j] = buf[j];
        }
        DataChunk c;
        DataChunk_init_compat(&c, 1);
        c.columns[0] = (VectorBase){.type = TYPE_INT64, .count = 200, .data = (data_ptr_t)buf};
        datatable_append(table, &c);
        free(buf);
        free(c.columns);
    }

    RowSegment* r0 = (RowSegment*)segmentTree_get_root_segment(&table->row_storage_tree);
    ASSERT_EQ_U64(r0->base.count, 250, "1st RowSegment == 250");
    RowSegment* r1 = (RowSegment*)r0->base.next;
    ASSERT_TRUE(r1 != NULL, "2nd RowSegment exists");
    ASSERT_EQ_U64(r1->base.count, 150, "2nd RowSegment == 150");

    usize ids[] = {0};
    usize ts = sizeof(i64);
    i64 out[400];
    void* bufs[] = {out};
    usize total = scan_all(table, ids, 1, bufs, &ts);
    ASSERT_EQ_U64(total, 400, "total scanned 400");
    ASSERT_TRUE(memcmp(out, expect, sizeof(expect)) == 0, "values correct");
}

/* C3. Append 1 row at a time, 300 times → crosses RowSegment boundary at 250 */
static void test_append_one_by_one(void)
{
    printf("\n--- C3. test_append_one_by_one (300 x 1 row) ---\n");

    TypeID types[] = {TYPE_INT32};
    DataTable* table = Datatable_create(NULL, "s", "t", 1, types);

    i32 expect[300];
    for (int k = 0; k < 300; k++)
    {
        i32 val = k * 7;
        expect[k] = val;
        i32* buf = malloc(sizeof(i32));
        buf[0] = val;
        DataChunk c;
        DataChunk_init_compat(&c, 1);
        c.columns[0] = (VectorBase){.type = TYPE_INT32, .count = 1, .data = (data_ptr_t)buf};
        datatable_append(table, &c);
        free(buf);
        free(c.columns);
    }

    RowSegment* r0 = (RowSegment*)segmentTree_get_root_segment(&table->row_storage_tree);
    ASSERT_EQ_U64(r0->base.count, 250, "1st RowSegment == 250");
    RowSegment* r1 = (RowSegment*)r0->base.next;
    ASSERT_TRUE(r1 != NULL, "2nd RowSegment exists");
    ASSERT_EQ_U64(r1->base.count, 50, "2nd RowSegment == 50");

    usize ids[] = {0};
    usize ts = sizeof(i32);
    i32 out[300];
    void* bufs[] = {out};
    usize total = scan_all(table, ids, 1, bufs, &ts);
    ASSERT_EQ_U64(total, 300, "scanned 300");
    ASSERT_TRUE(memcmp(out, expect, sizeof(expect)) == 0, "values correct");
}

/* C4. Multiple appends exactly filling multiple RowSegments (250+250+250) */
static void test_append_exact_multiple_chunks(void)
{
    printf("\n--- C4. test_append_exact_multiple_chunks (3 x 250) ---\n");

    TypeID types[] = {TYPE_INT32};
    DataTable* table = Datatable_create(NULL, "s", "t", 1, types);
    const usize N = STORAGE_CHUNK_SIZE;

    i32 expect[750];
    for (int batch = 0; batch < 3; batch++)
    {
        i32* buf = malloc(N * sizeof(i32));
        for (usize j = 0; j < N; j++)
        {
            buf[j] = (i32)(batch * N + j);
            expect[batch * N + j] = buf[j];
        }
        DataChunk c;
        DataChunk_init_compat(&c, 1);
        c.columns[0] = (VectorBase){.type = TYPE_INT32, .count = N, .data = (data_ptr_t)buf};
        datatable_append(table, &c);
        free(buf);
        free(c.columns);
    }

    /* verify 3 RowSegments, each exactly 250 */
    RowSegment* seg = (RowSegment*)segmentTree_get_root_segment(&table->row_storage_tree);
    for (int i = 0; i < 3; i++)
    {
        char msg[64];
        snprintf(msg, sizeof(msg), "RowSegment[%d] count == 250", i);
        ASSERT_TRUE(seg != NULL, msg);
        if (seg) ASSERT_EQ_U64(seg->base.count, N, msg);
        seg = seg ? (RowSegment*)seg->base.next : NULL;
    }
    ASSERT_TRUE(seg == NULL, "no 4th RowSegment");

    usize ids[] = {0};
    usize ts = sizeof(i32);
    i32 out[750];
    void* bufs[] = {out};
    usize total = scan_all(table, ids, 1, bufs, &ts);
    ASSERT_EQ_U64(total, 750, "scanned 750");
    ASSERT_TRUE(memcmp(out, expect, sizeof(expect)) == 0, "values correct");
}

/* ============================================================
 * D. datatable_append — ColumnSegment boundary crossing
 * ============================================================ */

/* D1. INT64: 1300 rows → crosses ColumnSegment boundary at 1024 */
static void test_append_cross_column_segment_i64(void)
{
    printf("\n--- D1. test_append_cross_column_segment_i64 (1300 rows, boundary@1024) ---\n");
    const usize N = 1300;

    TypeID types[] = {TYPE_INT64};
    DataTable* table = Datatable_create(NULL, "s", "t", 1, types);

    i64* data = malloc(N * sizeof(i64));
    for (usize i = 0; i < N; i++) data[i] = (i64)(i * 3 + 7);

    DataChunk c;
    DataChunk_init_compat(&c, 1);
    c.columns[0] = (VectorBase){.type = TYPE_INT64, .count = N, .data = (data_ptr_t)data};
    datatable_append(table, &c);

    /* verify that ColumnSegment chain has >1 segment */
    ColumnSegment* cs0 =
        (ColumnSegment*)segmentTree_get_root_segment(&table->column_storage_tree[0]);
    ASSERT_TRUE(cs0 != NULL, "first ColumnSegment exists");
    ASSERT_EQ_U64(cs0->base.count, COL_SEG_CAP_I64, "1st seg count == 1024");
    ASSERT_TRUE(cs0->base.next != NULL, "2nd ColumnSegment exists");
    ColumnSegment* cs1 = (ColumnSegment*)cs0->base.next;
    ASSERT_EQ_U64(cs1->base.count, N - COL_SEG_CAP_I64, "2nd seg count == 276");

    /* scan and verify */
    usize ids[] = {0};
    usize ts = sizeof(i64);
    i64* out = malloc(N * sizeof(i64));
    void* bufs[] = {out};
    usize total = scan_all(table, ids, 1, bufs, &ts);
    ASSERT_EQ_U64(total, N, "scanned 1300 rows");
    ASSERT_TRUE(memcmp(out, data, N * sizeof(i64)) == 0, "byte-identical across segment boundary");

    free(out);
    free(data);
    free(c.columns);
}

/* D2. INT32: 2200 rows → crosses ColumnSegment boundary at 2048 */
static void test_append_cross_column_segment_i32(void)
{
    printf("\n--- D2. test_append_cross_column_segment_i32 (2200 rows, boundary@2048) ---\n");
    const usize N = 2200;

    TypeID types[] = {TYPE_INT32};
    DataTable* table = Datatable_create(NULL, "s", "t", 1, types);

    i32* data = malloc(N * sizeof(i32));
    for (usize i = 0; i < N; i++) data[i] = (i32)(i * 5);

    DataChunk c;
    DataChunk_init_compat(&c, 1);
    c.columns[0] = (VectorBase){.type = TYPE_INT32, .count = N, .data = (data_ptr_t)data};
    datatable_append(table, &c);

    ColumnSegment* cs0 =
        (ColumnSegment*)segmentTree_get_root_segment(&table->column_storage_tree[0]);
    ASSERT_EQ_U64(cs0->base.count, COL_SEG_CAP_I32, "1st seg count == 2048");
    ASSERT_TRUE(cs0->base.next != NULL, "2nd ColumnSegment exists");
    ColumnSegment* cs1 = (ColumnSegment*)cs0->base.next;
    ASSERT_EQ_U64(cs1->base.count, N - COL_SEG_CAP_I32, "2nd seg count == 152");

    usize ids[] = {0};
    usize ts = sizeof(i32);
    i32* out = malloc(N * sizeof(i32));
    void* bufs[] = {out};
    usize total = scan_all(table, ids, 1, bufs, &ts);
    ASSERT_EQ_U64(total, N, "scanned 2200 rows");
    ASSERT_TRUE(memcmp(out, data, N * sizeof(i32)) == 0, "byte-identical across segment boundary");

    free(out);
    free(data);
    free(c.columns);
}

/* ============================================================
 * E. datatable_scan — empty / single
 * ============================================================ */

/* E1. Empty table: scan returns false immediately */
static void test_scan_empty_table(void)
{
    printf("\n--- E1. test_scan_empty_table ---\n");

    TypeID types[] = {TYPE_INT32};
    DataTable* table = Datatable_create(NULL, "s", "t", 1, types);

    ScanState st;
    datatable_init_scan(table, &st);

    usize ts = sizeof(i32);
    DataChunk out = alloc_output(1, &ts);
    usize ids[] = {0};
    bool got = datatable_scan(table, &st, &out, ids, 1);
    ASSERT_TRUE(!got, "empty table: scan returns false");

    free_output(&out);
    free(st.columns);
}

/* E2. Scan single row */
static void test_scan_single_row(void)
{
    printf("\n--- E2. test_scan_single_row ---\n");

    TypeID types[] = {TYPE_INT64};
    DataTable* table = Datatable_create(NULL, "s", "t", 1, types);

    i64* buf = malloc(sizeof(i64));
    buf[0] = 999999LL;
    DataChunk c;
    DataChunk_init_compat(&c, 1);
    c.columns[0] = (VectorBase){.type = TYPE_INT64, .count = 1, .data = (data_ptr_t)buf};
    datatable_append(table, &c);

    ScanState st;
    datatable_init_scan(table, &st);
    usize ts = sizeof(i64);
    DataChunk out = alloc_output(1, &ts);
    usize ids[] = {0};

    bool got = datatable_scan(table, &st, &out, ids, 1);
    ASSERT_TRUE(got, "got data");
    ASSERT_EQ_U64(out.columns[0].count, 1, "count == 1");
    ASSERT_TRUE(*(i64*)out.columns[0].data == 999999LL, "value correct");

    bool got2 = datatable_scan(table, &st, &out, ids, 1);
    ASSERT_TRUE(!got2, "second scan returns false (exhausted)");

    free_output(&out);
    free(st.columns);
    free(buf);
    free(c.columns);
}

/* ============================================================
 * F. datatable_scan — batch sizes / multi-chunk
 * ============================================================ */

/* F1. Scan exactly STORAGE_CHUNK_SIZE → batches of STANDARD_VECTOR_SIZE */
static void test_scan_exact_one_batch(void)
{
    printf("\n--- F1. test_scan_exact_one_batch ---\n");

    TypeID types[] = {TYPE_INT32};
    DataTable* table = Datatable_create(NULL, "s", "t", 1, types);

    i32* data = malloc(STORAGE_CHUNK_SIZE * sizeof(i32));
    for (usize i = 0; i < STORAGE_CHUNK_SIZE; i++) data[i] = (i32)i;
    DataChunk c;
    DataChunk_init_compat(&c, 1);
    c.columns[0] =
        (VectorBase){.type = TYPE_INT32, .count = STORAGE_CHUNK_SIZE, .data = (data_ptr_t)data};
    datatable_append(table, &c);

    ScanState st;
    datatable_init_scan(table, &st);
    usize ts = sizeof(i32);
    DataChunk out = alloc_output(1, &ts);
    usize ids[] = {0};

    int batches = 0;
    while (datatable_scan(table, &st, &out, ids, 1))
    {
        ASSERT_EQ_U64(out.columns[0].count, STANDARD_VECTOR_SIZE, "batch size == 50");
        batches++;
    }
    ASSERT_EQ_U64(batches, STORAGE_CHUNK_SIZE / STANDARD_VECTOR_SIZE, "exactly 5 batches");

    free_output(&out);
    free(st.columns);
    free(data);
    free(c.columns);
}

/* F2. 625 rows → 13 batches (12×50 + 1×25) */
static void test_scan_batch_sizes(void)
{
    printf("\n--- F2. test_scan_batch_sizes (625 rows → 12×50+25) ---\n");
    const usize N = 625;

    TypeID types[] = {TYPE_INT32};
    DataTable* table = Datatable_create(NULL, "s", "t", 1, types);

    i32* data = malloc(N * sizeof(i32));
    for (usize i = 0; i < N; i++) data[i] = (i32)i;
    DataChunk c;
    DataChunk_init_compat(&c, 1);
    c.columns[0] = (VectorBase){.type = TYPE_INT32, .count = N, .data = (data_ptr_t)data};
    datatable_append(table, &c);

    ScanState st;
    datatable_init_scan(table, &st);
    usize ts = sizeof(i32);
    DataChunk out = alloc_output(1, &ts);
    usize ids[] = {0};

    /* With STANDARD_VECTOR_SIZE=50: 625 rows = 12 batches of 50 + 1 batch of 25 */
    const int expected_batches = (int)((N + STANDARD_VECTOR_SIZE - 1) / STANDARD_VECTOR_SIZE);
    int batches = 0;
    usize total_rows = 0;
    while (datatable_scan(table, &st, &out, ids, 1))
    {
        usize expected_size =
            (total_rows + STANDARD_VECTOR_SIZE <= N) ? STANDARD_VECTOR_SIZE : (N - total_rows);
        char msg[80];
        snprintf(msg, sizeof(msg), "batch %d size == %lu", batches, (unsigned long)expected_size);
        ASSERT_EQ_U64(out.columns[0].count, expected_size, msg);
        total_rows += out.columns[0].count;
        batches++;
    }
    ASSERT_EQ_U64(batches, expected_batches, "13 batches total");

    free_output(&out);
    free(st.columns);
    free(data);
    free(c.columns);
}

/* ============================================================
 * G. datatable_scan — all four TypeIDs
 * ============================================================ */

/* G1. INT32 only roundtrip */
static void test_scan_type_int32(void)
{
    printf("\n--- G1. test_scan_type_int32 ---\n");
    const usize N = 100;
    TypeID types[] = {TYPE_INT32};
    DataTable* table = Datatable_create(NULL, "s", "t", 1, types);

    i32* data = malloc(N * sizeof(i32));
    for (usize i = 0; i < N; i++) data[i] = (i32)(i - 50);
    DataChunk c;
    DataChunk_init_compat(&c, 1);
    c.columns[0] = (VectorBase){.type = TYPE_INT32, .count = N, .data = (data_ptr_t)data};
    datatable_append(table, &c);

    usize ids[] = {0};
    usize ts = sizeof(i32);
    i32 out[100];
    void* bufs[] = {out};
    usize total = scan_all(table, ids, 1, bufs, &ts);
    ASSERT_EQ_U64(total, N, "total == 100");
    ASSERT_TRUE(memcmp(out, data, N * sizeof(i32)) == 0, "INT32 roundtrip OK");
    free(data);
    free(c.columns);
}

/* G2. INT64 only roundtrip */
static void test_scan_type_int64(void)
{
    printf("\n--- G2. test_scan_type_int64 ---\n");
    const usize N = 100;
    TypeID types[] = {TYPE_INT64};
    DataTable* table = Datatable_create(NULL, "s", "t", 1, types);

    i64* data = malloc(N * sizeof(i64));
    for (usize i = 0; i < N; i++) data[i] = (i64)i * 100000LL;
    DataChunk c;
    DataChunk_init_compat(&c, 1);
    c.columns[0] = (VectorBase){.type = TYPE_INT64, .count = N, .data = (data_ptr_t)data};
    datatable_append(table, &c);

    usize ids[] = {0};
    usize ts = sizeof(i64);
    i64 out[100];
    void* bufs[] = {out};
    usize total = scan_all(table, ids, 1, bufs, &ts);
    ASSERT_EQ_U64(total, N, "total == 100");
    ASSERT_TRUE(memcmp(out, data, N * sizeof(i64)) == 0, "INT64 roundtrip OK");
    free(data);
    free(c.columns);
}

/* G3. FLOAT32 only roundtrip */
static void test_scan_type_float32(void)
{
    printf("\n--- G3. test_scan_type_float32 ---\n");
    const usize N = 100;
    TypeID types[] = {TYPE_FLOAT32};
    DataTable* table = Datatable_create(NULL, "s", "t", 1, types);

    f32* data = malloc(N * sizeof(f32));
    for (usize i = 0; i < N; i++) data[i] = (f32)i * 0.123f;
    DataChunk c;
    DataChunk_init_compat(&c, 1);
    c.columns[0] = (VectorBase){.type = TYPE_FLOAT32, .count = N, .data = (data_ptr_t)data};
    datatable_append(table, &c);

    usize ids[] = {0};
    usize ts = sizeof(f32);
    f32 out[100];
    void* bufs[] = {out};
    usize total = scan_all(table, ids, 1, bufs, &ts);
    ASSERT_EQ_U64(total, N, "total == 100");
    ASSERT_TRUE(memcmp(out, data, N * sizeof(f32)) == 0, "FLOAT32 roundtrip OK");
    free(data);
    free(c.columns);
}

/* G4. FLOAT64 only roundtrip */
static void test_scan_type_float64(void)
{
    printf("\n--- G4. test_scan_type_float64 ---\n");
    const usize N = 100;
    TypeID types[] = {TYPE_FLOAT64};
    DataTable* table = Datatable_create(NULL, "s", "t", 1, types);

    f64* data = malloc(N * sizeof(f64));
    for (usize i = 0; i < N; i++) data[i] = (f64)i * 1.23456789;
    DataChunk c;
    DataChunk_init_compat(&c, 1);
    c.columns[0] = (VectorBase){.type = TYPE_FLOAT64, .count = N, .data = (data_ptr_t)data};
    datatable_append(table, &c);

    usize ids[] = {0};
    usize ts = sizeof(f64);
    f64 out[100];
    void* bufs[] = {out};
    usize total = scan_all(table, ids, 1, bufs, &ts);
    ASSERT_EQ_U64(total, N, "total == 100");
    ASSERT_TRUE(memcmp(out, data, N * sizeof(f64)) == 0, "FLOAT64 roundtrip OK");
    free(data);
    free(c.columns);
}

/* G5. All four types in one table */
static void test_scan_all_four_types(void)
{
    printf("\n--- G5. test_scan_all_four_types ---\n");
    const usize N = 80;

    TypeID types[] = {TYPE_INT32, TYPE_INT64, TYPE_FLOAT32, TYPE_FLOAT64};
    DataTable* table = Datatable_create(NULL, "s", "t", 4, types);

    i32* d0 = malloc(N * sizeof(i32));
    i64* d1 = malloc(N * sizeof(i64));
    f32* d2 = malloc(N * sizeof(f32));
    f64* d3 = malloc(N * sizeof(f64));
    for (usize i = 0; i < N; i++)
    {
        d0[i] = (i32)i;
        d1[i] = (i64)(i * 100);
        d2[i] = (f32)(i * 0.5f);
        d3[i] = (f64)(i * 1.1);
    }

    DataChunk c;
    DataChunk_init_compat(&c, 4);
    c.columns[0] = (VectorBase){.type = TYPE_INT32, .count = N, .data = (data_ptr_t)d0};
    c.columns[1] = (VectorBase){.type = TYPE_INT64, .count = N, .data = (data_ptr_t)d1};
    c.columns[2] = (VectorBase){.type = TYPE_FLOAT32, .count = N, .data = (data_ptr_t)d2};
    c.columns[3] = (VectorBase){.type = TYPE_FLOAT64, .count = N, .data = (data_ptr_t)d3};
    datatable_append(table, &c);

    usize ids[] = {0, 1, 2, 3};
    usize ts[] = {sizeof(i32), sizeof(i64), sizeof(f32), sizeof(f64)};
    i32 o0[80];
    i64 o1[80];
    f32 o2[80];
    f64 o3[80];
    void* bufs[] = {o0, o1, o2, o3};
    usize total = scan_all(table, ids, 4, bufs, ts);
    ASSERT_EQ_U64(total, N, "total == 80");
    ASSERT_TRUE(memcmp(o0, d0, N * sizeof(i32)) == 0, "INT32 correct");
    ASSERT_TRUE(memcmp(o1, d1, N * sizeof(i64)) == 0, "INT64 correct");
    ASSERT_TRUE(memcmp(o2, d2, N * sizeof(f32)) == 0, "FLOAT32 correct");
    ASSERT_TRUE(memcmp(o3, d3, N * sizeof(f64)) == 0, "FLOAT64 correct");

    free(d0);
    free(d1);
    free(d2);
    free(d3);
    free(c.columns);
}

/* ============================================================
 * H. datatable_scan — column projection
 * ============================================================ */

/* H1. 3-column table, project only first column */
static void test_scan_project_first_col(void)
{
    printf("\n--- H1. test_scan_project_first_col ---\n");
    const usize N = 60;
    TypeID types[] = {TYPE_INT32, TYPE_INT64, TYPE_FLOAT64};
    DataTable* table = Datatable_create(NULL, "s", "t", 3, types);

    i32* d0 = malloc(N * sizeof(i32));
    i64* d1 = malloc(N * sizeof(i64));
    f64* d2 = malloc(N * sizeof(f64));
    for (usize i = 0; i < N; i++)
    {
        d0[i] = (i32)i;
        d1[i] = 0;
        d2[i] = 0.0;
    }

    DataChunk c;
    DataChunk_init_compat(&c, 3);
    c.columns[0] = (VectorBase){.type = TYPE_INT32, .count = N, .data = (data_ptr_t)d0};
    c.columns[1] = (VectorBase){.type = TYPE_INT64, .count = N, .data = (data_ptr_t)d1};
    c.columns[2] = (VectorBase){.type = TYPE_FLOAT64, .count = N, .data = (data_ptr_t)d2};
    datatable_append(table, &c);

    usize ids[] = {0};
    usize ts = sizeof(i32);
    i32 out[60];
    void* bufs[] = {out};
    usize total = scan_all(table, ids, 1, bufs, &ts);
    ASSERT_EQ_U64(total, N, "total == 60");
    ASSERT_TRUE(memcmp(out, d0, N * sizeof(i32)) == 0, "projected col 0 correct");

    free(d0);
    free(d1);
    free(d2);
    free(c.columns);
}

/* H2. 3-column table, project only last column */
static void test_scan_project_last_col(void)
{
    printf("\n--- H2. test_scan_project_last_col ---\n");
    const usize N = 60;
    TypeID types[] = {TYPE_INT32, TYPE_INT64, TYPE_FLOAT64};
    DataTable* table = Datatable_create(NULL, "s", "t", 3, types);

    i32* d0 = malloc(N * sizeof(i32));
    i64* d1 = malloc(N * sizeof(i64));
    f64* d2 = malloc(N * sizeof(f64));
    for (usize i = 0; i < N; i++)
    {
        d0[i] = 0;
        d1[i] = 0;
        d2[i] = (f64)(i * 3.14);
    }

    DataChunk c;
    DataChunk_init_compat(&c, 3);
    c.columns[0] = (VectorBase){.type = TYPE_INT32, .count = N, .data = (data_ptr_t)d0};
    c.columns[1] = (VectorBase){.type = TYPE_INT64, .count = N, .data = (data_ptr_t)d1};
    c.columns[2] = (VectorBase){.type = TYPE_FLOAT64, .count = N, .data = (data_ptr_t)d2};
    datatable_append(table, &c);

    usize ids[] = {2};
    usize ts = sizeof(f64);
    f64 out[60];
    void* bufs[] = {out};
    usize total = scan_all(table, ids, 1, bufs, &ts);
    ASSERT_EQ_U64(total, N, "total == 60");
    ASSERT_TRUE(memcmp(out, d2, N * sizeof(f64)) == 0, "projected col 2 correct");

    free(d0);
    free(d1);
    free(d2);
    free(c.columns);
}

/* H3. Reverse column order */
static void test_scan_project_reverse_order(void)
{
    printf("\n--- H3. test_scan_project_reverse_order ---\n");
    const usize N = 50;
    TypeID types[] = {TYPE_INT32, TYPE_INT64, TYPE_FLOAT32};
    DataTable* table = Datatable_create(NULL, "s", "t", 3, types);

    i32* d0 = malloc(N * sizeof(i32));
    i64* d1 = malloc(N * sizeof(i64));
    f32* d2 = malloc(N * sizeof(f32));
    for (usize i = 0; i < N; i++)
    {
        d0[i] = (i32)(i + 1);
        d1[i] = (i64)(i + 100);
        d2[i] = (f32)(i + 200);
    }

    DataChunk c;
    DataChunk_init_compat(&c, 3);
    c.columns[0] = (VectorBase){.type = TYPE_INT32, .count = N, .data = (data_ptr_t)d0};
    c.columns[1] = (VectorBase){.type = TYPE_INT64, .count = N, .data = (data_ptr_t)d1};
    c.columns[2] = (VectorBase){.type = TYPE_FLOAT32, .count = N, .data = (data_ptr_t)d2};
    datatable_append(table, &c);

    /* scan columns in reverse: 2, 1, 0 */
    usize ids[] = {2, 1, 0};
    usize ts[] = {sizeof(f32), sizeof(i64), sizeof(i32)};
    f32 o0[50];
    i64 o1[50];
    i32 o2[50];
    void* bufs[] = {o0, o1, o2};
    usize total = scan_all(table, ids, 3, bufs, ts);
    ASSERT_EQ_U64(total, N, "total == 50");
    ASSERT_TRUE(memcmp(o0, d2, N * sizeof(f32)) == 0, "reverse[0] = col2 correct");
    ASSERT_TRUE(memcmp(o1, d1, N * sizeof(i64)) == 0, "reverse[1] = col1 correct");
    ASSERT_TRUE(memcmp(o2, d0, N * sizeof(i32)) == 0, "reverse[2] = col0 correct");

    free(d0);
    free(d1);
    free(d2);
    free(c.columns);
}

/* H4. Non-contiguous projection: 4 cols, project cols 0 and 3 */
static void test_scan_project_non_contiguous(void)
{
    printf("\n--- H4. test_scan_project_non_contiguous (cols 0,3 of 4) ---\n");
    const usize N = 70;
    TypeID types[] = {TYPE_INT32, TYPE_INT64, TYPE_FLOAT32, TYPE_FLOAT64};
    DataTable* table = Datatable_create(NULL, "s", "t", 4, types);

    i32* d0 = malloc(N * sizeof(i32));
    i64* d1 = malloc(N * sizeof(i64));
    f32* d2 = malloc(N * sizeof(f32));
    f64* d3 = malloc(N * sizeof(f64));
    for (usize i = 0; i < N; i++)
    {
        d0[i] = (i32)(i * 11);
        d1[i] = 0;
        d2[i] = 0.0f;
        d3[i] = (f64)(i * 22.0);
    }

    DataChunk c;
    DataChunk_init_compat(&c, 4);
    c.columns[0] = (VectorBase){.type = TYPE_INT32, .count = N, .data = (data_ptr_t)d0};
    c.columns[1] = (VectorBase){.type = TYPE_INT64, .count = N, .data = (data_ptr_t)d1};
    c.columns[2] = (VectorBase){.type = TYPE_FLOAT32, .count = N, .data = (data_ptr_t)d2};
    c.columns[3] = (VectorBase){.type = TYPE_FLOAT64, .count = N, .data = (data_ptr_t)d3};
    datatable_append(table, &c);

    usize ids[] = {0, 3};
    usize ts[] = {sizeof(i32), sizeof(f64)};
    i32 o0[70];
    f64 o3[70];
    void* bufs[] = {o0, o3};
    usize total = scan_all(table, ids, 2, bufs, ts);
    ASSERT_EQ_U64(total, N, "total == 70");
    ASSERT_TRUE(memcmp(o0, d0, N * sizeof(i32)) == 0, "col 0 correct");
    ASSERT_TRUE(memcmp(o3, d3, N * sizeof(f64)) == 0, "col 3 correct");

    free(d0);
    free(d1);
    free(d2);
    free(d3);
    free(c.columns);
}

/* ============================================================
 * I. datatable_scan — ColumnSegment boundary crossing during scan
 * ============================================================ */

/* I1. INT64: scan 1300 rows across ColumnSegment boundary (at element 1024) */
static void test_scan_cross_column_segment_i64(void)
{
    printf("\n--- I1. test_scan_cross_column_segment_i64 (1300 rows) ---\n");
    const usize N = 1300;
    TypeID types[] = {TYPE_INT64};
    DataTable* table = Datatable_create(NULL, "s", "t", 1, types);

    i64* data = malloc(N * sizeof(i64));
    for (usize i = 0; i < N; i++) data[i] = (i64)(i * 97);
    DataChunk c;
    DataChunk_init_compat(&c, 1);
    c.columns[0] = (VectorBase){.type = TYPE_INT64, .count = N, .data = (data_ptr_t)data};
    datatable_append(table, &c);

    /* scan in batches — one of them straddles the ColumnSegment boundary.
     * RowSegments: [0-249], [250-499], [500-749], [750-999], [1000-1249], [1250-1299]
     * ColumnSegment boundary at element 1024: falls inside [1000-1249] batch. */
    ScanState st;
    datatable_init_scan(table, &st);
    usize ts = sizeof(i64);
    DataChunk out = alloc_output(1, &ts);
    usize ids[] = {0};

    usize total = 0;
    bool all_ok = true;
    while (datatable_scan(table, &st, &out, ids, 1))
    {
        i64* row = (i64*)out.columns[0].data;
        for (usize i = 0; i < out.columns[0].count; i++)
        {
            if (row[i] != (i64)((total + i) * 97))
            {
                printf("    mismatch at row %lu\n", (unsigned long)(total + i));
                all_ok = false;
                break;
            }
        }
        total += out.columns[0].count;
        if (!all_ok) break;
    }
    ASSERT_TRUE(all_ok, "all values correct across ColumnSegment boundary");
    ASSERT_EQ_U64(total, N, "total scanned == 1300");

    free_output(&out);
    free(st.columns);
    free(data);
    free(c.columns);
}

/* I2. INT32: scan 2200 rows across ColumnSegment boundary (at element 2048) */
static void test_scan_cross_column_segment_i32(void)
{
    printf("\n--- I2. test_scan_cross_column_segment_i32 (2200 rows) ---\n");
    const usize N = 2200;
    TypeID types[] = {TYPE_INT32};
    DataTable* table = Datatable_create(NULL, "s", "t", 1, types);

    i32* data = malloc(N * sizeof(i32));
    for (usize i = 0; i < N; i++) data[i] = (i32)(N - i);
    DataChunk c;
    DataChunk_init_compat(&c, 1);
    c.columns[0] = (VectorBase){.type = TYPE_INT32, .count = N, .data = (data_ptr_t)data};
    datatable_append(table, &c);

    usize ids[] = {0};
    usize ts = sizeof(i32);
    i32* out = malloc(N * sizeof(i32));
    void* bufs[] = {out};
    usize total = scan_all(table, ids, 1, bufs, &ts);
    ASSERT_EQ_U64(total, N, "total scanned == 2200");
    ASSERT_TRUE(memcmp(out, data, N * sizeof(i32)) == 0, "byte-identical (i32 seg boundary)");

    free(out);
    free(data);
    free(c.columns);
}

/* I3. Multi-column: mixed sizes cross boundaries at different points
 *     INT64 boundary at 1024, INT32 boundary at 2048
 *     2100 rows forces INT64 to use 3 segments, INT32 still in 2 */
static void test_scan_cross_column_segment_mixed(void)
{
    printf("\n--- I3. test_scan_cross_column_segment_mixed (2100 rows, i32+i64) ---\n");
    const usize N = 2100;
    TypeID types[] = {TYPE_INT32, TYPE_INT64};
    DataTable* table = Datatable_create(NULL, "s", "t", 2, types);

    i32* d0 = malloc(N * sizeof(i32));
    i64* d1 = malloc(N * sizeof(i64));
    for (usize i = 0; i < N; i++)
    {
        d0[i] = (i32)i;
        d1[i] = (i64)(i * 3);
    }
    DataChunk c;
    DataChunk_init_compat(&c, 2);
    c.columns[0] = (VectorBase){.type = TYPE_INT32, .count = N, .data = (data_ptr_t)d0};
    c.columns[1] = (VectorBase){.type = TYPE_INT64, .count = N, .data = (data_ptr_t)d1};
    datatable_append(table, &c);

    /* verify segment counts */
    ColumnSegment* cs0 =
        (ColumnSegment*)segmentTree_get_root_segment(&table->column_storage_tree[0]);
    ASSERT_EQ_U64(cs0->base.count, COL_SEG_CAP_I32, "i32 seg0 == 2048");
    ASSERT_TRUE(cs0->base.next != NULL, "i32 has 2nd segment");
    ColumnSegment* cs0b = (ColumnSegment*)cs0->base.next;
    ASSERT_EQ_U64(cs0b->base.count, N - COL_SEG_CAP_I32, "i32 seg1 == 52");

    ColumnSegment* cs1 =
        (ColumnSegment*)segmentTree_get_root_segment(&table->column_storage_tree[1]);
    ASSERT_EQ_U64(cs1->base.count, COL_SEG_CAP_I64, "i64 seg0 == 1024");
    ColumnSegment* cs1b = (ColumnSegment*)cs1->base.next;
    ASSERT_TRUE(cs1b != NULL, "i64 has 2nd segment");
    ASSERT_EQ_U64(cs1b->base.count, COL_SEG_CAP_I64, "i64 seg1 == 1024");
    ColumnSegment* cs1c = (ColumnSegment*)cs1b->base.next;
    ASSERT_TRUE(cs1c != NULL, "i64 has 3rd segment");
    ASSERT_EQ_U64(cs1c->base.count, N - 2 * COL_SEG_CAP_I64, "i64 seg2 == 52");

    /* scan and verify */
    usize ids[] = {0, 1};
    usize ts[] = {sizeof(i32), sizeof(i64)};
    i32* o0 = malloc(N * sizeof(i32));
    i64* o1 = malloc(N * sizeof(i64));
    void* bufs[] = {o0, o1};
    usize total = scan_all(table, ids, 2, bufs, ts);
    ASSERT_EQ_U64(total, N, "scanned 2100");
    ASSERT_TRUE(memcmp(o0, d0, N * sizeof(i32)) == 0, "i32 byte-identical");
    ASSERT_TRUE(memcmp(o1, d1, N * sizeof(i64)) == 0, "i64 byte-identical");

    free(o0);
    free(o1);
    free(d0);
    free(d1);
    free(c.columns);
}

/* ============================================================
 * J. datatable_scan — state / re-scan / exhaustion
 * ============================================================ */

/* J1. Re-init and re-scan: two passes produce identical data */
static void test_scan_rescan(void)
{
    printf("\n--- J1. test_scan_rescan (two passes identical) ---\n");
    const usize N = 300;
    TypeID types[] = {TYPE_INT32};
    DataTable* table = Datatable_create(NULL, "s", "t", 1, types);

    i32* data = malloc(N * sizeof(i32));
    for (usize i = 0; i < N; i++) data[i] = (i32)(i * 13);
    DataChunk c;
    DataChunk_init_compat(&c, 1);
    c.columns[0] = (VectorBase){.type = TYPE_INT32, .count = N, .data = (data_ptr_t)data};
    datatable_append(table, &c);

    usize ids[] = {0};
    usize ts = sizeof(i32);

    i32* pass1 = malloc(N * sizeof(i32));
    void* bufs1[] = {pass1};
    usize t1 = scan_all(table, ids, 1, bufs1, &ts);

    i32* pass2 = malloc(N * sizeof(i32));
    void* bufs2[] = {pass2};
    usize t2 = scan_all(table, ids, 1, bufs2, &ts);

    ASSERT_EQ_U64(t1, N, "pass1 total");
    ASSERT_EQ_U64(t2, N, "pass2 total");
    ASSERT_TRUE(memcmp(pass1, pass2, N * sizeof(i32)) == 0, "two passes identical");

    free(pass1);
    free(pass2);
    free(data);
    free(c.columns);
}

/* J2. After exhaustion, repeated scan returns false */
static void test_scan_after_exhaustion(void)
{
    printf("\n--- J2. test_scan_after_exhaustion ---\n");
    TypeID types[] = {TYPE_INT32};
    DataTable* table = Datatable_create(NULL, "s", "t", 1, types);

    i32* data = malloc(10 * sizeof(i32));
    for (int i = 0; i < 10; i++) data[i] = i;
    DataChunk c;
    DataChunk_init_compat(&c, 1);
    c.columns[0] = (VectorBase){.type = TYPE_INT32, .count = 10, .data = (data_ptr_t)data};
    datatable_append(table, &c);

    ScanState st;
    datatable_init_scan(table, &st);
    usize ts = sizeof(i32);
    DataChunk out = alloc_output(1, &ts);
    usize ids[] = {0};

    /* exhaust */
    while (datatable_scan(table, &st, &out, ids, 1))
    {
    }

    /* call again — must be false */
    bool again1 = datatable_scan(table, &st, &out, ids, 1);
    bool again2 = datatable_scan(table, &st, &out, ids, 1);
    ASSERT_TRUE(!again1, "1st call after exhaust: false");
    ASSERT_TRUE(!again2, "2nd call after exhaust: false");

    free_output(&out);
    free(st.columns);
    free(data);
    free(c.columns);
}

/* J3. init_scan state fields are correct */
static void test_scan_state_init(void)
{
    printf("\n--- J3. test_scan_state_init ---\n");
    TypeID types[] = {TYPE_INT32, TYPE_INT64};
    DataTable* table = Datatable_create(NULL, "s", "t", 2, types);

    i32* d0 = malloc(100 * sizeof(i32));
    i64* d1 = malloc(100 * sizeof(i64));
    for (int i = 0; i < 100; i++)
    {
        d0[i] = i;
        d1[i] = i;
    }
    DataChunk c;
    DataChunk_init_compat(&c, 2);
    c.columns[0] = (VectorBase){.type = TYPE_INT32, .count = 100, .data = (data_ptr_t)d0};
    c.columns[1] = (VectorBase){.type = TYPE_INT64, .count = 100, .data = (data_ptr_t)d1};
    datatable_append(table, &c);

    ScanState st;
    datatable_init_scan(table, &st);

    ASSERT_TRUE(st.current_chunk != NULL, "current_chunk set");
    ASSERT_TRUE(st.last_chunk != NULL, "last_chunk set");
    ASSERT_EQ_U64(st.last_chunk_count, 100, "last_chunk_count == 100");
    ASSERT_EQ_U64(st.offset, 0, "offset == 0");
    ASSERT_TRUE(st.columns != NULL, "columns allocated");
    ASSERT_TRUE(st.columns[0].segment != NULL, "col0 segment set");
    ASSERT_TRUE(st.columns[1].segment != NULL, "col1 segment set");
    ASSERT_EQ_U64(st.columns[0].bytes_offset, 0, "col0 bytes_offset == 0");
    ASSERT_EQ_U64(st.columns[1].bytes_offset, 0, "col1 bytes_offset == 0");

    free(st.columns);
    free(d0);
    free(d1);
    free(c.columns);
}

/* ============================================================
 * K. DataChunk / VectorBase helpers
 * ============================================================ */

/* K1. DataChunk_init, dataChunk_size */
static void test_datachunk_helpers(void)
{
    printf("\n--- K1. test_datachunk_helpers ---\n");

    DataChunk chunk;
    DataChunk_init_compat(&chunk, 3);
    ASSERT_EQ_U64(chunk.count, 3, "column_count == 3");
    ASSERT_TRUE(chunk.columns != NULL, "columns allocated");

    /* size determined by first column's count */
    chunk.arrays[0].count = 42;
    chunk.arrays[1].count = 99;
    chunk.arrays[2].count = 0;
    ASSERT_EQ_U64(dataChunk_size(&chunk), 42, "dataChunk_size == col[0].count");

    /* dataChunk_clear zeros all counts */
    dataChunk_clear(&chunk);
    ASSERT_EQ_U64(chunk.arrays[0].count, 0, "after clear: col0.count == 0");
    ASSERT_EQ_U64(chunk.arrays[1].count, 0, "after clear: col1.count == 0");
    ASSERT_EQ_U64(chunk.arrays[2].count, 0, "after clear: col2.count == 0");

    free(chunk.columns);
}

/* K2. dataChunk_size on zero-column chunk */
static void test_datachunk_size_zero_cols(void)
{
    printf("\n--- K2. test_datachunk_size_zero_cols ---\n");
    DataChunk chunk;
    chunk.count = 0;
    chunk.columns = NULL;
    ASSERT_EQ_U64(dataChunk_size(&chunk), 0, "0-column chunk size == 0");
}

/* K3. VectorBase_from_vector */
static void test_column_vector_from_vector(void)
{
    printf("\n--- K3. test_column_vector_from_vector ---\n");
    Vector v;
    Vector_init(&v, sizeof(i32), 4);
    for (int i = 0; i < 3; i++)
    {
        i32 val = i * 10;
        vector_push_back(&v, &val);
    }

    VectorBase cv;
    VectorBase_from_vector(&cv, v, TYPE_INT32);

    ASSERT_TRUE(cv.type == TYPE_INT32, "type == INT32");
    ASSERT_EQ_U64(cv.count, 3, "count == 3");
    ASSERT_TRUE(cv.data == v.data, "data pointer shared");

    i32* arr = (i32*)cv.data;
    ASSERT_TRUE(arr[0] == 0 && arr[1] == 10 && arr[2] == 20, "values correct");

    /* cv.data == v.data, free once */
    vector_deinit(&v);
}

/* K4. dataChunk_append */
static void test_datachunk_append(void)
{
    printf("\n--- K4. test_datachunk_append ---\n");
    DataChunk chunk;
    DataChunk_init_compat(&chunk, 2);

    i32 buf1[] = {1, 2, 3};
    VectorBase cv1 = {.type = TYPE_INT32, .count = 3, .data = (data_ptr_t)buf1};
    dataChunk_append(&chunk, 0, cv1);
    ASSERT_TRUE(chunk.arrays[0].count == 3, "col 0 count == 3");
    ASSERT_TRUE(chunk.arrays[0].data == (data_ptr_t)buf1, "col 0 data ptr");

    i64 buf2[] = {10, 20};
    VectorBase cv2 = {.type = TYPE_INT64, .count = 2, .data = (data_ptr_t)buf2};
    dataChunk_append(&chunk, 1, cv2);
    ASSERT_TRUE(chunk.arrays[1].count == 2, "col 1 count == 2");

    free(chunk.columns);
}

/* ============================================================
 * L. Combined append + scan workflows
 * ============================================================ */

/* L1. Multiple small appends, then full scan */
static void test_combined_multi_append_full_scan(void)
{
    printf("\n--- L1. test_combined_multi_append_full_scan ---\n");
    TypeID types[] = {TYPE_INT32, TYPE_INT64};
    DataTable* table = Datatable_create(NULL, "s", "t", 2, types);

    const int NAPPENDS = 7;
    const int ROWS_PER = 50; /* 7 * 50 = 350 rows total → 2 RowSegments */
    i32 expect_i32[350];
    i64 expect_i64[350];

    for (int a = 0; a < NAPPENDS; a++)
    {
        i32* d0 = malloc(ROWS_PER * sizeof(i32));
        i64* d1 = malloc(ROWS_PER * sizeof(i64));
        for (int j = 0; j < ROWS_PER; j++)
        {
            d0[j] = a * ROWS_PER + j;
            d1[j] = (a * ROWS_PER + j) * 100LL;
            expect_i32[a * ROWS_PER + j] = d0[j];
            expect_i64[a * ROWS_PER + j] = d1[j];
        }
        DataChunk c;
        DataChunk_init_compat(&c, 2);
        c.columns[0] = (VectorBase){.type = TYPE_INT32, .count = ROWS_PER, .data = (data_ptr_t)d0};
        c.columns[1] = (VectorBase){.type = TYPE_INT64, .count = ROWS_PER, .data = (data_ptr_t)d1};
        datatable_append(table, &c);
        free(d0);
        free(d1);
        free(c.columns);
    }

    usize ids[] = {0, 1};
    usize ts[] = {sizeof(i32), sizeof(i64)};
    i32 out0[350];
    i64 out1[350];
    void* bufs[] = {out0, out1};
    usize total = scan_all(table, ids, 2, bufs, ts);
    ASSERT_EQ_U64(total, 350, "total == 350");
    ASSERT_TRUE(memcmp(out0, expect_i32, 350 * sizeof(i32)) == 0, "i32 correct");
    ASSERT_TRUE(memcmp(out1, expect_i64, 350 * sizeof(i64)) == 0, "i64 correct");
}

/* L2. Append partial, scan, append more, re-scan all */
static void test_combined_append_scan_append_rescan(void)
{
    printf("\n--- L2. test_combined_append_scan_append_rescan ---\n");
    TypeID types[] = {TYPE_INT32};
    DataTable* table = Datatable_create(NULL, "s", "t", 1, types);

    /* First append: 100 rows */
    i32* d1 = malloc(100 * sizeof(i32));
    for (int i = 0; i < 100; i++) d1[i] = i;
    DataChunk c1;
    DataChunk_init_compat(&c1, 1);
    c1.columns[0] = (VectorBase){.type = TYPE_INT32, .count = 100, .data = (data_ptr_t)d1};
    datatable_append(table, &c1);

    /* Scan #1: should see 100 rows */
    usize ids[] = {0};
    usize ts = sizeof(i32);
    i32 out1[100];
    void* bufs1[] = {out1};
    usize t1 = scan_all(table, ids, 1, bufs1, &ts);
    ASSERT_EQ_U64(t1, 100, "scan #1: 100 rows");
    ASSERT_TRUE(memcmp(out1, d1, 100 * sizeof(i32)) == 0, "scan #1 correct");

    /* Second append: 200 more rows (total 300, crosses RowSegment at 250) */
    i32* d2 = malloc(200 * sizeof(i32));
    for (int i = 0; i < 200; i++) d2[i] = 100 + i;
    DataChunk c2;
    DataChunk_init_compat(&c2, 1);
    c2.columns[0] = (VectorBase){.type = TYPE_INT32, .count = 200, .data = (data_ptr_t)d2};
    datatable_append(table, &c2);

    /* Scan #2: should see all 300 rows */
    i32 out2[300];
    void* bufs2[] = {out2};
    usize t2 = scan_all(table, ids, 1, bufs2, &ts);
    ASSERT_EQ_U64(t2, 300, "scan #2: 300 rows");

    /* build expected for all 300 */
    bool ok = true;
    for (int i = 0; i < 300; i++)
    {
        if (out2[i] != i)
        {
            ok = false;
            break;
        }
    }
    ASSERT_TRUE(ok, "scan #2 values 0..299 correct");

    free(d1);
    free(c1.columns);
    free(d2);
    free(c2.columns);
}

/* L3. Large test: 5000 rows of INT64 — exercises both RowSegment
 *     (5000/250 = 20 segments) and ColumnSegment (5000/1024 = ~5 segments)
 *     boundaries simultaneously */
static void test_combined_large_i64(void)
{
    printf("\n--- L3. test_combined_large_i64 (5000 rows) ---\n");
    const usize N = 5000;
    TypeID types[] = {TYPE_INT64};
    DataTable* table = Datatable_create(NULL, "s", "t", 1, types);

    i64* data = malloc(N * sizeof(i64));
    for (usize i = 0; i < N; i++) data[i] = (i64)(i * 31 + 17);
    DataChunk c;
    DataChunk_init_compat(&c, 1);
    c.columns[0] = (VectorBase){.type = TYPE_INT64, .count = N, .data = (data_ptr_t)data};
    datatable_append(table, &c);

    /* verify RowSegment count: ceil(5000/250) = 20 */
    usize seg_count = 0;
    SegmentBase* s = segmentTree_get_root_segment(&table->row_storage_tree);
    while (s)
    {
        seg_count++;
        s = s->next;
    }
    ASSERT_EQ_U64(seg_count, 20, "20 RowSegments");

    /* verify ColumnSegment count: ceil(5000/1024) = 5 */
    usize cseg_count = 0;
    SegmentBase* cs = segmentTree_get_root_segment(&table->column_storage_tree[0]);
    while (cs)
    {
        cseg_count++;
        cs = cs->next;
    }
    ASSERT_EQ_U64(cseg_count, 5, "5 ColumnSegments");

    /* scan and verify */
    usize ids[] = {0};
    usize ts = sizeof(i64);
    i64* out = malloc(N * sizeof(i64));
    void* bufs[] = {out};

    usize total = scan_all(table, ids, 1, bufs, &ts);
    ASSERT_EQ_U64(total, N, "total scanned == 5000");
    ASSERT_TRUE(memcmp(out, data, N * sizeof(i64)) == 0, "byte-identical");

    /* verify batch count matches RowSegment count */
    ScanState st;
    datatable_init_scan(table, &st);
    DataChunk bout = alloc_output(1, &ts);
    int batches = 0;
    while (datatable_scan(table, &st, &bout, ids, 1)) batches++;
    ASSERT_EQ_U64(batches, N / STANDARD_VECTOR_SIZE, "scan produced 100 batches");

    free_output(&bout);
    free(st.columns);
    free(out);
    free(data);
    free(c.columns);
}

/* L4. Incremental single-row appends across ColumnSegment boundary
 *     1100 single-row INT64 appends → boundary at 1024 */
static void test_combined_incremental_cross_column_seg(void)
{
    printf("\n--- L4. test_combined_incremental_cross_column_seg (1100 x 1 i64) ---\n");
    const usize N = 1100;
    TypeID types[] = {TYPE_INT64};
    DataTable* table = Datatable_create(NULL, "s", "t", 1, types);

    i64 expect[1100];
    for (usize k = 0; k < N; k++)
    {
        expect[k] = (i64)(k * k);
        i64* buf = malloc(sizeof(i64));
        buf[0] = expect[k];
        DataChunk c;
        DataChunk_init_compat(&c, 1);
        c.columns[0] = (VectorBase){.type = TYPE_INT64, .count = 1, .data = (data_ptr_t)buf};
        datatable_append(table, &c);
        free(buf);
        free(c.columns);
    }

    usize ids[] = {0};
    usize ts = sizeof(i64);
    i64* out = malloc(N * sizeof(i64));
    void* bufs[] = {out};
    usize total = scan_all(table, ids, 1, bufs, &ts);
    ASSERT_EQ_U64(total, N, "total == 1100");
    ASSERT_TRUE(memcmp(out, expect, N * sizeof(i64)) == 0, "byte-identical");

    free(out);
}

/* L5. Projection across multi-chunk with ColumnSegment boundary
 *     4 columns (all types), 1300 rows, project cols 1 and 3 only */
static void test_combined_projection_cross_boundary(void)
{
    printf("\n--- L5. test_combined_projection_cross_boundary ---\n");
    const usize N = 1300;
    TypeID types[] = {TYPE_INT32, TYPE_INT64, TYPE_FLOAT32, TYPE_FLOAT64};
    DataTable* table = Datatable_create(NULL, "s", "t", 4, types);

    i32* d0 = malloc(N * sizeof(i32));
    i64* d1 = malloc(N * sizeof(i64));
    f32* d2 = malloc(N * sizeof(f32));
    f64* d3 = malloc(N * sizeof(f64));
    for (usize i = 0; i < N; i++)
    {
        d0[i] = (i32)i;
        d1[i] = (i64)(i * 7);
        d2[i] = (f32)(i * 0.1f);
        d3[i] = (f64)(i * 0.01);
    }
    DataChunk c;
    DataChunk_init_compat(&c, 4);
    c.columns[0] = (VectorBase){.type = TYPE_INT32, .count = N, .data = (data_ptr_t)d0};
    c.columns[1] = (VectorBase){.type = TYPE_INT64, .count = N, .data = (data_ptr_t)d1};
    c.columns[2] = (VectorBase){.type = TYPE_FLOAT32, .count = N, .data = (data_ptr_t)d2};
    c.columns[3] = (VectorBase){.type = TYPE_FLOAT64, .count = N, .data = (data_ptr_t)d3};
    datatable_append(table, &c);

    /* project only cols 1 (INT64) and 3 (FLOAT64) — both 8-byte types,
     * ColumnSegment boundary at 1024 elements */
    usize ids[] = {1, 3};
    usize ts[] = {sizeof(i64), sizeof(f64)};
    i64* o1 = malloc(N * sizeof(i64));
    f64* o3 = malloc(N * sizeof(f64));
    void* bufs[] = {o1, o3};
    usize total = scan_all(table, ids, 2, bufs, ts);
    ASSERT_EQ_U64(total, N, "total == 1300");
    ASSERT_TRUE(memcmp(o1, d1, N * sizeof(i64)) == 0, "projected col1 correct");
    ASSERT_TRUE(memcmp(o3, d3, N * sizeof(f64)) == 0, "projected col3 correct");

    free(o1);
    free(o3);
    free(d0);
    free(d1);
    free(d2);
    free(d3);
    free(c.columns);
}

/* ============================================================
 * M. Large-scale multi-segment correctness
 *
 * Key boundaries exercised:
 *   STORAGE_CHUNK_SIZE = 250    → RowSegment capacity
 *   BLOCK_SIZE         = 8192   → ColumnSegment capacity
 *   INT32/FLOAT32 → 2048 elems/seg   INT64/FLOAT64 → 1024 elems/seg
 * ============================================================ */

#define COL_SEG_CAP_F32 (BLOCK_SIZE / (usize)sizeof(f32)) /* 2048 */
#define COL_SEG_CAP_F64 (BLOCK_SIZE / (usize)sizeof(f64)) /* 1024 */

/* Helper: count segments in a linked list starting from root */
static usize count_segments(SegmentBase* root)
{
    usize n = 0;
    while (root)
    {
        n++;
        root = root->next;
    }
    return n;
}

/* M1. 100K rows of INT64: 400 RowSegments x ~98 ColumnSegments
 *     Single bulk append, full scan, byte-for-byte verify */
static void test_large_100k_i64(void)
{
    printf("\n--- M1. test_large_100k_i64 (100000 rows) ---\n");
    const usize N = 100000;
    TypeID types[] = {TYPE_INT64};
    DataTable* table = Datatable_create(NULL, "s", "t", 1, types);

    i64* data = malloc(N * sizeof(i64));
    for (usize i = 0; i < N; i++) data[i] = (i64)i * 37 - 500000;

    DataChunk c;
    DataChunk_init_compat(&c, 1);
    c.columns[0] = (VectorBase){.type = TYPE_INT64, .count = N, .data = (data_ptr_t)data};
    datatable_append(table, &c);

    /* verify segment topology */
    usize row_segs = count_segments(segmentTree_get_root_segment(&table->row_storage_tree));
    usize col_segs = count_segments(segmentTree_get_root_segment(&table->column_storage_tree[0]));
    /* 100000 / 250 = 400 */
    ASSERT_EQ_U64(row_segs, 400, "400 RowSegments");
    /* ceil(100000 / 1024) = 98 */
    ASSERT_EQ_U64(col_segs, 98, "98 ColumnSegments (i64)");

    /* scan all and verify */
    usize ids[] = {0};
    usize ts = sizeof(i64);
    i64* out = malloc(N * sizeof(i64));
    void* bufs[] = {out};
    usize total = scan_all(table, ids, 1, bufs, &ts);
    ASSERT_EQ_U64(total, N, "total == 100000");
    ASSERT_TRUE(memcmp(out, data, N * sizeof(i64)) == 0, "byte-identical (100K i64)");

    free(out);
    free(data);
    free(c.columns);
    Datatable_destroy(table);
}

/* M2. 100K rows of INT32: 400 RowSegments x 49 ColumnSegments */
static void test_large_100k_i32(void)
{
    printf("\n--- M2. test_large_100k_i32 (100000 rows) ---\n");
    const usize N = 100000;
    TypeID types[] = {TYPE_INT32};
    DataTable* table = Datatable_create(NULL, "s", "t", 1, types);

    i32* data = malloc(N * sizeof(i32));
    for (usize i = 0; i < N; i++) data[i] = (i32)(N - 1 - i);

    DataChunk c;
    DataChunk_init_compat(&c, 1);
    c.columns[0] = (VectorBase){.type = TYPE_INT32, .count = N, .data = (data_ptr_t)data};
    datatable_append(table, &c);

    usize col_segs = count_segments(segmentTree_get_root_segment(&table->column_storage_tree[0]));
    /* ceil(100000 / 2048) = 49 */
    ASSERT_EQ_U64(col_segs, 49, "49 ColumnSegments (i32)");

    usize ids[] = {0};
    usize ts = sizeof(i32);
    i32* out = malloc(N * sizeof(i32));
    void* bufs[] = {out};
    usize total = scan_all(table, ids, 1, bufs, &ts);
    ASSERT_EQ_U64(total, N, "total == 100000");
    ASSERT_TRUE(memcmp(out, data, N * sizeof(i32)) == 0, "byte-identical (100K i32)");

    free(out);
    free(data);
    free(c.columns);
    Datatable_destroy(table);
}

/* M3. 50K rows x 4 columns (all types) — columns cross boundaries at different points
 *     INT32:  50000/2048 = ceil 25 segments
 *     INT64:  50000/1024 = ceil 49 segments
 *     FLOAT32:50000/2048 = ceil 25 segments
 *     FLOAT64:50000/1024 = ceil 49 segments */
static void test_large_50k_mixed_4col(void)
{
    printf("\n--- M3. test_large_50k_mixed_4col (50000 rows x 4 cols) ---\n");
    const usize N = 50000;
    TypeID types[] = {TYPE_INT32, TYPE_INT64, TYPE_FLOAT32, TYPE_FLOAT64};
    DataTable* table = Datatable_create(NULL, "s", "t", 4, types);

    i32* d0 = malloc(N * sizeof(i32));
    i64* d1 = malloc(N * sizeof(i64));
    f32* d2 = malloc(N * sizeof(f32));
    f64* d3 = malloc(N * sizeof(f64));
    for (usize i = 0; i < N; i++)
    {
        d0[i] = (i32)(i ^ 0xDEAD);
        d1[i] = (i64)i * 131 + 7;
        d2[i] = (f32)i * 0.333f;
        d3[i] = (f64)i * 1.41421356;
    }

    DataChunk c;
    DataChunk_init_compat(&c, 4);
    c.columns[0] = (VectorBase){.type = TYPE_INT32, .count = N, .data = (data_ptr_t)d0};
    c.columns[1] = (VectorBase){.type = TYPE_INT64, .count = N, .data = (data_ptr_t)d1};
    c.columns[2] = (VectorBase){.type = TYPE_FLOAT32, .count = N, .data = (data_ptr_t)d2};
    c.columns[3] = (VectorBase){.type = TYPE_FLOAT64, .count = N, .data = (data_ptr_t)d3};
    datatable_append(table, &c);

    /* verify segment counts for each column */
    usize row_segs = count_segments(segmentTree_get_root_segment(&table->row_storage_tree));
    ASSERT_EQ_U64(row_segs, 200, "200 RowSegments");

    usize c0_segs = count_segments(segmentTree_get_root_segment(&table->column_storage_tree[0]));
    usize c1_segs = count_segments(segmentTree_get_root_segment(&table->column_storage_tree[1]));
    usize c2_segs = count_segments(segmentTree_get_root_segment(&table->column_storage_tree[2]));
    usize c3_segs = count_segments(segmentTree_get_root_segment(&table->column_storage_tree[3]));
    ASSERT_EQ_U64(c0_segs, 25, "col0 (i32): 25 ColumnSegments");
    ASSERT_EQ_U64(c1_segs, 49, "col1 (i64): 49 ColumnSegments");
    ASSERT_EQ_U64(c2_segs, 25, "col2 (f32): 25 ColumnSegments");
    ASSERT_EQ_U64(c3_segs, 49, "col3 (f64): 49 ColumnSegments");

    /* full scan all 4 columns */
    usize ids[] = {0, 1, 2, 3};
    usize ts[] = {sizeof(i32), sizeof(i64), sizeof(f32), sizeof(f64)};
    i32* o0 = malloc(N * sizeof(i32));
    i64* o1 = malloc(N * sizeof(i64));
    f32* o2 = malloc(N * sizeof(f32));
    f64* o3 = malloc(N * sizeof(f64));
    void* bufs[] = {o0, o1, o2, o3};
    usize total = scan_all(table, ids, 4, bufs, ts);
    ASSERT_EQ_U64(total, N, "total == 50000");
    ASSERT_TRUE(memcmp(o0, d0, N * sizeof(i32)) == 0, "i32 byte-identical");
    ASSERT_TRUE(memcmp(o1, d1, N * sizeof(i64)) == 0, "i64 byte-identical");
    ASSERT_TRUE(memcmp(o2, d2, N * sizeof(f32)) == 0, "f32 byte-identical");
    ASSERT_TRUE(memcmp(o3, d3, N * sizeof(f64)) == 0, "f64 byte-identical");

    free(o0);
    free(o1);
    free(o2);
    free(o3);
    free(d0);
    free(d1);
    free(d2);
    free(d3);
    free(c.columns);
    Datatable_destroy(table);
}

/* M4. Exact ColumnSegment boundary — N is exact multiple of segment capacity
 *     10240 INT64 rows = exactly 10 full ColumnSegments (10240/1024)
 *     10240 / 250 = 40 full RowSegments + 1 with 240 rows = 41 total */
static void test_large_exact_segment_multiple(void)
{
    printf("\n--- M4. test_large_exact_segment_multiple (10240 i64) ---\n");
    const usize N = 10240; /* 10 * 1024 */
    TypeID types[] = {TYPE_INT64};
    DataTable* table = Datatable_create(NULL, "s", "t", 1, types);

    i64* data = malloc(N * sizeof(i64));
    for (usize i = 0; i < N; i++) data[i] = (i64)i;

    DataChunk c;
    DataChunk_init_compat(&c, 1);
    c.columns[0] = (VectorBase){.type = TYPE_INT64, .count = N, .data = (data_ptr_t)data};
    datatable_append(table, &c);

    usize col_segs = count_segments(segmentTree_get_root_segment(&table->column_storage_tree[0]));
    usize row_segs = count_segments(segmentTree_get_root_segment(&table->row_storage_tree));
    ASSERT_EQ_U64(col_segs, 10, "exactly 10 ColumnSegments");
    ASSERT_EQ_U64(row_segs, 41, "41 RowSegments (40x250 + 1x240)");

    /* verify last RowSegment has 240 rows */
    SegmentBase* last_row = segmentTree_get_last_segment(&table->row_storage_tree);
    ASSERT_EQ_U64(last_row->count, 240, "last RowSegment has 240 rows");

    /* verify every ColumnSegment is fully packed (count == 1024) */
    SegmentBase* seg = segmentTree_get_root_segment(&table->column_storage_tree[0]);
    bool all_full = true;
    usize idx = 0;
    while (seg)
    {
        if (seg->count != COL_SEG_CAP_I64)
        {
            all_full = false;
            break;
        }
        idx++;
        seg = seg->next;
    }
    ASSERT_TRUE(all_full, "all 10 ColumnSegments fully packed (1024)");

    /* scan and verify */
    usize ids[] = {0};
    usize ts = sizeof(i64);
    i64* out = malloc(N * sizeof(i64));
    void* bufs[] = {out};
    usize total = scan_all(table, ids, 1, bufs, &ts);
    ASSERT_EQ_U64(total, N, "total == 10240");
    ASSERT_TRUE(memcmp(out, data, N * sizeof(i64)) == 0, "byte-identical");

    free(out);
    free(data);
    free(c.columns);
    Datatable_destroy(table);
}

/* M5. Incremental multi-batch append: 200 batches x 100 rows = 20,000 rows
 *     Tests chaining correctness with many small appends across segments */
static void test_large_incremental_batch_append(void)
{
    printf("\n--- M5. test_large_incremental_batch_append (200 x 100 = 20000) ---\n");
    const usize BATCH = 100;
    const usize NBATCH = 200;
    const usize N = BATCH * NBATCH; /* 20000 */
    TypeID types[] = {TYPE_INT64};
    DataTable* table = Datatable_create(NULL, "s", "t", 1, types);

    i64* expect = malloc(N * sizeof(i64));
    for (usize i = 0; i < N; i++) expect[i] = (i64)(i * 41 + 3);

    for (usize b = 0; b < NBATCH; b++)
    {
        DataChunk c;
        DataChunk_init_compat(&c, 1);
        c.columns[0] = (VectorBase){
            .type = TYPE_INT64,
            .count = BATCH,
            .data = (data_ptr_t)(expect + b * BATCH),
        };
        datatable_append(table, &c);
        free(c.columns);
    }

    usize row_segs = count_segments(segmentTree_get_root_segment(&table->row_storage_tree));
    usize col_segs = count_segments(segmentTree_get_root_segment(&table->column_storage_tree[0]));
    ASSERT_EQ_U64(row_segs, 80, "80 RowSegments (20000/250)");
    /* ceil(20000/1024) = 20 */
    ASSERT_EQ_U64(col_segs, 20, "20 ColumnSegments");

    usize ids[] = {0};
    usize ts = sizeof(i64);
    i64* out = malloc(N * sizeof(i64));
    void* bufs[] = {out};
    usize total = scan_all(table, ids, 1, bufs, &ts);
    ASSERT_EQ_U64(total, N, "total == 20000");
    ASSERT_TRUE(memcmp(out, expect, N * sizeof(i64)) == 0, "byte-identical after 200 batches");

    free(out);
    free(expect);
    Datatable_destroy(table);
}

/* M6. Projection across many segments: 4 cols, 30K rows, project cols 1,3 only */
static void test_large_projection_30k(void)
{
    printf("\n--- M6. test_large_projection_30k (30000 rows, project 2 of 4 cols) ---\n");
    const usize N = 30000;
    TypeID types[] = {TYPE_INT32, TYPE_INT64, TYPE_FLOAT32, TYPE_FLOAT64};
    DataTable* table = Datatable_create(NULL, "s", "t", 4, types);

    i32* d0 = malloc(N * sizeof(i32));
    i64* d1 = malloc(N * sizeof(i64));
    f32* d2 = malloc(N * sizeof(f32));
    f64* d3 = malloc(N * sizeof(f64));
    for (usize i = 0; i < N; i++)
    {
        d0[i] = (i32)i;
        d1[i] = (i64)i * 99;
        d2[i] = (f32)i * 0.5f;
        d3[i] = (f64)i * 2.718;
    }

    DataChunk c;
    DataChunk_init_compat(&c, 4);
    c.columns[0] = (VectorBase){.type = TYPE_INT32, .count = N, .data = (data_ptr_t)d0};
    c.columns[1] = (VectorBase){.type = TYPE_INT64, .count = N, .data = (data_ptr_t)d1};
    c.columns[2] = (VectorBase){.type = TYPE_FLOAT32, .count = N, .data = (data_ptr_t)d2};
    c.columns[3] = (VectorBase){.type = TYPE_FLOAT64, .count = N, .data = (data_ptr_t)d3};
    datatable_append(table, &c);

    /* project only col 1 (INT64) and col 3 (FLOAT64) */
    usize ids[] = {1, 3};
    usize ts[] = {sizeof(i64), sizeof(f64)};
    i64* o1 = malloc(N * sizeof(i64));
    f64* o3 = malloc(N * sizeof(f64));
    void* bufs[] = {o1, o3};
    usize total = scan_all(table, ids, 2, bufs, ts);
    ASSERT_EQ_U64(total, N, "total == 30000");
    ASSERT_TRUE(memcmp(o1, d1, N * sizeof(i64)) == 0, "projected i64 col byte-identical");
    ASSERT_TRUE(memcmp(o3, d3, N * sizeof(f64)) == 0, "projected f64 col byte-identical");

    free(o1);
    free(o3);
    free(d0);
    free(d1);
    free(d2);
    free(d3);
    free(c.columns);
    Datatable_destroy(table);
}

/* M7. Boundary sentinel verification — check specific values at ColumnSegment edges
 *     INT64: boundaries at 1024, 2048, 3072 ...
 *     Place sentinel patterns and verify they survive the segment crossing */
static void test_large_boundary_sentinels(void)
{
    printf("\n--- M7. test_large_boundary_sentinels (5000 i64) ---\n");
    const usize N = 5000;
    TypeID types[] = {TYPE_INT64};
    DataTable* table = Datatable_create(NULL, "s", "t", 1, types);

    i64* data = malloc(N * sizeof(i64));
    for (usize i = 0; i < N; i++) data[i] = (i64)i;
    /* place sentinels at segment boundaries */
    data[1023] = 0x1111111111111111LL; /* last elem of seg 0 */
    data[1024] = 0x2222222222222222LL; /* first elem of seg 1 */
    data[2047] = 0x3333333333333333LL; /* last elem of seg 1 */
    data[2048] = 0x4444444444444444LL; /* first elem of seg 2 */
    data[3071] = 0x5555555555555555LL; /* last elem of seg 2 */
    data[3072] = 0x6666666666666666LL; /* first elem of seg 3 */
    data[4095] = 0x7777777777777777LL; /* last elem of seg 3 */
    data[4096] = (i64)0x8888888888888888LL; /* first elem of seg 4 */

    DataChunk c;
    DataChunk_init_compat(&c, 1);
    c.columns[0] = (VectorBase){.type = TYPE_INT64, .count = N, .data = (data_ptr_t)data};
    datatable_append(table, &c);

    usize ids[] = {0};
    usize ts = sizeof(i64);
    i64* out = malloc(N * sizeof(i64));
    void* bufs[] = {out};
    usize total = scan_all(table, ids, 1, bufs, &ts);
    ASSERT_EQ_U64(total, N, "total == 5000");

    ASSERT_TRUE(out[1023] == 0x1111111111111111LL, "sentinel[1023] (end seg0)");
    ASSERT_TRUE(out[1024] == 0x2222222222222222LL, "sentinel[1024] (start seg1)");
    ASSERT_TRUE(out[2047] == 0x3333333333333333LL, "sentinel[2047] (end seg1)");
    ASSERT_TRUE(out[2048] == 0x4444444444444444LL, "sentinel[2048] (start seg2)");
    ASSERT_TRUE(out[3071] == 0x5555555555555555LL, "sentinel[3071] (end seg2)");
    ASSERT_TRUE(out[3072] == 0x6666666666666666LL, "sentinel[3072] (start seg3)");
    ASSERT_TRUE(out[4095] == 0x7777777777777777LL, "sentinel[4095] (end seg3)");
    ASSERT_TRUE(out[4096] == (i64)0x8888888888888888LL, "sentinel[4096] (start seg4)");

    /* also verify non-sentinel values survived */
    ASSERT_TRUE(out[0] == 0 && out[1] == 1 && out[4999] == 4999, "non-sentinel values intact");

    free(out);
    free(data);
    free(c.columns);
    Datatable_destroy(table);
}

/* M8. Per-batch streaming verification — verify each scan batch in-flight
 *     20,000 INT64 rows; check every value during scan (not collected after) */
static void test_large_streaming_verify(void)
{
    printf("\n--- M8. test_large_streaming_verify (20000 i64, per-batch check) ---\n");
    const usize N = 20000;
    TypeID types[] = {TYPE_INT64};
    DataTable* table = Datatable_create(NULL, "s", "t", 1, types);

    i64* data = malloc(N * sizeof(i64));
    for (usize i = 0; i < N; i++) data[i] = (i64)(i * 59 + 11);

    DataChunk c;
    DataChunk_init_compat(&c, 1);
    c.columns[0] = (VectorBase){.type = TYPE_INT64, .count = N, .data = (data_ptr_t)data};
    datatable_append(table, &c);

    ScanState st;
    datatable_init_scan(table, &st);
    usize ts = sizeof(i64);
    DataChunk out = alloc_output(1, &ts);
    usize ids[] = {0};

    usize total = 0;
    int batches = 0;
    bool all_ok = true;
    while (datatable_scan(table, &st, &out, ids, 1))
    {
        i64* row = (i64*)out.columns[0].data;
        usize n = out.columns[0].count;
        for (usize i = 0; i < n; i++)
        {
            i64 expected = (i64)((total + i) * 59 + 11);
            if (row[i] != expected)
            {
                printf("    mismatch at row %lu: expected %ld, got %ld\n",
                       (unsigned long)(total + i), (long)expected, (long)row[i]);
                all_ok = false;
                break;
            }
        }
        total += n;
        batches++;
        if (!all_ok) break;
    }
    ASSERT_TRUE(all_ok, "every value correct in streaming scan");
    ASSERT_EQ_U64(total, N, "total == 20000");
    ASSERT_EQ_U64(batches, N / STANDARD_VECTOR_SIZE, "400 batches (20000/50)");

    free_output(&out);
    scanstate_deinit(&st);
    free(data);
    free(c.columns);
    Datatable_destroy(table);
}

/* M9. Append-after-scan consistency — append 10K, scan, append 10K more, re-scan all 20K */
static void test_large_append_scan_append(void)
{
    printf("\n--- M9. test_large_append_scan_append (10K + 10K) ---\n");
    const usize HALF = 10000;
    const usize N = HALF * 2;
    TypeID types[] = {TYPE_INT64};
    DataTable* table = Datatable_create(NULL, "s", "t", 1, types);

    i64* data = malloc(N * sizeof(i64));
    for (usize i = 0; i < N; i++) data[i] = (i64)i;

    /* append first half */
    DataChunk c;
    DataChunk_init_compat(&c, 1);
    c.columns[0] = (VectorBase){.type = TYPE_INT64, .count = HALF, .data = (data_ptr_t)data};
    datatable_append(table, &c);

    /* scan first half */
    usize ids[] = {0};
    usize ts = sizeof(i64);
    i64* out1 = malloc(HALF * sizeof(i64));
    void* bufs1[] = {out1};
    usize t1 = scan_all(table, ids, 1, bufs1, &ts);
    ASSERT_EQ_U64(t1, HALF, "first scan: 10000 rows");
    ASSERT_TRUE(memcmp(out1, data, HALF * sizeof(i64)) == 0, "first half correct");

    /* append second half */
    c.columns[0] =
        (VectorBase){.type = TYPE_INT64, .count = HALF, .data = (data_ptr_t)(data + HALF)};
    datatable_append(table, &c);

    /* re-scan all 20K */
    i64* out2 = malloc(N * sizeof(i64));
    void* bufs2[] = {out2};
    usize t2 = scan_all(table, ids, 1, bufs2, &ts);
    ASSERT_EQ_U64(t2, N, "full scan: 20000 rows");
    ASSERT_TRUE(memcmp(out2, data, N * sizeof(i64)) == 0, "all 20K byte-identical");

    usize row_segs = count_segments(segmentTree_get_root_segment(&table->row_storage_tree));
    usize col_segs = count_segments(segmentTree_get_root_segment(&table->column_storage_tree[0]));
    ASSERT_EQ_U64(row_segs, 80, "80 RowSegments");
    ASSERT_EQ_U64(col_segs, 20, "20 ColumnSegments");

    free(out1);
    free(out2);
    free(data);
    free(c.columns);
    Datatable_destroy(table);
}

/* M10. Mixed-width columns at scale: 20K rows, i32 + f64
 *      i32 boundary at 2048, f64 boundary at 1024 — different cadence
 *      Both are scanned together; verify cross-column segment alignment is independent */
static void test_large_mixed_width_20k(void)
{
    printf("\n--- M10. test_large_mixed_width_20k (20000 rows, i32+f64) ---\n");
    const usize N = 20000;
    TypeID types[] = {TYPE_INT32, TYPE_FLOAT64};
    DataTable* table = Datatable_create(NULL, "s", "t", 2, types);

    i32* d0 = malloc(N * sizeof(i32));
    f64* d1 = malloc(N * sizeof(f64));
    for (usize i = 0; i < N; i++)
    {
        d0[i] = (i32)(i * 3 + 1);
        d1[i] = (f64)i * 3.14159265;
    }

    DataChunk c;
    DataChunk_init_compat(&c, 2);
    c.columns[0] = (VectorBase){.type = TYPE_INT32, .count = N, .data = (data_ptr_t)d0};
    c.columns[1] = (VectorBase){.type = TYPE_FLOAT64, .count = N, .data = (data_ptr_t)d1};
    datatable_append(table, &c);

    /* verify different segment counts */
    usize c0_segs = count_segments(segmentTree_get_root_segment(&table->column_storage_tree[0]));
    usize c1_segs = count_segments(segmentTree_get_root_segment(&table->column_storage_tree[1]));
    /* i32: ceil(20000/2048) = 10 */
    ASSERT_EQ_U64(c0_segs, 10, "i32: 10 ColumnSegments");
    /* f64: ceil(20000/1024) = 20 */
    ASSERT_EQ_U64(c1_segs, 20, "f64: 20 ColumnSegments");

    usize ids[] = {0, 1};
    usize ts[] = {sizeof(i32), sizeof(f64)};
    i32* o0 = malloc(N * sizeof(i32));
    f64* o1 = malloc(N * sizeof(f64));
    void* bufs[] = {o0, o1};
    usize total = scan_all(table, ids, 2, bufs, ts);
    ASSERT_EQ_U64(total, N, "total == 20000");
    ASSERT_TRUE(memcmp(o0, d0, N * sizeof(i32)) == 0, "i32 byte-identical");
    ASSERT_TRUE(memcmp(o1, d1, N * sizeof(f64)) == 0, "f64 byte-identical");

    free(o0);
    free(o1);
    free(d0);
    free(d1);
    free(c.columns);
    Datatable_destroy(table);
}

/* ============================================================
 * Main
 * ============================================================ */
int main(void)
{
    printf("==============================================\n");
    printf("  DataTable append/scan Comprehensive Test\n");
    printf("==============================================\n");

    /* A. create */
    test_create_initial_state();

    /* B. append edge cases */
    test_append_empty_chunk();
    test_append_single_row();
    test_append_exact_chunk_size();
    test_append_chunk_plus_one();

    /* C. multiple appends / RowSegment boundary */
    test_append_multi_small_within_chunk();
    test_append_cross_row_segment_boundary();
    test_append_one_by_one();
    test_append_exact_multiple_chunks();

    /* D. ColumnSegment boundary */
    test_append_cross_column_segment_i64();
    test_append_cross_column_segment_i32();

    /* E. scan empty / single */
    test_scan_empty_table();
    test_scan_single_row();

    /* F. scan batch sizes */
    test_scan_exact_one_batch();
    test_scan_batch_sizes();

    /* G. all TypeIDs */
    test_scan_type_int32();
    test_scan_type_int64();
    test_scan_type_float32();
    test_scan_type_float64();
    test_scan_all_four_types();

    /* H. column projection */
    test_scan_project_first_col();
    test_scan_project_last_col();
    test_scan_project_reverse_order();
    test_scan_project_non_contiguous();

    /* I. ColumnSegment boundary during scan */
    test_scan_cross_column_segment_i64();
    test_scan_cross_column_segment_i32();
    test_scan_cross_column_segment_mixed();

    /* J. re-scan / exhaustion / state */
    test_scan_rescan();
    test_scan_after_exhaustion();
    test_scan_state_init();

    /* K. DataChunk / VectorBase helpers */
    test_datachunk_helpers();
    test_datachunk_size_zero_cols();
    test_column_vector_from_vector();
    test_datachunk_append();

    /* L. combined workflows */
    test_combined_multi_append_full_scan();
    test_combined_append_scan_append_rescan();
    test_combined_large_i64();
    test_combined_incremental_cross_column_seg();
    test_combined_projection_cross_boundary();

    /* M. large-scale multi-segment correctness */
    test_large_100k_i64();
    test_large_100k_i32();
    test_large_50k_mixed_4col();
    test_large_exact_segment_multiple();
    test_large_incremental_batch_append();
    test_large_projection_30k();
    test_large_boundary_sentinels();
    test_large_streaming_verify();
    test_large_append_scan_append();
    test_large_mixed_width_20k();

    printf("\n==============================================\n");
    printf("  Results: %d passed, %d failed, %d total\n", g_pass, g_fail, g_pass + g_fail);
    printf("==============================================\n");

    return g_fail > 0 ? 1 : 0;
}
