/*
 * example_datatable.c — VectorBase columnar storage demo
 *
 * Demonstrates:
 *   1. Creating a DataTable with four typed columns
 *   2. Appending rows via DataChunk (columnar batch interface)
 *   3. Scanning the table back with column projection
 *   4. Pretty-printing the result as a formatted table
 *
 * Build:  make          (from the example/ directory)
 * Run:    ./example_datatable
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "../src/datatable.h"

/* ── helpers ─────────────────────────────────────────────────────── */

static const char* typeid_name(TypeID t)
{
    switch (t)
    {
        case TYPE_INT32:
            return "INT32";
        case TYPE_INT64:
            return "INT64";
        case TYPE_FLOAT32:
            return "FLOAT32";
        case TYPE_FLOAT64:
            return "FLOAT64";
        default:
            return "???";
    }
}

/* format one cell value into buf; returns buf */
static char* fmt_cell(char* buf, usize bufsz, TypeID type, const void* ptr)
{
    switch (type)
    {
        case TYPE_INT32:
            snprintf(buf, bufsz, "%d", *(const i32*)ptr);
            break;
        case TYPE_INT64:
            snprintf(buf, bufsz, "%ld", *(const i64*)ptr);
            break;
        case TYPE_FLOAT32:
            snprintf(buf, bufsz, "%.2f", *(const f32*)ptr);
            break;
        case TYPE_FLOAT64:
            snprintf(buf, bufsz, "%.4f", *(const f64*)ptr);
            break;
        default:
            snprintf(buf, bufsz, "?");
            break;
    }
    return buf;
}

/* ── pretty printer ──────────────────────────────────────────────── */

/*
 * print_table — scan a DataTable and print it as a bordered ASCII table.
 *
 * column_names : display name for each column
 * column_ids   : which table columns to project (NULL = all, in order)
 */
static void print_table(DataTable* table, const char** column_names, usize* column_ids,
                        usize col_count)
{
    /* decide column projection */
    usize proj[col_count];
    if (column_ids)
        memcpy(proj, column_ids, col_count * sizeof(usize));
    else
        for (usize i = 0; i < col_count; i++) proj[i] = i;

    /* ---- first pass: scan all data into memory so we can measure widths ---- */

    /* accumulate rows in flat buffers, one per projected column */
    usize cap = 256, total_rows = 0;
    void* cols_data[col_count];
    usize elem_sz[col_count];
    for (usize c = 0; c < col_count; c++)
    {
        elem_sz[c] = get_typeid_size(table->column_types[proj[c]]);
        cols_data[c] = malloc(cap * elem_sz[c]);
    }

    ScanState state;
    datatable_init_scan(table, &state);

    /* allocate scan output chunk with per-column buffers */
    DataChunk output;
    DataChunk_init(&output, col_count);
    for (usize c = 0; c < col_count; c++)
        output.columns[c] = (VectorBase){
            .type = table->column_types[proj[c]],
            .count = 0,
            .data = malloc(STORAGE_CHUNK_SIZE * elem_sz[c]),
        };

    while (datatable_scan(table, &state, &output, proj, col_count))
    {
        usize n = output.columns[0].count;
        /* grow buffers if needed */
        while (total_rows + n > cap)
        {
            cap *= 2;
            for (usize c = 0; c < col_count; c++)
                cols_data[c] = realloc(cols_data[c], cap * elem_sz[c]);
        }
        for (usize c = 0; c < col_count; c++)
            memcpy((u8*)cols_data[c] + total_rows * elem_sz[c], output.columns[c].data,
                   n * elem_sz[c]);
        total_rows += n;
    }

    /* free scan resources */
    for (usize c = 0; c < col_count; c++) free(output.columns[c].data);
    free(output.columns);
    scanstate_deinit(&state);

    /* ---- compute column widths ---- */

    usize widths[col_count];
    char cell[64];
    for (usize c = 0; c < col_count; c++)
    {
        widths[c] = strlen(column_names[c]);
        /* also account for the type tag shown under the name */
        usize type_len = strlen(typeid_name(table->column_types[proj[c]]));
        if (type_len > widths[c]) widths[c] = type_len;
    }
    for (usize r = 0; r < total_rows; r++)
    {
        for (usize c = 0; c < col_count; c++)
        {
            const void* ptr = (const u8*)cols_data[c] + r * elem_sz[c];
            fmt_cell(cell, sizeof(cell), table->column_types[proj[c]], ptr);
            usize len = strlen(cell);
            if (len > widths[c]) widths[c] = len;
        }
    }

    /* ---- render ---- */

    /* total line width: "| " + col + " " for each, plus final "|" */
    usize line_len = 1;
    for (usize c = 0; c < col_count; c++) line_len += widths[c] + 3;

    /* Print a horizontal rule using box-drawing characters */
#define HRULE(L, M, R, F)                                                  \
    do {                                                                   \
        printf("%s", L);                                                   \
        for (usize _c = 0; _c < col_count; _c++)                           \
        {                                                                  \
            for (usize _i = 0; _i < widths[_c] + 2; _i++) printf("%s", F); \
            printf("%s", _c + 1 < col_count ? M : R);                      \
        }                                                                  \
        printf("\n");                                                      \
    } while (0)

    /* ┌─┬─┐ */
    HRULE("\xe2\x94\x8c", "\xe2\x94\xac", "\xe2\x94\x90", "\xe2\x94\x80");

    /* column names */
    printf("\xe2\x94\x82");
    for (usize c = 0; c < col_count; c++)
        printf(" %-*s \xe2\x94\x82", (int)widths[c], column_names[c]);
    printf("\n");

    /* type tags (dimmed) */
    printf("\xe2\x94\x82");
    for (usize c = 0; c < col_count; c++)
        printf(" \033[2m%-*s\033[0m \xe2\x94\x82", (int)widths[c],
               typeid_name(table->column_types[proj[c]]));
    printf("\n");

    /* ├─┼─┤ */
    HRULE("\xe2\x94\x9c", "\xe2\x94\xbc", "\xe2\x94\xa4", "\xe2\x94\x80");

    /* data rows */
    for (usize r = 0; r < total_rows; r++)
    {
        printf("\xe2\x94\x82");
        for (usize c = 0; c < col_count; c++)
        {
            const void* ptr = (const u8*)cols_data[c] + r * elem_sz[c];
            fmt_cell(cell, sizeof(cell), table->column_types[proj[c]], ptr);

            /* right-align numbers, left-align otherwise */
            printf(" %*s \xe2\x94\x82", (int)widths[c], cell);
        }
        printf("\n");
    }

    /* └─┴─┘ */
    HRULE("\xe2\x94\x94", "\xe2\x94\xb4", "\xe2\x94\x98", "\xe2\x94\x80");

#undef HRULE

    printf("(%zu rows)\n", total_rows);

    /* cleanup */
    for (usize c = 0; c < col_count; c++) free(cols_data[c]);
}

/* ── main ────────────────────────────────────────────────────────── */

int main(void)
{
    /* ========== 1. Define schema ========== */

    TypeID column_types[] = {TYPE_INT64, TYPE_INT32, TYPE_FLOAT32, TYPE_FLOAT64};
    const usize NUM_COLS = sizeof(column_types) / sizeof(column_types[0]);

    const char* column_names[] = {"id", "age", "score", "balance"};

    DataTable* table = Datatable_create(NULL, "public", "users", NUM_COLS, column_types);

    printf("VectorBase DataTable Example\n");
    printf("============================\n\n");
    printf("Schema: public.users  (%zu columns)\n\n", NUM_COLS);

    /* ========== 2. Prepare data ========== */

    /*  id (INT64) | age (INT32) | score (FLOAT32) | balance (FLOAT64) */
    const i64 ids[] = {1001, 1002, 1003, 1004, 1005, 1006, 1007, 1008, 1009, 1010};
    const i32 ages[] = {28, 34, 22, 45, 31, 27, 39, 52, 24, 36};
    const f32 scores[] = {92.5f, 87.3f, 95.1f, 78.6f, 88.0f, 91.2f, 83.7f, 76.4f, 96.8f, 85.5f};
    const f64 balances[] = {
        15230.75, 8420.50,  32100.00, 5670.25,  19840.30,
        27650.80, 11300.45, 3200.10,  45000.00, 22175.60,
    };
    const usize NROWS = sizeof(ids) / sizeof(ids[0]);

    /* ========== 3. Append — first batch (rows 0..4) ========== */

    DataChunk chunk;
    DataChunk_init(&chunk, NUM_COLS);
    chunk.arrays[0] = (VectorBase){TYPE_INT64, 5, (data_ptr_t)&ids[0]};
    chunk.arrays[1] = (VectorBase){TYPE_INT32, 5, (data_ptr_t)&ages[0]};
    chunk.arrays[2] = (VectorBase){TYPE_FLOAT32, 5, (data_ptr_t)&scores[0]};
    chunk.arrays[3] = (VectorBase){TYPE_FLOAT64, 5, (data_ptr_t)&balances[0]};
    datatable_append(table, &chunk);

    /* ========== 4. Append — second batch (rows 5..9) ========== */

    chunk.arrays[0] = (VectorBase){TYPE_INT64, 5, (data_ptr_t)&ids[5]};
    chunk.arrays[1] = (VectorBase){TYPE_INT32, 5, (data_ptr_t)&ages[5]};
    chunk.arrays[2] = (VectorBase){TYPE_FLOAT32, 5, (data_ptr_t)&scores[5]};
    chunk.arrays[3] = (VectorBase){TYPE_FLOAT64, 5, (data_ptr_t)&balances[5]};
    datatable_append(table, &chunk);
    free(chunk.columns);

    printf("[+] Appended %zu rows in 2 batches.\n\n", NROWS);

    /* ========== 5. Full scan — all columns ========== */

    printf(">> SELECT * FROM public.users\n");
    usize all_ids[] = {0, 1, 2, 3};
    print_table(table, column_names, all_ids, NUM_COLS);

    /* ========== 6. Projected scan — subset of columns ========== */

    printf("\n>> SELECT id, score FROM public.users\n");
    usize proj_ids[] = {0, 2};
    const char* proj_names[] = {"id", "score"};
    print_table(table, proj_names, proj_ids, 2);

    /* ========== 7. Cleanup ========== */

    Datatable_destroy(table);

    return 0;
}
