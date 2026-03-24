#ifndef STORE_H
#define STORE_H

#include "interface.h"
#include "segment.h"
#include "types.h"
#include "vb_type.h"

#define INVALID_ITEM_PTR \
    ((ItemPtr){.ip_blkid_hi = UINT16_MAX, .ip_blkid_lo = UINT16_MAX, .ip_posid = UINT16_MAX})

#define INVALID_TXN_ID ((TxnId)0)  /* 未设置 / 占位 */

/* Forward declaration — full definition is in datatable.h.
 * Pointer-only use here; datatable.h provides the struct body.
 * Avoids the circular: datatable.h → store.h → datatable.h. */
typedef struct DataChunk DataChunk;

typedef enum
{
    TUPLE_COL_NULL = 0,  /* null / no value */
    TUPLE_COL_B = 1,
    TUPLE_COL_I32 = 2,
    TUPLE_COL_I64 = 3,
    TUPLE_COL_F32 = 4,
    TUPLE_COL_F64 = 5,
    TUPLE_COL_TEXT = 6,
    TUPLE_COL_JSONB = 7,
} TupleColType;

typedef struct
{
    TupleColType type;
    union
    {
        bool b;
        i32 i32;
        i64 i64;
        f32 f32;
        f64 f64;
        struct
        {
            char* ptr;
            u32 len;
        } text;    /* heap-allocated when from row_store_get */
       // JsonB* jsonb;   /* heap-allocated when from row_store_get */
    } v;
} TupleVal;

typedef u32 CommandId;

typedef struct
{
    u16 ip_blkid_hi;  /* 2: high 16 bits of disk block_id (PG bi_hi)              */
    u16 ip_blkid_lo;  /* 2: low  16 bits of disk block_id (PG bi_lo)              */
    u16 ip_posid;     /* 2: 1-based slot number within the page (PG OffsetNumber) */
} ItemPtr;            /* 6 bytes, alignment 2 — matches PG ItemPointerData */

typedef struct
{
    TxnId t_xmin;      /*  0: inserting transaction                           */
    TxnId t_xmax;      /*  8: deleting/updating transaction; 0 = alive        */
    ItemPtr t_emb_ctid;  /* 16: EmbeddingStore ctid (INVALID_ITEM_PTR if none)  */
    ItemPtr t_ctid;      /* 22: heap physical location or UPDATE forward ptr    */
    u64 null_bits;   /* 32: bit i set → user col i is NULL                  */
} TupleHdr; /* 40 bytes: 8+8+4+6+2+2+1+1+8 = 40                  */

typedef struct
{
    TupleHdr hdr; /* MVCC header (written by row_store_*; read back by get) */
    u64 row_id; /* logical row identifier */
    TupleVal* cols; /* array of ncols TupleVal (caller-allocated for insert/update) */
    u16 ncols; /* number of columns (must match schema->ncols) */
} HeapTuple;

/* EmbeddingStore — forward declaration only.
 * Full struct body is defined in src/store.c (for libvectorbase.a)
 * and in tmp/src/embedding_store.h (for tmp/ build).
 * Using forward decl here avoids a conflicting-types error when
 * tmp/src/ TUs include both this header and tmp/src/embedding_store.h. */
typedef struct EmbeddingStore EmbeddingStore;

void EmbeddingStore_init(EmbeddingStore* store, i16 dimension);
void EmbeddingStore_deinit(EmbeddingStore* store);
ItemPtr embeddingStore_append_and_get_ctid(EmbeddingStore* store, VectorBase* vec);

typedef struct
{
    SegmentTree tree;              /* one SegmentTree of slotted-page BlockSegments */
    hmap id_map;            /* row_id (u64) → latest internal_id (u64) */
    TxnId next_txn_id;       /* MVCC transaction counter */
    BlockManager* block_manager;
    Vector free_list;
} HeapStore;

void HeapStore_init(HeapStore* store);
void HeapStore_deinit(HeapStore* store);
void heapStore_append(HeapStore* store, u64 row_id, HeapTuple* tuple);
/* Complete EmbeddingStore struct — store.h only has the forward decl.
 * Must match the layout in tmp/src/embedding_store.h for ABI compatibility. */
struct EmbeddingStore
{
    SegmentTree tree;
    i16 dimension;
    usize count;
    usize elem_size;
    usize vecs_per_blk;       /* = BLOCK_SIZE / elem_size  (computed once at init)*/
    Vector free_list;
    ItemPtr last_ctid;
};

/* ColumnStore = DataTable forward declaration.
 * Compatible with: typedef DataTable ColumnStore; in tmp/src/column_store.h
 * and with the struct DataTable definition in datatable.h. */
typedef struct DataTable ColumnStore;

/**
 * Build a physical ItemPtr from (block_id, 0-based slot_idx).
 * Stores lower 32 bits of block_id; ip_posid = slot_idx + 1 (1-based).
 */
static inline ItemPtr make_item_ptr(block_id_t block_id, u16 slot_idx)
{
    u32 blk = (u32)block_id;
    return (ItemPtr){.ip_blkid_hi = (u16)(blk >> 16),
                     .ip_blkid_lo = (u16)(blk & 0xFFFFu),
                     .ip_posid = (u16)(slot_idx + 1)};
}

/**
 * Return the disk block_id encoded in an ItemPtr.
 * Note: only the lower 32 bits of block_id_t are stored in ip_blkid.
 */
static inline block_id_t item_ptr_block_id(ItemPtr p)
{
    return (block_id_t)(((u32)p.ip_blkid_hi << 16) | (u32)p.ip_blkid_lo);
}

/** Return the 0-based slot index encoded in an ItemPtr. */
static inline u16 item_ptr_slot(ItemPtr p)
{
    return (u16)(p.ip_posid - 1);
}

/** Compatibility alias: same as item_ptr_block_id. */
static inline block_id_t item_ptr_block(ItemPtr p)
{
    return item_ptr_block_id(p);
}

static inline bool item_ptr_is_valid(ItemPtr p)
{
    return p.ip_blkid_hi != UINT16_MAX;
}

u64 heapStore_slot_count(HeapStore* store)
{
    SegmentBase* last = segmentTree_get_last_segment(&store->tree);
    if (!last) return 0;
    return (u64)(last->start + last->count);
}

#endif
