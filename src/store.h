#ifndef STORE_H
#define STORE_H

#include "interface.h"
#include "segment.h"
#include "types.h"
#include "vb_type.h"
#include "lock.h"

typedef struct TableSchema TableSchema;

/* Forward declaration — full definition is in datatable.h.
 * Pointer-only use here; datatable.h provides the struct body.
 * Avoids the circular: datatable.h → store.h → datatable.h. */
typedef struct DataChunk DataChunk;

/* ============================================================
 * heap store
 * ============================================================*/

#define INVALID_ITEM_PTR \
    ((ItemPtr){.ip_blkid_hi = UINT16_MAX, .ip_blkid_lo = UINT16_MAX, .ip_posid = UINT16_MAX})

#define INVALID_TXN_ID    ((TxnId)0)  /* 未设置 / 占位 */

#define HS_BLOCK_HDR_SIZE 6u /* sizeof(pd_lower) + sizeof(pd_upper) + sizeof(pd_flags) */
#define HS_SLOT_SIZE      4u /* sizeof(RowSlotId) */
/** PG-compatible page flag: set when page has at least one LP_UNUSED slot. */
#define PD_HAS_FREE_LINES 0x0001u

// Max tuple size in heap store
#define HS_MAX_TUPLE_SIZE ((usize)(BLOCK_SIZE - HS_BLOCK_HDR_SIZE - HS_SLOT_SIZE))

typedef enum
{
    TUPLE_COL_NULL = 0,  /* null / no value */
    TUPLE_COL_BOOL = 1,
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

/* ============================================================
 * Datum — PostgreSQL-style unified value representation (u64).
 *
 * By-value types (BOOL, INT32, INT64, FLOAT32, FLOAT64):
 *   Stored inline in the low bits of the u64.
 *
 * By-reference types (TEXT, BYTEA, JSONB):
 *   Stored as pointer (PointerGetDatum), in-memory layout:
 *     TEXT / BYTEA: [u32 content_len][data bytes...]
 *     JSONB:        VbJsonb* — vl_len_ bytes total (includes 4-byte header)
 *
 * null_bits (u64 in TupleHdr): bit i set → col i is NULL → Datum[i] undefined.
 * ============================================================ */

typedef u64 Datum;  // 每个字段的数据载体

/* ---- By-value → Datum ---- */
#define BoolGetDatum(v)  ((Datum)(u8)(!!(v)))
#define Int32GetDatum(v) ((Datum)(u64)(i64)(i32)(v))
#define Int64GetDatum(v) ((Datum)(u64)(i64)(v))
static inline Datum Float32GetDatum(f32 v)
{
    u32 b;
    memcpy(&b, &v, 4);
    return (Datum)b;
}
static inline Datum Float64GetDatum(f64 v)
{
    u64 b;
    memcpy(&b, &v, 8);
    return b;
}

/* ---- Datum → by-value ---- */
#define DatumGetBool(d)  ((bool)((u8)(d) != 0))
#define DatumGetInt32(d) ((i32)(i64)(u64)(d))
#define DatumGetInt64(d) ((i64)(u64)(d))
static inline f32 DatumGetFloat32(Datum d)
{
    u32 b = (u32)(d);
    f32 v;
    memcpy(&v, &b, 4);
    return v;
}
static inline f64 DatumGetFloat64(Datum d)
{
    u64 b = (u64)(d);
    f64 v;
    memcpy(&v, &b, 8);
    return v;
}

/* ---- By-reference ---- */
#define PointerGetDatum(p) ((Datum)(uintptr_t)(const void*)(p))
#define DatumGetPointer(d) ((void*)(uintptr_t)(d))
#define DatumGetJsonb(d)   ((VbJsonb*)DatumGetPointer(d))
#define JsonbGetDatum(jb)  PointerGetDatum(jb)

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
    u16 lp_off;  /* 2: byte offset of the tuple data within the page */
    u16 lp_len;  /* 2: length of the tuple data in bytes */
} TupleSlotId;

typedef struct
{
    TupleHdr hdr; /* MVCC header (written by row_store_*; read back by get) */
    Datum* cols; /* array of ncols TupleVal (caller-allocated for insert/update) */
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
const f32* embedding_store_get_ptr_ctid(EmbeddingStore* store, ItemPtr emb_ctid);

typedef struct
{
    SegmentTree tree;              /* one SegmentTree of slotted-page BlockSegments */
    TxnId next_txn_id;       /* MVCC transaction counter */
    u32 page_count;
    BlockManager* block_manager;
    BlockSegment* hint_free_seg; /* PG-style: first segment that may have LP_UNUSED
                                  * slots (PD_HAS_FREE_LINES set on its page).
                                  * NULL = no known free page.  Set by vacuum_row_store;
                                  * consumed lazily by row_store_insert.
                                  * Not serialized — rebuilt from pd_flags on load. */
    const TableSchema* schema;
    LWLock lock;  /* EXCLUSIVE=write/vacuum, SHARED=read*/
} HeapStore;

void HeapStore_init(HeapStore* store, const TableSchema* schema, BlockManager* bm);
void HeapStore_deinit(HeapStore* store);

/* ============================================================
 * HeapTupleRef — zero-copy in-page tuple view (PG buffer-pin style)
 *
 * Holds LW_SHARED on store->content_lock for the ref lifetime.
 * hdr / col_data point directly into the page buffer.
 *
 * Lifecycle:
 *   row_store_acquire_ref() → use hdr / col_data / heap_deform_tuple()
 *   row_store_release_ref() → LW_SHARED released
 * ============================================================ */
typedef struct
{
    const TupleHdr* hdr;       /* direct page-buffer pointer */
    const u8* col_data;  /* hdr + sizeof(TupleHdr)     */
    const TableSchema* schema;
    HeapStore* store;
} HeapTupleRef;

int heapStore_deform_tuple(const HeapTupleRef* ref, Datum* out, usize ncols);

/**
 * Insert a new tuple.
 *
 * Caller must fill: tuple->cols, tuple->ncols (= schema->ncols), tuple->hdr.null_bits.
 * row_store_insert fills hdr.{t_xmin,t_xmax,t_ctid} (t_emb_ctid set by table_am layer).
 *
 * Returns the physical ctid (ItemPtr) assigned to this row.
 * The returned ctid is the authoritative row identity for all subsequent operations.
 */
ItemPtr heapStore_insert(HeapStore* store, TxnId xid, ItemPtr emb_ctid, const Datum* values,
                         u64 null_bits);

/**
 * Read the physical row at ctid into *out (regardless of MVCC status).
 * Useful for inspecting old versions and the MVCC version chain.
 * Returns 0 on success, -1 if ctid is invalid or the tuple is deleted.
 * Updated-but-not-deleted old versions are returned normally for chain traversal.
 * out->cols is heap-allocated; call row_tuple_free(out, schema) when done.
 */
int heapStore_get_by_ctid(HeapStore* store, const TableSchema* schema, ItemPtr ctid,
                          HeapTuple* out);

/**
 * MVCC delete: mark the current version as deleted.
 *
 * Sets t_xmax=txn (non-zero marks tuple as deleted).
 * t_ctid stays self-pointing.
 * Returns the txn_id used on success, INVALID_TXN_ID if ctid is not found or already dead.
 */
TxnId heapStore_delete_by_ctid(HeapStore* store, ItemPtr ctid);
/**
 * MVCC update: append a new version and mark the old one superseded.
 *
 * OLD tuple: t_xmax=txn, t_ctid=new_location (forward pointer).
 * NEW tuple: t_xmin=txn, t_xmax=INVALID_TXN_ID, t_ctid=self.
 *
 * Caller fills: new_tuple->cols, new_tuple->ncols, new_tuple->hdr.null_bits.
 * After return, new_tuple->hdr.t_ctid holds the new physical ctid.
 * Returns the txn_id used on success, INVALID_TXN_ID if old_ctid is not found or dead.
 */
TxnId heapStore_update_by_ctid(HeapStore* store, ItemPtr old_ctid, HeapTuple* new_tuple);

typedef struct
{
    HeapStore* store;
    SegmentBase* curr_seg;
    u32 slot_idx;
    u16 curr_tup_len;
    const TupleHdr* hdr;
    const u8* curr_col_data; /* points directly into page buffer (valid while lock held) */
} HeapStoreIter;

void heapStoreIter_begin(HeapStoreIter* iter, HeapStore* store);
const TupleHdr* heapStoreIter_next(HeapStoreIter* iter);
void heapStoreIter_end(HeapStoreIter* iter);

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

u64 heapStore_slot_count(HeapStore* store);

/* ============================================================
 * embedding store
 * ============================================================*/

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
    LWLock lock;   /* EXCLUSIVE=write, SHARED=read*/
};

/* ============================================================
 * column store
 * ============================================================*/
typedef struct ColumnStore ColumnStore;

struct ColumnStore
{
};
#endif
