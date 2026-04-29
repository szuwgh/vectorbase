#ifndef INDEX_H
#define INDEX_H

#include "vb_type.h"
#include "interface.h"
#include "store.h"
#include "operator.h"

typedef struct
{
    u64 heap_ctid_packed; /* itemptr_pack(heap_ctid) — physical heap identity */
    u64 emb_ctid_packed;  /* itemptr_pack(emb_ctid) — packed EmbeddingStore ItemPtr */
    f32 distance;         /* 与查询向量的距离 */
} SearchResult;

/* ============================================================
 * 索引类型枚举
 * ============================================================ */
typedef enum
{
    INDEX_FLAT = 0,
    INDEX_HNSW = 1,
    INDEX_IVF = 2,
    INDEX_DISKANN = 3,
} VectorIndexType;

// clang-format off
DEFINE_CLASS(VectorIndex,
    VMETHOD(VectorIndex, insert, void, u64 heap_ctid_packed, ItemPtr emb_ctid, const f32* vector)
    VMETHOD(VectorIndex, search, usize, const f32* query, usize k, SearchResult* results)
    VMETHOD(VectorIndex, remove, bool, u64 heap_ctid_packed)
    VMETHOD(VectorIndex, write_blocks, void, BlockManager* bm, MetaBlockWriter* w)
    VMETHOD(VectorIndex, load_blocks, void, BlockManager* bm, MetaBlockReader* r)
    VMETHOD(VectorIndex, destroy, void)
    ,
    FIELD(type, VectorIndexType)
    FIELD(metric, DistanceType)
    FIELD(dimension, i16)
    FIELD(vector_count, u64)
    FIELD(store, struct EmbeddingStore*)
)
// clang-format on

#endif