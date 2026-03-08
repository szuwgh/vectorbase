#ifndef VECTOR_INDEX_H
#define VECTOR_INDEX_H
#include "vb_type.h"
#include "interface.h"
#include "storage.h"
#include "operator.h"

typedef struct SearchResult SearchResult;

typedef enum : u8
{
    IVF = 1,
    HNSW = 2,
    DISKANN = 3,
} VectorIndexType;

// clang-format off
DEFINE_CLASS(VectorIndex,
    VMETHOD(VectorIndex, insert, void, u64 row_id, const f32* vector)
    VMETHOD(VectorIndex, search, usize, const f32* query, usize k, SearchResult* results)
    VMETHOD(VectorIndex, remove, bool, u64 row_id)
    VMETHOD(VectorIndex, serialize, void, MetaBlockWriter* writer)
    VMETHOD(VectorIndex, deserialize, void, MetaBlockReader* reader)
    VMETHOD(VectorIndex, destroy, void)
    ,
    FIELD(type, VectorIndexType)
    FIELD(metric, DistanceType)
    FIELD(dimension, i16)
    FIELD(vector_count, u64)
    FIELD(store, struct EmbeddingStore*)
)
// clang-format on

#endif /* VECTOR_INDEX_H */