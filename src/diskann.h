#ifndef DISKANN_H
#define DISKANN_H

#include "index.h"

typedef struct
{
    EXTENDS(VectorIndex);

} DiskAnn;

typedef struct
{
    i16* labels;             /* palloc'd sorted array */
    int numLabels;
    int capacity;
} DiskANNLabels;

void DiskAnn_init(DiskAnn* index, i16 dimension, DistanceType metric, struct EmbeddingStore* store);

void DiskAnn_deinit(DiskAnn* index);

void diskAnn_build(VectorIndex* index, u64 heap_ctid_packed, ItemPtr emb_ctid, const f32* vector);

void diskAnn_insert(VectorIndex* index, u64 heap_ctid_packed, ItemPtr emb_ctid, const f32* vector,
                    const DiskANNLabels* labels);
usize diskAnn_search(VectorIndex* index, const f32* query, const DiskANNLabels* labels, usize k,
                     SearchResult* results);

bool diskAnn_remove(VectorIndex* index, u64 heap_ctid_packed);

void diskAnn_write_blocks(VectorIndex* index, BlockManager* bm, MetaBlockWriter* w);

void diskAnn_load_blocks(VectorIndex* index, BlockManager* bm, MetaBlockReader* r);

void diskAnn_destroy(VectorIndex* index);

#endif