#ifndef SEGMENT_H
#define SEGMENT_H
#include "vb_type.h"
#include "vector.h"
#include "interface.h"
#include "storage.h"

typedef struct DataTable DataTable;

typedef enum
{
    COLUMEN_SEGMENT = 0,
    ROW_SEGMENT = 1,
} SegmentBaseType;

typedef struct SegmentBase SegmentBase;

struct SegmentBase
{
    SegmentBaseType type;
    usize start;
    usize count;
    SegmentBase* next;
};

// 行存储节点
typedef struct
{
    usize row_start;    // 指向的行起始索引
    SegmentBase* node; // 指向的 BlockSegment 或 RowSegment
} SegmentNode;

// 行存储树
typedef struct
{
    SegmentBase* root_node;
    Vector* nodes;
} SegmentTree;

void SegmentTree_init(SegmentTree* tree);

void SegmentTree_deinit(SegmentTree* tree, void (*node_destroy)(SegmentBase*));

void segmentTree_append_segment(SegmentTree* tree, SegmentBase* segment);

// 列存储段
typedef struct
{
    EXTENDS(SegmentBase);
    block_id_t block_id; // 指向的块ID
    usize byte_offset; // 指向的字节偏移量
    BlockManager* block_manager; // 指向的块管理器
    Block* block; // 指向的块
} BlockSegment;

BlockSegment* BlockSegment_create1(BlockManager* manager, block_id_t block_id, usize offset,
                                   usize count);

BlockSegment* BlockSegment_create2(usize start);

void BlockSegment_destroy(SegmentBase* segment);

typedef struct
{
    BlockSegment* segment; // 指向的 BlockSegment
    usize bytes_offset;
} ColumnPointer;

typedef struct
{
    EXTENDS(SegmentBase);
    DataTable* table; // 指向的 DataTable
    ColumnPointer* columns;
    usize columns_count;
} RowSegment;

RowSegment* RowSegment_create(DataTable* table, usize start);

void RowSegment_destroy(RowSegment* segment);

SegmentBase* segmentTree_get_root_segment(SegmentTree* tree);

SegmentBase* SegmentTree_get_segment(SegmentTree* tree, usize index);

SegmentBase* segmentTree_get_last_segment(SegmentTree* tree);

data_ptr_t segment_get_data(BlockSegment* segment);

#endif