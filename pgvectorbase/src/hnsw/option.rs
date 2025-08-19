use crate::error::VBResult;
use crate::Tensor;
use pgrx::pg_sys;
use pgrx::pg_sys::FmgrInfo;
use pgrx::pg_sys::MemoryContext;
use std::ptr;
/// HNSW 索引选项结构体（与 C 端完全兼容）
#[repr(C)]
struct HnswOptions {
    vl_len_: i32,         // varlena 头部 (必须作为第一个字段)
    m: i32,               // 最大连接数
    ef_construction: i32, // 构建时的动态候选列表大小
}

pub(crate) struct HnswBuildState {
    heap: Option<pg_sys::Relation>,
    index: pg_sys::Relation,
    index_info: *mut pg_sys::IndexInfo,
    fork_num: pg_sys::ForkNumber::Type, //用于标识表或索引的不同物理文件分支 MAIN_FORKNUM 主数据文件分支，存储实际数据

    // HNSW 参数
    m: i32,
    ef_construction: i32,
    dimensions: i32,

    // 统计信息
    reltuples: f64,
    indtuples: f64,

    // 图结构
    elements: Vec<HnswElement>,
    entry_point: Option<Box<HnswElement>>,
    ml: f64,
    max_level: i32,
    max_in_memory_elements: f64,
    flushed: bool,
    normvec: Option<Tensor>,

    // 支持函数
    procinfo: Option<FmgrInfo>,
    normprocinfo: Option<FmgrInfo>,
    collation: pg_sys::Oid,

    // 内存上下文
    tmp_ctx: MemoryContext,
}

impl HnswBuildState {
    pub(crate) fn new() -> Self {
        Self {
            heap: None,
            index: ptr::null_mut(),
            index_info: ptr::null_mut(),
            fork_num: pg_sys::ForkNumber::MAIN_FORKNUM, // 初始化为主分支
            m: 0,
            ef_construction: 0,
            dimensions: 0,
            reltuples: 0.0,
            indtuples: 0.0,
            elements: Vec::new(),
            entry_point: None,
            ml: 0.0,
            max_level: 0,
            max_in_memory_elements: 0.0,
            flushed: false,
            normvec: None,
            procinfo: None,
            normprocinfo: None,
            collation: pg_sys::InvalidOid,
            tmp_ctx: std::ptr::null_mut(),
        }
    }

    pub(crate) fn init(
        &mut self,
        heap_rel: pg_sys::Relation,
        index_rel: pg_sys::Relation,
        index_info: *mut pg_sys::IndexInfo,
        fork_num: pg_sys::ForkNumber::Type,
    ) -> VBResult<()> {
        // 保存基础关系
        self.heap = Some(heap_rel);
        self.index = index_rel;
        self.index_info = index_info;
        self.fork_num = fork_num; // 赋值枚举值

        // 获取索引参数
        self.m = hnsw_get_M(index_rel);
        self.ef_construction = hnsw_get_ef_construction(index_rel);

        // 获取向量维度
        self.dimensions = hnsw_get_dim(index_rel);
        // 验证参数
        if self.dimensions < 0 {
            return Err("column does not have dimensions".into());
        }

        if self.dimensions > HNSW_MAX_DIM {
            return Err(format!(
                "column cannot have more than {} dimensions for hnsw index",
                HNSW_MAX_DIM,
            )
            .into());
        }
        if self.ef_construction < 2 * self.m {
            return Err("ef_construction must be greater than or equal to 2 * m".into());
        }
        self.reltuples = 0.0;
        self.indtuples = 0.0;

        Ok(())
    }

    pub(crate) fn build_index(
        &mut self,
        heap_rel: pg_sys::Relation,
        index_rel: pg_sys::Relation,
        index_info: *mut pg_sys::IndexInfo,
        fork_num: pg_sys::ForkNumber::Type,
    ) -> VBResult<()> {
        self.init(heap_rel, index_rel, index_info, fork_num)?;
        Ok(())
    }

    /// 创建索引元数据页
    pub(crate) fn create_meta_page(&mut self) {
        unsafe {
            let index = self.index;
            let fork_num = self.fork_num;

            let buf = hnsw_new_buffer(index, fork_num);

            // l
        }
    }
}

const HNSW_DEFAULT_M: i32 = 16; // 默认 M 参数
const HNSW_MAX_DIM: i32 = 2000; // 最大支持维度

unsafe fn hnsw_new_buffer(
    index: pg_sys::Relation,
    fork_num: pg_sys::ForkNumber::Type,
) -> pg_sys::Buffer {
    // 使用 pgrx 的缓冲区分配函数
    let buf = pg_sys::ReadBufferExtended(
        index,
        fork_num,
        pg_sys::InvalidBlockNumber,
        pg_sys::ReadBufferMode::RBM_NORMAL,
        ptr::null_mut(),
    );

    // 锁定缓冲区
    pg_sys::LockBuffer(buf, pg_sys::BUFFER_LOCK_EXCLUSIVE as i32);

    buf
}

pub fn hnsw_get_dim(index_rel: pg_sys::Relation) -> i32 {
    unsafe {
        let relation = &*index_rel;
        let tupdesc = relation.rd_att;
        let first_attr = &*tupdesc;
        let dimensions = first_attr.attrs.as_slice(1)[0].atttypmod;
        dimensions
    }
}

pub fn hnsw_get_M(index_rel: pg_sys::Relation) -> i32 {
    unsafe {
        let relation = &*index_rel;
        // 获取选项指针
        let opts_ptr = relation.rd_options as *mut HnswOptions;
        if opts_ptr.is_null() {
            return HNSW_DEFAULT_M;
        }
        let opts = &*opts_ptr;
        let m = opts.m;
        m
    }
}

pub fn hnsw_get_ef_construction(index_rel: pg_sys::Relation) -> i32 {
    unsafe {
        let relation = &*index_rel;
        // 获取选项指针
        let opts_ptr = relation.rd_options as *mut HnswOptions;
        if opts_ptr.is_null() {
            return 0;
        }
        let opts = &*opts_ptr;
        opts.ef_construction
    }
}

type HnswElement = HnswElementData;

#[repr(C)] // 保证与 C 兼容的内存布局
pub struct HnswElementData {
    /// 堆元组 ID 列表（使用 PostgreSQL 的 List 结构）
    pub heaptids: *mut pg_sys::List,

    /// 元素所在层级 (0 表示底层)
    pub level: u8,

    /// 删除标记
    pub deleted: u8,

    /// 各层的邻居数组
    pub neighbors: *mut HnswNeighborArray,

    /// 磁盘位置：块号
    pub blkno: pg_sys::BlockNumber,

    /// 磁盘位置：偏移号
    pub offno: pg_sys::OffsetNumber,

    /// 邻居元组的偏移号
    pub neighbor_offno: pg_sys::OffsetNumber,

    /// 邻居元组所在块号
    pub neighbor_page: pg_sys::BlockNumber,

    /// 向量数据
    pub vec: *mut Tensor,
}

/// 邻居数组结构
#[repr(C)]
pub struct HnswNeighborArray {
    /// 邻居数量
    pub length: i32,

    /// 邻居候选列表
    pub items: *mut HnswCandidate,
}

/// 邻居候选元素
#[repr(C)]
pub struct HnswCandidate {
    /// 指向的图元素
    pub element: *mut HnswElement,

    /// 与查询向量的距离
    pub distance: f32,
}
