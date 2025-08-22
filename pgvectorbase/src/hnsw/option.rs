use crate::error::VBResult;
use crate::Vector;
use pgrx::pg_sys;
use pgrx::pg_sys::FmgrInfo;
use pgrx::pg_sys::MemoryContext;
use std::ptr;

const HNSW_MAGIC_NUMBER: u32 = 0xA953A953;
const HNSW_DEFAULT_M: i32 = 16; // 默认 M 参数
const HNSW_MAX_DIM: i32 = 2000; // 最大支持维度
const HNSW_PAGE_ID: u16 = 0xFF90;
/// HNSW 索引选项结构体（与 C 端完全兼容）
#[repr(C)]
struct HnswOptions {
    vl_len_: i32,         // varlena 头部 (必须作为第一个字段)
    m: i32,               // 最大连接数
    ef_construction: i32, // 构建时的动态候选列表大小
}

pub(crate) struct HnswBuildState {
    heap: pg_sys::Relation,
    index: pg_sys::Relation,
    index_info: *mut pg_sys::IndexInfo,
    fork_num: pg_sys::ForkNumber::Type, //用于标识表或索引的不同物理文件分支 MAIN_FORKNUM 主数据文件分支，存储实际数据

    // HNSW 参数
    m: i32,
    ef_construction: i32,
    dimensions: i32,

    // 统计信息
    pub(crate) reltuples: f64,
    pub(crate) indtuples: f64,

    // 图结构
    elements: Vec<HnswElement>,
    entry_point: Option<Box<HnswElement>>,
    ml: f64,
    max_level: i32,
    max_in_memory_elements: f64,
    flushed: bool,
    normvec: Option<Vector>,

    // 支持函数
    procinfo: Option<FmgrInfo>,
    normprocinfo: Option<FmgrInfo>,
    collation: pg_sys::Oid,

    // 内存上下文
    tmp_ctx: MemoryContext,
}

/// HNSW 元页面数据结构
#[repr(C)]
pub struct HnswMetaPageData {
    pub magic_number: u32,                 // 魔数标识
    pub version: u32,                      // 版本号
    pub dimensions: u32,                   // 向量维度
    pub m: u16,                            // 最大连接数
    pub ef_construction: u16,              // 构建时候选集大小
    pub entry_blkno: pg_sys::BlockNumber,  // 入口点块号
    pub entry_offno: pg_sys::OffsetNumber, // 入口点偏移
    pub entry_level: i16,                  // 入口点层级
    pub insert_page: pg_sys::BlockNumber,  // 插入页面
}

/// HNSW 页面特殊数据
#[repr(C)]
pub struct HnswPageOpaqueData {
    pub nextblkno: pg_sys::BlockNumber, // PostgreSQL 块号类型
    pub unused: u16,                    // 未使用字段
    pub page_id: u16,                   // 页面标识符
}

impl HnswBuildState {
    pub(crate) fn new() -> Self {
        Self {
            heap: ptr::null_mut(),
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
        self.heap = heap_rel;
        self.index = index_rel;
        self.index_info = index_info;
        self.fork_num = fork_num;

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

        // 7. 创建临时内存上下文
        unsafe {
            self.tmp_ctx = pg_sys::AllocSetContextCreateInternal(
                pg_sys::CurrentMemoryContext,
                "Hnsw build temporary context".as_ptr() as *const _,
                pg_sys::ALLOCSET_DEFAULT_MINSIZE as usize,
                pg_sys::ALLOCSET_DEFAULT_INITSIZE as usize,
                pg_sys::ALLOCSET_DEFAULT_MAXSIZE as usize,
            );
        }

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
        self.flush_page()?;
        self.free()?;
        Ok(())
    }

    pub(crate) fn free(&mut self) -> VBResult<()> {
        // 释放内存上下文
        if !self.tmp_ctx.is_null() {
            unsafe {
                pgrx::pg_sys::MemoryContextDelete(self.tmp_ctx);
            }
            self.tmp_ctx = std::ptr::null_mut();
        }
        // 清理元素列表
        self.elements.clear();
        self.entry_point = None;
        Ok(())
    }

    // PostgreSQL 页面布局：
    // +-------------------+ <-- page (0x1000)
    // | PageHeaderData    | 24字节
    // +-------------------+ <-- metap (0x1018)
    // | HnswMetaPageData  | 40字节
    // +-------------------+ <-- metap_end (0x1040) pd_lower
    // | 空闲空间          |
    // |                   |
    // | ...               |
    // |                   |
    // +-------------------+ <-- pd_upper
    // | 未使用空间        |
    // +-------------------+
    /// 创建索引元数据页
    pub(crate) fn create_meta_page(&self) -> VBResult<()> {
        unsafe {
            let index: pg_sys::Relation = self.index;
            let fork_num = self.fork_num;
            let buf: pg_sys::Buffer = hnsw_new_buffer(index, fork_num);
            let (state, page): (*mut pg_sys::GenericXLogState, pg_sys::Page) =
                hnsw_init_register_page(index, buf);
            let metap = hnsw_page_get_meta(page);
            (*metap).magic_number = HNSW_MAGIC_NUMBER; // HNSW 魔数
            (*metap).version = 1; // 版本号
            (*metap).dimensions = self.dimensions as u32;
            (*metap).m = self.m as u16;
            (*metap).ef_construction = self.ef_construction as u16;
            (*metap).entry_blkno = pg_sys::InvalidBlockNumber;
            (*metap).entry_offno = pg_sys::InvalidOffsetNumber;
            (*metap).entry_level = -1; // 初始入口点层级为 -1
            (*metap).insert_page = pg_sys::InvalidBlockNumber;
            //设置页面头部的 pd_lower 指向页面内空闲空间的起始位置
            let page_header = page as *mut pg_sys::PageHeaderData;
            let metap_end = (metap as *mut u8).add(std::mem::size_of::<HnswMetaPageData>());
            let page_start = page as *mut u8;
            let pd_lower = metap_end.offset_from(page_start) as usize;
            (*page_header).pd_lower = pd_lower as u16;
            hnsw_commit_buffer(buf, state)
        }
    }

    pub(crate) fn flush_page(&mut self) -> VBResult<()> {
        self.create_meta_page()?;
        self.flushed = true;
        Ok(())
    }
}

// Rust 函数:
unsafe fn hnsw_page_get_meta(page: pg_sys::Page) -> *mut HnswMetaPageData {
    pg_sys::PageGetContents(page) as *mut HnswMetaPageData
}

/// # 安全
/// 调用者必须确保缓冲区和状态指针有效
pub unsafe fn hnsw_commit_buffer(
    buf: pg_sys::Buffer,
    state: *mut pg_sys::GenericXLogState,
) -> VBResult<()> {
    pg_sys::MarkBufferDirty(buf);
    pg_sys::GenericXLogFinish(state);
    pg_sys::UnlockReleaseBuffer(buf);
    Ok(())
}

unsafe fn hnsw_new_buffer(
    index: pg_sys::Relation,
    fork_num: pg_sys::ForkNumber::Type,
) -> pg_sys::Buffer {
    // 使用 pgrx 的缓冲区分配函数
    let buf: pg_sys::Buffer = pg_sys::ReadBufferExtended(
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

/// 初始化并注册页面 (替代 HnswInitRegisterPage)
/// # 安全
/// 调用者必须确保所有指针有效
unsafe fn hnsw_init_register_page(
    index: pg_sys::Relation,
    buf: pg_sys::Buffer,
) -> (*mut pg_sys::GenericXLogState, pg_sys::Page) {
    // 1. 开始通用 XLog
    let state = pg_sys::GenericXLogStart(index);

    // 2. 注册缓冲区
    let page = pg_sys::GenericXLogRegisterBuffer(state, buf, pg_sys::GENERIC_XLOG_FULL_IMAGE as i32)
        as pg_sys::Page;
    // 3. 初始化页面
    hnsw_init_page(buf, page);
    (state, page)
}

unsafe fn hnsw_init_page(buf: pg_sys::Buffer, page: pg_sys::Page) {
    // 1. 获取页面大小
    let page_size = pg_sys::BufferGetPageSize(buf);
    // 2. 初始化页面结构
    pg_sys::PageInit(page, page_size, std::mem::size_of::<HnswPageOpaqueData>());
    // 3. 设置特殊数据
    let opaque = pg_sys::PageGetSpecialPointer(page) as *mut HnswPageOpaqueData;
    (*opaque).nextblkno = pg_sys::InvalidBlockNumber;
    (*opaque).page_id = HNSW_PAGE_ID;
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
    pub vec: *mut Vector,
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
