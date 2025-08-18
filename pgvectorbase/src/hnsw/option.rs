use pgrx::pg_sys;
use pgrx::pg_sys::FmgrInfo;
use pgrx::pg_sys::MemoryContext;

use crate::Tensor;
struct HnswBuildState {
    heap: Option<pg_sys::Relation>,
    index: pg_sys::Relation,
    index_info: *mut pg_sys::IndexInfo,
    fork_num: pg_sys::ForkNumber::Type,

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
