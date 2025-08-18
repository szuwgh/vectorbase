use pgrx::pg_extern;
use pgrx::pg_guard;
use pgrx::pg_sys;
use pgrx::PgBox;

/// 简化：统一的错误抛出工具
#[inline(always)]
fn unimplemented_err(func: &str) -> ! {
    pgrx::error!("hnsw_am: {} is not implemented yet", func)
}

/// ------------------------------
/// 必要的回调“占位”实现（均抛错）
/// ------------------------------

#[pg_guard]
unsafe extern "C-unwind" fn hnsw_ambuild(
    _heap_rel: pg_sys::Relation,
    _index_rel: pg_sys::Relation,
    _index_info: *mut pg_sys::IndexInfo,
) -> *mut pg_sys::IndexBuildResult {
    unimplemented_err("ambuild")
}

#[pg_guard]
unsafe extern "C-unwind" fn hnsw_ambuildempty(_index_rel: pg_sys::Relation) {
    unimplemented_err("ambuildempty")
}

#[pg_guard]
unsafe extern "C-unwind" fn hnsw_aminsert(
    _index_rel: pg_sys::Relation,
    _values: *mut pg_sys::Datum,
    _isnull: *mut bool,
    _ht_ctid: *mut pg_sys::ItemPointerData,
    _heap_rel: pg_sys::Relation,
    checkUnique: pg_sys::IndexUniqueCheck::Type,
    _index_unmodified: bool,
    _index_info: *mut pg_sys::IndexInfo,
) -> bool {
    unimplemented_err("aminsert")
}

#[pg_guard]
unsafe extern "C-unwind" fn hnsw_ambulkdelete(
    _info: *mut pg_sys::IndexVacuumInfo,
    _stats: *mut pg_sys::IndexBulkDeleteResult,
    _callback: pg_sys::IndexBulkDeleteCallback,
    _callback_state: *mut std::os::raw::c_void,
) -> *mut pg_sys::IndexBulkDeleteResult {
    unimplemented_err("ambulkdelete")
}

#[pg_guard]
unsafe extern "C-unwind" fn hnsw_amvacuumcleanup(
    _info: *mut pg_sys::IndexVacuumInfo,
    _stats: *mut pg_sys::IndexBulkDeleteResult,
) -> *mut pg_sys::IndexBulkDeleteResult {
    unimplemented_err("amvacuumcleanup")
}

#[pg_guard]
unsafe extern "C-unwind" fn hnsw_amcanreturn(_index_rel: pg_sys::Relation, _attno: i32) -> bool {
    // 本示例不支持 index-only return
    false
}

#[pg_guard]
unsafe extern "C-unwind" fn hnsw_amoptions(
    _relopts: pg_sys::Datum,
    _validate: bool,
) -> *mut pg_sys::varlena {
    // 无自定义 reloptions
    std::ptr::null_mut()
}

// #[pg_guard]
// unsafe extern "C-unwind" fn hnsw_amproperty(
//     _index_rel: pg_sys::Relation,
//     _attno: i32,
//     _prop: pg_sys::IndexAMProperty,
//     _propstr: *const std::os::raw::c_char,
//     _res: *mut bool,
// ) -> bool {
//     // 不支持任何特殊属性查询
//     false
// }

#[pg_guard]
unsafe extern "C-unwind" fn hnsw_amcostestimate(
    _root: *mut pg_sys::PlannerInfo,
    _path: *mut pg_sys::IndexPath,
    _loop_count: f64,
    _index_startup_cost: *mut f64,
    _index_total_cost: *mut f64,
    _index_selectivity: *mut f64,
    _index_correlation: *mut f64,
    _index_pages: *mut f64,
) {
    unimplemented_err("amcostestimate")
}

#[pg_guard]
unsafe extern "C-unwind" fn hnsw_ambeginscan(
    _index_rel: pg_sys::Relation,
    _nkeys: i32,
    _norderbys: i32,
) -> pg_sys::IndexScanDesc {
    unimplemented_err("ambeginscan")
}

#[pg_guard]
unsafe extern "C-unwind" fn hnsw_amrescan(
    _scan: pg_sys::IndexScanDesc,
    _keys: pg_sys::ScanKey,
    _nkeys: i32,
    _orderbys: pg_sys::ScanKey,
    _norderbys: i32,
) {
    unimplemented_err("amrescan")
}

#[pg_guard]
unsafe extern "C-unwind" fn hnsw_amgettuple(
    _scan: pg_sys::IndexScanDesc,
    _direction: pg_sys::ScanDirection::Type,
) -> bool {
    unimplemented_err("amgettuple")
}

#[pg_guard]
unsafe extern "C-unwind" fn hnsw_amgetbitmap(
    _scan: pg_sys::IndexScanDesc,
    _tbm: *mut pg_sys::TIDBitmap,
) -> i64 {
    unimplemented_err("amgetbitmap")
}

#[pg_guard]
unsafe extern "C-unwind" fn hnsw_amendscan(_scan: pg_sys::IndexScanDesc) {
    unimplemented_err("amendscan")
}

#[pg_guard]
unsafe extern "C-unwind" fn hnsw_ammarkpos(_scan: pg_sys::IndexScanDesc) {
    unimplemented_err("ammarkpos")
}

#[pg_guard]
unsafe extern "C-unwind" fn hnsw_amrestrpos(_scan: pg_sys::IndexScanDesc) {
    unimplemented_err("amrestrpos")
}

#[pg_guard]
unsafe extern "C-unwind" fn hnsw_amestimateparallelscan(
    _rel: pg_sys::Relation,
    _nkeys: i32,
    _norderbys: i32,
) -> usize {
    0
}

#[pg_guard]
unsafe extern "C-unwind" fn hnsw_aminitparallelscan(
    _rel: pg_sys::Relation,
    _pstate: *mut std::os::raw::c_void,
    _nkeys: i32,
    _norderbys: i32,
) {
    // no-op
}

#[pg_guard]
unsafe extern "C-unwind" fn hnsw_amparallelrescan(
    _rel: pg_sys::Relation,
    _pstate: *mut std::os::raw::c_void,
) {
    // no-op
}

#[pg_guard]
unsafe extern "C-unwind" fn hnswvalidate(opclassoid: pg_sys::Oid) -> bool {
    // 在原始 PostgreSQL 实现中始终返回 true
    // 这表示所有操作符类都被认为是有效的
    true
}

/// --------------------------------------
/// Handler：返回 IndexAmRoutine 给 PostgreSQL
/// --------------------------------------
/// 注意：在 SQL 层它的返回类型是 `index_am_handler`（伪类型）。

#[pg_extern(sql = "
    CREATE OR REPLACE FUNCTION hnsw_am_handler(internal) RETURNS index_am_handler
    PARALLEL SAFE IMMUTABLE STRICT LANGUAGE c AS 'MODULE_PATHNAME', '@FUNCTION_NAME@';
    CREATE ACCESS METHOD hnsw_am TYPE INDEX HANDLER hnsw_am_handler;
")]
pub fn hnsw_am_handler(
    fcinfo: pgrx::PgBox<pg_sys::FunctionCallInfo>,
) -> pgrx::PgBox<pg_sys::IndexAmRoutine> {
    // 分配 IndexAmRoutine（放在 PG 的内存中）
    let mut am =
        unsafe { PgBox::<pg_sys::IndexAmRoutine>::alloc_node(pg_sys::NodeTag::T_IndexAmRoutine) };

    // 固定属性（可按需修改）
    am.amstrategies = 0; // 无策略（如 B-Tree 的策略编号）
    am.amsupport = 0; // 无支持函数个数
    am.amcanorder = false.into();
    am.amcanorderbyop = false.into();
    am.amcanbackward = false.into();
    am.amcanunique = false.into();
    am.amcanmulticol = true.into(); // 示例：允许多列
    am.amoptionalkey = true.into();
    am.amsearcharray = false.into();
    am.amsearchnulls = false.into();
    am.amstorage = false.into();
    am.amclusterable = false.into();
    am.ampredlocks = false.into();
    am.amcanparallel = false.into();
    am.amvalidate = Some(hnswvalidate);
    //am.amsupports_bitmap = false.into();
    // am.amvalidate_opclass = false.into();
    am.amkeytype = pg_sys::InvalidOid; // "generic" key type

    // 必要回调函数指针
    am.ambuild = Some(hnsw_ambuild);
    am.ambuildempty = Some(hnsw_ambuildempty);
    am.aminsert = Some(hnsw_aminsert);
    am.ambulkdelete = Some(hnsw_ambulkdelete);
    am.amvacuumcleanup = Some(hnsw_amvacuumcleanup);
    am.amcanreturn = Some(hnsw_amcanreturn);
    am.amoptions = Some(hnsw_amoptions);
    am.amproperty = None; //Some(hnsw_amproperty);
    am.amcostestimate = Some(hnsw_amcostestimate);

    // 扫描接口
    am.ambeginscan = Some(hnsw_ambeginscan);
    am.amrescan = Some(hnsw_amrescan);
    am.amgettuple = Some(hnsw_amgettuple);
    am.amgetbitmap = Some(hnsw_amgetbitmap);
    am.amendscan = Some(hnsw_amendscan);
    am.ammarkpos = Some(hnsw_ammarkpos);
    am.amrestrpos = Some(hnsw_amrestrpos);

    // 并行扫描（可选）
    // am.amestimateparallelscan = Some(hnsw_amestimateparallelscan);
    // am.aminitparallelscan = Some(hnsw_aminitparallelscan);
    // am.amparallelrescan = Some(hnsw_amparallelrescan);

    am.into_pg_boxed()
}
