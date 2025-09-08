use super::option::HnswBuildState;
use crate::datatype::Vector;
use crate::error::VBResult;
use crate::hnsw::option::HnswCandidate;
use crate::hnsw::option::HnswElement;
use crate::hnsw::option::HnswElementData;
use core::ffi::c_void;
use pgrx::info;
use pgrx::notice;
use pgrx::pg_sys;
use pgrx::pg_sys::list_make1_impl;
use pgrx::pg_sys::palloc;
use pgrx::pg_sys::Datum;
use pgrx::pg_sys::DatumGetFloat8;
use pgrx::pg_sys::FmgrInfo;
use pgrx::pg_sys::FunctionCall2Coll;
use pgrx::pg_sys::MemoryContextReset;
use pgrx::pg_sys::MemoryContextSwitchTo;
use pgrx::pg_sys::Oid;
use pgrx::pg_sys::PointerGetDatum;
use std::ptr;
use std::ptr::copy_nonoverlapping;

pub(crate) unsafe fn get_candidate_distance(
    hc: *mut HnswCandidate,
    q: Datum,
    procinfo: *mut FmgrInfo,
    collation: Oid,
) -> f32 {
    DatumGetFloat8(FunctionCall2Coll(
        procinfo,
        collation,
        q,
        PointerGetDatum((*(*hc).element).vec.cast()),
    )) as f32
}

pub(crate) unsafe fn hnsw_entry_candidate(
    entry_point: HnswElement,
    q: Datum,
    index: pg_sys::Relation,
    procinfo: *mut FmgrInfo,
    collation: Oid,
    load_vec: bool,
) -> *mut HnswCandidate {
    let hc = palloc(size_of::<HnswCandidate>()) as *mut HnswCandidate;
    (*hc).element = entry_point;
    if index.is_null() {
        (*hc).distance = get_candidate_distance();
    } else {
    }
}
