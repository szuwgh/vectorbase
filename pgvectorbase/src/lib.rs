#![recursion_limit = "256"]
mod error;
mod hnsw;
use pgrx::error;
use pgrx::ffi::CString;
use pgrx::opname;
use pgrx::pg_extern;
use pgrx::pg_operator;
use pgrx::pg_schema;
use pgrx::pg_sys::Datum;
use pgrx::pg_sys::Oid;
use pgrx::pgrx_sql_entity_graph::metadata::ArgumentError;
use pgrx::pgrx_sql_entity_graph::metadata::Returns;
use pgrx::pgrx_sql_entity_graph::metadata::ReturnsError;
use pgrx::pgrx_sql_entity_graph::metadata::SqlMapping;
use pgrx::pgrx_sql_entity_graph::metadata::SqlTranslatable;
use pgrx::Array;
use pgrx::FromDatum;
use pgrx::IntoDatum;
use std::alloc::Layout;
use std::ffi::CStr;
use std::ops::Deref;
use std::ops::DerefMut;
use std::ptr::NonNull;
::pgrx::pg_module_magic!(name, version);
use serde::Deserialize;
use serde::Serialize;
pgrx::extension_sql_file!("./sql/vector_type.sql");
//pgrx::extension_sql_file!("./sql/op_class.sql", finalize);

pgrx::extension_sql!(
    r#"
CREATE TYPE vector (
    INPUT     = vector_in,
    OUTPUT    = vector_out,
    TYPMOD_IN = vector_typmod_in,
    TYPMOD_OUT = vector_typmod_out,
    STORAGE   = EXTENDED,
    INTERNALLENGTH = VARIABLE,
    ALIGNMENT = double
);
"#,
    name = "vector",
    creates = [Type(vector)],
    requires = [vector_in, vector_out, vector_typmod_in, vector_typmod_out],
);

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum VectorTypmod {
    Any,
    Dims(u16),
}

impl VectorTypmod {
    pub fn parse_from_str(s: &str) -> Option<Self> {
        use VectorTypmod::*;
        if let Ok(x) = s.parse::<u16>() {
            Some(Dims(x))
        } else {
            None
        }
    }
    pub fn parse_from_i32(x: i32) -> Option<Self> {
        use VectorTypmod::*;
        if x == -1 {
            Some(Any)
        } else if u16::MIN as i32 <= x && x <= u16::MAX as i32 {
            Some(Dims(x as u16))
        } else {
            None
        }
    }
    pub fn into_option_string(self) -> Option<String> {
        use VectorTypmod::*;
        match self {
            Any => None,
            Dims(x) => Some(i32::from(x).to_string()),
        }
    }
    pub fn into_i32(self) -> i32 {
        use VectorTypmod::*;
        match self {
            Any => -1,
            Dims(x) => i32::from(x),
        }
    }
    pub fn dims(self) -> Option<u16> {
        use VectorTypmod::*;
        match self {
            Any => None,
            Dims(dims) => Some(dims),
        }
    }
}

#[repr(C, align(8))]
pub struct Vector {
    varlena: u32,
    len: u16,
    phantom: [f32; 0],
}

impl Vector {
    fn len(&self) -> usize {
        self.len as usize
    }

    pub fn data(&self) -> &[f32] {
        debug_assert_eq!(self.varlena & 3, 0);
        unsafe { std::slice::from_raw_parts(self.phantom.as_ptr(), self.len as usize) }
    }

    fn as_slice(&self) -> &[f32] {
        unsafe { std::slice::from_raw_parts(self.phantom.as_ptr(), self.len()) }
    }

    fn into_raw(self) -> *mut Vector {
        Box::into_raw(Box::new(self))
    }

    fn varlena(size: usize) -> u32 {
        (size << 2) as u32
    }

    fn layout(len: usize) -> Layout {
        u16::try_from(len).expect("vector is too large.");
        let layout_alpha = Layout::new::<Vector>();
        let layout_beta = Layout::array::<f32>(len).unwrap();
        let layout = layout_alpha.extend(layout_beta).unwrap().0;
        layout.pad_to_align()
    }
}

pub enum VectorPack<'a> {
    Owned(NonNull<Vector>),
    Borrowed(&'a mut Vector),
}

impl VectorPack<'_> {
    pub unsafe fn new(mut p: NonNull<Vector>) -> Self {
        let q = NonNull::new(pgrx::pg_sys::pg_detoast_datum(p.cast().as_ptr()).cast()).unwrap();
        if p != q {
            VectorPack::Owned(q)
        } else {
            VectorPack::Borrowed(p.as_mut())
        }
    }

    pub fn data(&self) -> &[f32] {
        self.deref().data()
    }
}

unsafe impl<'a> SqlTranslatable for VectorPack<'a> {
    fn argument_sql() -> Result<SqlMapping, ArgumentError> {
        Ok(SqlMapping::As(String::from("VectorPack")))
    }
    fn return_sql() -> Result<Returns, ReturnsError> {
        Ok(Returns::One(SqlMapping::As(String::from("VectorPack"))))
    }
}

impl<'a> IntoDatum for VectorPack<'a> {
    fn into_datum(self) -> Option<Datum> {
        Some(Datum::from(self.into_raw() as *mut ()))
    }

    fn type_oid() -> Oid {
        pgrx::wrappers::regtypein("VectorPack")
    }
}

impl<'a> VectorPack<'a> {
    pub fn into_raw(self) -> *mut Vector {
        match &self {
            VectorPack::Owned(x) => {
                let result = x.as_ptr();
                std::mem::forget(self);
                result
            }
            VectorPack::Borrowed(x) => todo!(),
        }
    }

    pub fn euclidean(&self, other: &Self) -> f32 {
        assert_eq!(self.as_slice().len(), other.as_slice().len());
        self.as_slice()
            .iter()
            .zip(other.as_slice().iter()) // 将两个切片的元素配对
            .map(|(l, r)| (l - r).powi(2)) // 计算差值的平方
            .sum::<f32>() // 求和
            .sqrt() // 计算平方根
    }
}

impl Deref for VectorPack<'_> {
    type Target = Vector;

    fn deref(&self) -> &Self::Target {
        match self {
            VectorPack::Owned(x) => unsafe { x.as_ref() },
            VectorPack::Borrowed(x) => x,
        }
    }
}

impl<'a> DerefMut for VectorPack<'a> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        match self {
            VectorPack::Owned(x) => unsafe { x.as_mut() },
            VectorPack::Borrowed(x) => x,
        }
    }
}

impl<'a> Drop for VectorPack<'a> {
    fn drop(&mut self) {
        match self {
            VectorPack::Owned(x) => unsafe {
                pgrx::pg_sys::pfree(x.as_ptr() as _);
            },
            VectorPack::Borrowed(x) => {}
        }
    }
}

unsafe impl<'a> pgrx::callconv::BoxRet for VectorPack<'a> {
    unsafe fn box_into<'fcx>(
        self,
        fcinfo: &mut pgrx::callconv::FcInfo<'fcx>,
    ) -> pgrx::datum::Datum<'fcx> {
        unsafe { fcinfo.return_raw_datum(Datum::from(self.into_raw() as *mut ())) }
    }
}

unsafe impl<'fcx> pgrx::callconv::ArgAbi<'fcx> for VectorPack<'fcx> {
    unsafe fn unbox_arg_unchecked(arg: pgrx::callconv::Arg<'_, 'fcx>) -> Self {
        unsafe { arg.unbox_arg_using_from_datum().unwrap() }
    }
}

impl<'a> FromDatum for VectorPack<'a> {
    unsafe fn from_polymorphic_datum(datum: Datum, is_null: bool, _typoid: Oid) -> Option<Self> {
        if is_null {
            None
        } else {
            let ptr = NonNull::new(datum.cast_mut_ptr::<Vector>()).unwrap();
            unsafe { Some(VectorPack::new(ptr)) }
        }
    }
}

#[pg_extern(immutable, parallel_safe, strict)]
fn vector_in(input: &CStr, _oid: Oid, typmod: i32) -> VectorPack {
    let value: Vec<f32> = serde_json::from_str(input.to_str().expect("UTF-8编码错误"))
        .unwrap_or_else(|e| {
            error!("反序列化失败: {}", e);
        });

    // 校验维度
    let dimension = value.len() as u16;
    let typmod = VectorTypmod::parse_from_i32(typmod).unwrap();
    let expected_dimension = typmod.dims().unwrap_or(0);
    if expected_dimension != 0 && dimension != expected_dimension {
        error!(
            "维度不匹配: 预期 {}，实际 {}",
            expected_dimension, dimension
        );
    }
    unsafe {
        assert!(u16::try_from(value.len()).is_ok());
        let layout = Vector::layout(value.len());
        let ptr = pgrx::pg_sys::palloc(layout.size()) as *mut Vector;
        std::ptr::addr_of_mut!((*ptr).varlena).write(Vector::varlena(layout.size()));
        std::ptr::addr_of_mut!((*ptr).len).write(value.len() as u16);
        std::ptr::copy_nonoverlapping(value.as_ptr(), (*ptr).phantom.as_mut_ptr(), value.len());
        VectorPack::Owned(NonNull::new(ptr).unwrap())
    }
}

#[pgrx::pg_extern(immutable, parallel_safe, strict)]
fn vector_out(vector: VectorPack) -> CString {
    let mut buffer = String::new();
    buffer.push('[');
    if let Some(&x) = vector.data().first() {
        buffer.push_str(format!("{}", x).as_str());
    }
    for &x in vector.data().iter().skip(1) {
        buffer.push_str(format!(", {}", x).as_str());
    }
    buffer.push(']');
    CString::new(buffer).unwrap()
}

#[pgrx::pg_extern(immutable, parallel_safe, strict)]
fn vector_typmod_in(list: Array<&CStr>) -> i32 {
    if list.is_empty() {
        -1
    } else if list.len() == 1 {
        let s = list.get(0).unwrap().unwrap().to_str().unwrap();
        let typmod = VectorTypmod::parse_from_str(s).expect("Invaild typmod.");
        typmod.into_i32()
    } else {
        panic!("Invaild typmod.");
    }
}

#[pgrx::pg_extern(immutable, parallel_safe, strict)]
fn vector_typmod_out(typmod: i32) -> CString {
    if typmod > 0 {
        CString::new(format!("({})", typmod)).unwrap()
    } else {
        CString::new("(unspecified)").unwrap()
    }
}

// #[pg_extern(immutable, strict, parallel_safe)]
// fn euclidean_distance(a: Vector, b: Vector) -> f32 {
//     // 自定义距离逻辑
//     a.euclidean(&b)
// }

// #[pg_operator(immutable, strict, parallel_safe)]
// #[opname(<->)]
// fn operator_euclidean_distance(a: Vector, b: Vector) -> f32 {
//     euclidean_distance(a, b)
// }

// #[pg_extern]
// fn hnswbuild(
//     heap: pg_sys::Relation,
//     index: pg_sys::Relation,
//     index_info: pg_sys::IndexInfo,
// ) -> *mut pg_sys::IndexBuildResult {
//     return None;
// }

#[pg_extern]
fn hello_pgVectorPackbase() -> &'static str {
    "Hello, pgVectorPackbase"
}

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use pgrx::prelude::*;

    #[pg_test]
    fn test_hello_pgVectorPackbase() {
        assert_eq!("Hello, pgVectorPackbase", crate::hello_pgVectorPackbase());
    }
}

/// This module is required by `cargo pgrx test` invocations.
/// It must be visible at the root of your extension crate.
#[cfg(test)]
pub mod pg_test {
    pub fn setup(_options: Vec<&str>) {
        // perform one-off initialization when the pg_test framework starts
    }

    #[must_use]
    pub fn postgresql_conf_options() -> Vec<&'static str> {
        // return any postgresql.conf settings that are required for your tests
        vec![]
    }
}
