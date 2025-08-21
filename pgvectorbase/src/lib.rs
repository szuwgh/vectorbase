#![recursion_limit = "256"]
mod error;
mod hnsw;
use pgrx::error;
use pgrx::ffi::CString;
use pgrx::opname;
use pgrx::pg_extern;
use pgrx::pg_operator;
use pgrx::pg_sys::Datum;
use pgrx::pg_sys::Oid;
use pgrx::pgrx_sql_entity_graph::metadata::ArgumentError;
use pgrx::pgrx_sql_entity_graph::metadata::Returns;
use pgrx::pgrx_sql_entity_graph::metadata::ReturnsError;
use pgrx::pgrx_sql_entity_graph::metadata::SqlMapping;
use pgrx::pgrx_sql_entity_graph::metadata::SqlTranslatable;
use pgrx::FromDatum;
use pgrx::IntoDatum;
use std::alloc::Layout;
use std::ffi::CStr;
use std::ops::Deref;
use std::ops::DerefMut;
use std::ptr::NonNull;
use wwml::similarity::Similarity;
::pgrx::pg_module_magic!(name, version);

pgrx::extension_sql_file!("./sql/tensor_type.sql");
pgrx::extension_sql_file!("./sql/op_class.sql", finalize);

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
    creates = [Type(Vector)],
    requires = [vector_in, vector_out, vector_typmod_in, vector_typmod_out],
);

#[repr(C, align(8))]
pub(crate) struct VectorImpl {
    varlena: u32,
    len: u16,
    phantom: [f32; 0],
}

impl VectorImpl {
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

    fn into_raw(self) -> *mut VectorImpl {
        Box::into_raw(Box::new(self))
    }

    fn varlena(size: usize) -> u32 {
        (size << 2) as u32
    }

    fn layout(len: usize) -> Layout {
        u16::try_from(len).expect("Vector is too large.");
        let layout_alpha = Layout::new::<VectorImpl>();
        let layout_beta = Layout::array::<f32>(len).unwrap();
        let layout = layout_alpha.extend(layout_beta).unwrap().0;
        layout.pad_to_align()
    }
}

pub struct Vector(NonNull<VectorImpl>);

unsafe impl SqlTranslatable for Vector {
    fn argument_sql() -> Result<SqlMapping, ArgumentError> {
        Ok(SqlMapping::As(String::from("vector")))
    }
    fn return_sql() -> Result<Returns, ReturnsError> {
        Ok(Returns::One(SqlMapping::As(String::from("vector"))))
    }
}

impl IntoDatum for Vector {
    fn into_datum(self) -> Option<Datum> {
        Some(Datum::from(self.into_raw() as *mut ()))
    }

    fn type_oid() -> Oid {
        pgrx::wrappers::regtypein("vector")
    }
}

impl Vector {
    pub fn into_raw(self) -> *mut VectorImpl {
        let result = self.0.as_ptr();
        std::mem::forget(self);
        result
    }
}

impl Deref for Vector {
    type Target = VectorImpl;

    fn deref(&self) -> &Self::Target {
        unsafe { self.0.as_ref() }
    }
}

impl DerefMut for Vector {
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe { self.0.as_mut() }
    }
}

impl Drop for Vector {
    fn drop(&mut self) {
        unsafe {
            pgrx::pg_sys::pfree(self.0.as_ptr() as _);
        }
    }
}

unsafe impl pgrx::callconv::BoxRet for Vector {
    unsafe fn box_into<'fcx>(
        self,
        fcinfo: &mut pgrx::callconv::FcInfo<'fcx>,
    ) -> pgrx::datum::Datum<'fcx> {
        unsafe { fcinfo.return_raw_datum(Datum::from(self.into_raw() as *mut ())) }
    }
}

unsafe impl<'fcx> pgrx::callconv::ArgAbi<'fcx> for Vector {
    unsafe fn unbox_arg_unchecked(arg: pgrx::callconv::Arg<'_, 'fcx>) -> Self {
        unsafe { arg.unbox_arg_using_from_datum().unwrap() }
    }
}

impl FromDatum for Vector {
    unsafe fn from_polymorphic_datum(datum: Datum, is_null: bool, _typoid: Oid) -> Option<Self> {
        if is_null {
            None
        } else {
            let p = NonNull::new(datum.cast_mut_ptr::<VectorImpl>())?;
            let q =
                unsafe { NonNull::new(pgrx::pg_sys::pg_detoast_datum(p.cast().as_ptr()).cast())? };
            if p != q {
                Some(Vector(q))
            } else {
                let header = p.as_ptr();
                let vector = unsafe { (*header).as_borrowed() };
                Some(Vector(vector))
            }
        }
    }
}

#[pg_extern(immutable, parallel_safe, strict)]
fn vector_in(input: &CStr, _oid: Oid, typmod: i32) -> Vector {
    let value: Vec<f32> = serde_json::from_str(input.to_str().expect("UTF-8编码错误"))
        .unwrap_or_else(|e| {
            error!("反序列化失败: {}", e);
        });

    // 校验维度
    let dimension = value.len() as u16;
    let expected_dimension = typmod as u16; // 修饰符存储的维度
    if dimension != expected_dimension {
        error!(
            "维度不匹配: 预期 {}，实际 {}",
            expected_dimension, dimension
        );
    }
    unsafe {
        assert!(u16::try_from(value.len()).is_ok());
        let layout = VectorImpl::layout(value.len());
        let ptr = pgrx::pg_sys::palloc(layout.size()) as *mut VectorImpl;
        std::ptr::addr_of_mut!((*ptr).varlena).write(VectorImpl::varlena(layout.size()));
        std::ptr::addr_of_mut!((*ptr).len).write(value.len() as u16);
        std::ptr::copy_nonoverlapping(value.as_ptr(), (*ptr).phantom.as_mut_ptr(), value.len());
        Vector(NonNull::new(ptr).unwrap())
    }
}

#[pgrx::pg_extern(immutable, parallel_safe, strict)]
fn vector_out(vector: Vector) -> CString {
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

#[pg_extern]
fn tensor_typmod_in(typmod_str: &std::ffi::CStr) -> i32 {
    // 接收字符串形式的 typmod，格式通常形如 "(768)" 或 "768"
    let s = typmod_str.to_str().unwrap_or("");
    // 去掉左右括号并解析整数
    let s = s.trim().trim_start_matches('(').trim_end_matches(')');
    match s.parse::<i32>() {
        Ok(v) if v > 0 => v,
        _ => -1, // 返回 -1 表示无效 typmod（后续调用处需检查并报错）
    }
}

#[pg_extern]
fn tensor_typmod_out(typmod: i32) -> String {
    if typmod > 0 {
        format!("({})", typmod)
    } else {
        String::from("(unspecified)")
    }
}

// #[pg_extern]
// fn tensor_typmod_in(typmod_str: &std::ffi::CStr) -> i32 {
//     use pgrx::spi::Spi;
//     // accept forms like [768,512] or (768,512) or 768
//     let s = typmod_str.to_str().unwrap_or("").trim();
//     if s.is_empty() {
//         return -1;
//     }
//     // normalize: allow surrounding () or []
//     let inner = s
//         .trim_start_matches('(')
//         .trim_end_matches(')')
//         .trim_start_matches('[')
//         .trim_end_matches(']')
//         .trim();

//     // single number also allowed
//     let parts: Vec<&str> = inner
//         .split(',')
//         .map(|p| p.trim())
//         .filter(|p| !p.is_empty())
//         .collect();
//     if parts.is_empty() {
//         return -1;
//     }
//     // validate all positive integers
//     for p in &parts {
//         if p.parse::<i32>().ok().filter(|v| *v > 0).is_none() {
//             return -1;
//         }
//     }
//     let canonical = format!("[{}]", parts.join(","));

//     // lookup or insert in mapping table
//     let esc = canonical.replace('\'', "''");
//     // try select
//     if let Ok(Some(id)) = Spi::get_one::<i32>(&format!(
//         "SELECT id FROM tensor_typmod_map WHERE dims = '{}' LIMIT 1",
//         esc
//     )) {
//         return id;
//     }
//     // insert
//     match Spi::get_one::<i32>(&format!(
//         "INSERT INTO tensor_typmod_map(dims) VALUES ('{}') RETURNING id",
//         esc
//     )) {
//         Ok(Some(id)) => id,
//         _ => -1,
//     }
// }

// #[pg_extern]
// fn tensor_typmod_out(typmod: i32) -> String {
//     use pgrx::spi::Spi;
//     if typmod <= 0 {
//         return String::from("(unspecified)");
//     }
//     let q = format!(
//         "SELECT dims FROM tensor_typmod_map WHERE id = {} LIMIT 1",
//         typmod
//     );
//     match Spi::get_one::<String>(&q) {
//         Ok(Some(dims)) => dims,
//         _ => String::from("(unspecified)"),
//     }
// }

#[pg_extern(immutable, strict, parallel_safe)]
fn euclidean_distance(a: Vector, b: Vector) -> f32 {
    // 自定义距离逻辑
    a.0.euclidean(&b.0)
}

#[pg_operator(immutable, strict, parallel_safe)]
#[opname(<->)]
fn operator_euclidean_distance(a: Vector, b: Vector) -> f32 {
    euclidean_distance(a, b)
}

// #[pg_extern]
// fn hnswbuild(
//     heap: pg_sys::Relation,
//     index: pg_sys::Relation,
//     index_info: pg_sys::IndexInfo,
// ) -> *mut pg_sys::IndexBuildResult {
//     return None;
// }

#[pg_extern]
fn hello_pgvectorbase() -> &'static str {
    "Hello, pgvectorbase"
}

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use pgrx::prelude::*;

    #[pg_test]
    fn test_hello_pgvectorbase() {
        assert_eq!("Hello, pgvectorbase", crate::hello_pgvectorbase());
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
