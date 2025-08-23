#![recursion_limit = "256"]
mod datatype;
mod error;
mod hnsw;
use pgrx::pg_extern;
use pgrx::pg_schema;
::pgrx::pg_module_magic!(name, version);
pgrx::extension_sql_file!("./sql/vector_type.sql");
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
fn hello_pgvectorbase() -> &'static str {
    "Hello, pgvectorbase"
}

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use pgrx::prelude::*;

    #[pg_test]
    fn test_hello_pgvectorbase() {
        assert_eq!("Hello, pgVectorInputbase", crate::hello_pgvectorbase());
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
