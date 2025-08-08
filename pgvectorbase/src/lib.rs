use pgrx::prelude::*;
use pgrx::PgMemoryContexts;
use serde::ser::Error;
use serde::ser::SerializeTuple;
use serde::Deserialize;
use serde::Deserializer;
use serde::Serialize;
use serde::Serializer;
use std::ptr::copy_nonoverlapping;
use wwml::shape::MAX_DIM;
use wwml::StorageProto;
use wwml::Tensor as WWTensor;
use wwml::TensorProto;
::pgrx::pg_module_magic!(name, version);

#[derive(PostgresType)]
pub struct Tensor(WWTensor);

impl Serialize for Tensor {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        if serializer.is_human_readable() {
            self.0.serialize(serializer)
        } else {
            // 计算总字节数：dtype(1) + shape长度(1) + shape每个维度(变长) + 数据长度(变长) + 数据本身
            let ww = &self.0;
            let shape = ww.shape();
            let shape_len = shape.len();
            if shape_len > MAX_DIM {
                return Err(S::Error::custom("Shape length exceeds 255 dimensions"));
            }

            // 手动构建紧凑二进制格式
            // 初始化元组序列化器
            let mut seq = serializer.serialize_tuple(0)?;

            // 1. 写入dtype（1字节）
            seq.serialize_element(&ww.dtype().as_u8())?;

            // 2. 写入shape维度数（1字节）
            seq.serialize_element(&(shape_len as u8))?;

            // 3. 写入shape每个维度（变长LEB128编码）
            for dim in shape {
                seq.serialize_element(dim)?;
            }
            let data = ww.storage().as_bytes();
            let data_len = data.len();

            seq.end()
        }
    }
}

impl<'de> Deserialize<'de> for Tensor {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        if deserializer.is_human_readable() {
            let ww = WWTensor::deserialize(deserializer)?;
            Ok(Tensor(ww))
        } else {
            todo!()
        }
    }
}

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
