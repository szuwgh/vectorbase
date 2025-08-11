use byteorder::LittleEndian;
use byteorder::ReadBytesExt;
use pgrx::prelude::*;
use serde::de;
use serde::ser;
use serde::Deserialize;
use serde::Deserializer;
use serde::Serialize;
use serde::Serializer;
use std::io::BufRead;
use std::io::Cursor;
use wwml::shape::MAX_DIM;
use wwml::GGmlType;
use wwml::Shape;
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
                return Err(ser::Error::custom("Shape length exceeds 255 dimensions"));
            }
            let data = ww.storage().as_bytes();
            // 创建一个字节缓冲区
            let total_len = 1 + 1 + shape_len * 4 + data.len();
            let mut buffer = Vec::with_capacity(total_len);
            // 写入 dtype（1 字节）
            buffer.push(ww.dtype().as_u8());
            // 写入 shape 长度（1 字节）
            buffer.push(shape_len as u8);

            // 写入 shape 的每个维度（每个 4 字节）
            for &dim in shape {
                buffer.extend_from_slice(&(dim as u32).to_le_bytes());
            }
            // 写入数据本身
            buffer.extend_from_slice(data);
            // 使用 serializer 序列化字节缓冲区
            serializer.serialize_bytes(&buffer)
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
            //todo!();
            // 读取原始字节流
            let bytes: &[u8] = serde::Deserialize::deserialize(deserializer)?;
            let mut cursor = Cursor::new(bytes);
            let dtype = GGmlType::from_u8(cursor.read_u8().map_err(serde::de::Error::custom)?);
            let n_dims = cursor.read_u8().map_err(serde::de::Error::custom)? as usize;
            if n_dims > MAX_DIM {
                return Err(serde::de::Error::custom(
                    "Shape length exceeds 255 dimensions",
                ));
            }
            let mut shape = [1; MAX_DIM];
            for i in 0..n_dims {
                let dim = cursor
                    .read_u32::<LittleEndian>()
                    .map_err(serde::de::Error::custom)?;
                shape[i] = dim as usize;
            }
            let data = cursor.fill_buf().map_err(serde::de::Error::custom)?;
            let ww = unsafe {
                WWTensor::from_bytes(
                    data,
                    n_dims,
                    Shape::from_array(shape),
                    dtype,
                    &wwml::Device::Cpu,
                )
                .map_err(|e| {
                    de::Error::custom(format!("WWTensor::from_bytes failed:{}", e.to_string()))
                })?
            };
            Ok(Tensor(ww))
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
