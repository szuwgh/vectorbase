// 每一行数据
use super::ann::BoxedAnnIndex;

use super::util::error::GyResult;
use serde::{Deserialize, Serialize};
use serde_bytes::ByteBuf;
use std::collections::HashMap;
use std::sync::atomic::AtomicU64;

use std::fmt;
use std::io::Read;
use std::io::Write;
use varintrs::{vint_size, Binary, ReadBytesVarExt, WriteBytesVarExt};

pub trait BinarySerialize: Sized {
    /// Serialize
    fn serialize<W: Write>(&self, writer: &mut W) -> GyResult<usize>;
    /// Deserialize
    fn deserialize<R: Read>(reader: &mut R) -> GyResult<Self>;
}

pub type DocID = u64;

pub struct Schema {
    pub fields: Vec<FieldType>,
    pub fields_map: HashMap<String, FieldID>,
}

pub enum VectorType {
    Flat,
    BinFlat,
    IvfFlat,
    BinIvfFlat,
    IvfPQ,
    IvfSQ8,
    IvfSQ8H,
    NSG,
    HNSW,
    RHNSWFlat,
    RHNSWPQ,
    RHNSWSQ,
    IvfHNSW,
    ANNOY,
    NGTPANNG,
    NGTONNG,
}

pub enum FieldType {
    Str,
    I64,
    I32,
    U64,
    U32,
    F64,
    F32,
    Bytes,
}

impl Schema {
    // pub fn set_vector(&mut self, vector: VectorType) {
    //     self.vector = vector
    // }

    //添加一个域
    pub fn add_field(&mut self, field_entry: FieldType) {}

    //注册索引方案
    pub fn register_index() {}
}

pub struct Vector<V> {
    pub v: V,
    pub d: Document,
}

impl<V> Vector<V> {
    pub fn with(v: V) -> Vector<V> {
        Self {
            v: v,
            d: Document::new(),
        }
    }

    pub fn into(self) -> V {
        self.v
    }

    pub fn with_fields(v: V, field_values: Vec<FieldValue>) -> Vector<V> {
        Self {
            v: v,
            d: Document::with(field_values),
        }
    }
}

pub struct Document {
    doc_id: DocID,
    pub field_values: Vec<FieldValue>,
}

impl Document {
    fn new() -> Document {
        Self {
            doc_id: 0,
            field_values: Vec::new(),
        }
    }

    fn with(field_values: Vec<FieldValue>) -> Document {
        Self {
            doc_id: 0,
            field_values: field_values,
        }
    }

    pub fn add_field_value(mut self, field: FieldValue) -> Self {
        self.field_values.push(field);
        self
    }

    pub fn get_field_values(&self) -> &[FieldValue] {
        &self.field_values
    }

    pub fn sort_fieldvalues(&mut self) {
        self.field_values
            .sort_by_key(|field_value| field_value.field_id.0);
    }

    //pub fn
    // pub fn get_sort_fieldvalues(&mut self, field_id: &HashMap<String, FieldID>) -> &[FieldValue] {
    //     self.field_values
    //         .sort_by_key(|field_value| field_id.get(field_value.field()).unwrap().0);
    //     &self.field_values
    // }
}

impl BinarySerialize for Document {
    /// Serialize
    fn serialize<W: Write>(&self, writer: &mut W) -> GyResult<usize> {
        // let
        Ok(0)
    }
    /// Deserialize
    fn deserialize<R: Read>(reader: &mut R) -> GyResult<Self> {
        todo!()
    }
}

pub struct VInt(pub u64);

impl BinarySerialize for VInt {
    /// Serialize
    fn serialize<W: Write>(&self, writer: &mut W) -> GyResult<usize> {
        let i = writer.write_leb128_u64::<Binary>(self.0)?;
        Ok(i)
    }
    /// Deserialize
    fn deserialize<R: Read>(reader: &mut R) -> GyResult<Self> {
        let v = reader.read_led128_u64::<Binary>()?;
        Ok(VInt(v))
    }
}

pub struct FieldID(pub u32);

//域定义
pub struct FieldValue {
    field_id: FieldID,
    pub(crate) value: Value,
}

impl FieldValue {
    fn text(name: &str, text: &str) -> Self {
        Self {
            field_id: FieldID(0),
            value: Value::Str(text.to_string()),
        }
    }

    fn new(name: &str, value: Value) -> Self {
        Self {
            field_id: FieldID(0),
            value: value,
        }
    }

    pub fn field_id(&self) -> &FieldID {
        &self.field_id
    }

    pub fn value(&self) -> &Value {
        &self.value
    }
}

//域 值类型
pub enum Value {
    Str(String),
    I64(i64),
    I32(i32),
    U64(u64),
    U32(u32),
    F64(f64),
    F32(f32),
    Bytes(Vec<u8>),
}

impl Value {
    pub fn to_string(&self) -> String {
        todo!()
    }

    pub fn to_vec(&self) -> Vec<u8> {
        todo!()
    }
}

impl Serialize for Value {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        todo!()
    }
}

pub enum Column {
    Str(String),
    I64(i64),
    I32(i32),
    U64(u64),
    U32(u32),
    F64(f64),
    F32(f32),
}
