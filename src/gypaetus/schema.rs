// 每一行数据
use super::ann::BoxedAnnIndex;

use super::util::common;
use super::util::error::GyResult;
use byteorder::{BigEndian, WriteBytesExt};
use serde_bytes::ByteBuf;
use std::collections::HashMap;
use std::sync::atomic::AtomicU64;

use std::fmt;
use std::io::Read;
use std::io::Write;

pub trait BinarySerialize: fmt::Debug + Sized {
    /// Serialize
    fn serialize<W: Write>(&self, writer: &mut W) -> GyResult<()>;
    /// Deserialize
    fn deserialize<R: Read>(reader: &mut R) -> GyResult<Self>;
}

//pub type VecID = u64;
pub type DocID = u64;

#[derive(Default)]
pub struct Schema {
    pub fields: Vec<FieldEntry>,
    pub fields_map: HashMap<String, FieldID>,
}

impl Schema {
    pub fn new() -> Schema {
        Schema::default()
    }

    pub fn get_field(&self, field_name: &str) -> Option<FieldID> {
        self.fields_map.get(field_name).cloned()
    }

    //添加一个域
    pub fn add_field(&mut self, field_entry: FieldEntry) {
        let field_id = self.fields.len();
        let field_name = field_entry.name().to_string();
        self.fields.push(field_entry);
        self.fields_map
            .insert(field_name, FieldID::from_field_id(field_id as u32));
    }
}

pub struct FieldEntry {
    name: String,
    field_type: FieldType,
}

impl FieldEntry {
    pub(crate) fn str(field_name: &str) -> FieldEntry {
        FieldEntry {
            name: field_name.to_string(),
            field_type: FieldType::Str,
        }
    }

    pub(crate) fn i64(field_name: &str) -> FieldEntry {
        FieldEntry {
            name: field_name.to_string(),
            field_type: FieldType::I64,
        }
    }

    pub(crate) fn i32(field_name: &str) -> FieldEntry {
        FieldEntry {
            name: field_name.to_string(),
            field_type: FieldType::I32,
        }
    }

    pub(crate) fn u64(field_name: &str) -> FieldEntry {
        FieldEntry {
            name: field_name.to_string(),
            field_type: FieldType::U64,
        }
    }

    pub(crate) fn name(&self) -> &str {
        &self.name
    }

    pub fn field_type(&self) -> &FieldType {
        &self.field_type
    }
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

pub struct Vector<V> {
    pub v: V,
    pub d: Document,
}

impl<V> Vector<V> {
    pub fn with(v: V) -> Vector<V> {
        Self {
            v: v,
            d: Document::new(0),
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

#[derive(Debug)]
pub struct Document {
    doc_id: DocID,
    pub field_values: Vec<FieldValue>,
}

impl BinarySerialize for Document {
    fn serialize<W: Write>(&self, writer: &mut W) -> GyResult<()> {
        todo!()
    }

    fn deserialize<R: Read>(reader: &mut R) -> GyResult<Self> {
        todo!()
    }
}

impl Document {
    pub fn new(doc_id: DocID) -> Document {
        Self {
            doc_id: doc_id,
            field_values: Vec::new(),
        }
    }

    pub fn add_u64(&mut self, field: FieldID, value: u64) {
        self.add_field_value(FieldValue::new(field, Value::U64(value)));
    }

    pub fn add_i32(&mut self, field: FieldID, value: i32) {
        self.add_field_value(FieldValue::new(field, Value::I32(value)));
    }

    pub fn add_i64(&mut self, field: FieldID, value: i64) {
        self.add_field_value(FieldValue::new(field, Value::I64(value)));
    }

    pub fn add_str(&mut self, field: FieldID, value: &str) {
        self.add_field_value(FieldValue::new(field, Value::Str(value.to_string())));
    }

    fn with(field_values: Vec<FieldValue>) -> Document {
        Self {
            doc_id: 0,
            field_values: field_values,
        }
    }

    pub fn add_field_value(&mut self, field: FieldValue) {
        self.field_values.push(field);
    }

    pub fn sort_fieldvalues(&mut self) {
        self.field_values
            .sort_by_key(|field_value| field_value.field_id.0);
    }
}

#[derive(Debug, Clone, Copy)]
pub struct FieldID(pub u32);

impl FieldID {
    pub(crate) fn from_field_id(field_id: u32) -> FieldID {
        FieldID(field_id)
    }
}

//域定义
#[derive(Debug)]
pub struct FieldValue {
    field_id: FieldID,
    pub(crate) value: Value,
}

impl FieldValue {
    fn text(text: &str) -> Self {
        Self {
            field_id: FieldID(0),
            value: Value::Str(text.to_string()),
        }
    }

    fn new(field_id: FieldID, value: Value) -> Self {
        Self {
            field_id: field_id,
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

#[derive(Debug)]
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

    pub fn to_vec(&self) -> GyResult<Vec<u8>> {
        match &self {
            Value::Str(s) => Ok(s.as_bytes().to_vec()),
            Value::I64(i64) => {
                let mut v1 = vec![0u8; 8];
                i64.serialize(&mut v1)?;
                Ok(v1)
            }
            Value::I32(i32) => {
                let mut v2 = vec![0u8; 0];
                i32.serialize(&mut v2)?;
                Ok(v2)
            }

            _ => Ok(Vec::new()),
        }
    }
}

impl BinarySerialize for i32 {
    fn serialize<W: Write>(&self, writer: &mut W) -> GyResult<()> {
        writer.write_i32::<BigEndian>(*self)?;
        Ok(())
    }

    fn deserialize<R: Read>(reader: &mut R) -> GyResult<Self> {
        todo!()
    }
}

impl BinarySerialize for i64 {
    fn serialize<W: Write>(&self, writer: &mut W) -> GyResult<()> {
        let val_u64: u64 = common::i64_to_u64(*self);
        writer.write_u64::<BigEndian>(val_u64)?;
        Ok(())
    }

    fn deserialize<R: Read>(reader: &mut R) -> GyResult<Self> {
        todo!()
    }
}
