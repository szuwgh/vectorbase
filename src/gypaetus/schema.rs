// 每一行数据
use super::ann::BoxedAnnIndex;

use super::util::common;
use super::util::error::GyResult;
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use serde_bytes::ByteBuf;
use std::collections::HashMap;
use std::sync::atomic::AtomicU64;
use varintrs::{Binary, ReadBytesVarExt, WriteBytesVarExt};

use std::fmt;
use std::io::Read;
use std::io::Write;

pub type DateTime = chrono::DateTime<chrono::Utc>;

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
    DATE,
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
            d: Document::from(field_values),
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
        VUInt(self.field_values.len() as u64).serialize(writer)?;
        for field_value in &self.field_values {
            field_value.serialize(writer)?;
        }
        Ok(())
    }

    fn deserialize<R: Read>(reader: &mut R) -> GyResult<Self> {
        let num_field_values = VUInt::deserialize(reader)?.val() as usize;
        let field_values = (0..num_field_values)
            .map(|_| FieldValue::deserialize(reader))
            .collect::<GyResult<Vec<FieldValue>>>()?;
        Ok(Document::from(field_values))
    }
}

impl Document {
    pub fn new(doc_id: DocID) -> Document {
        Self {
            doc_id: doc_id,
            field_values: Vec::new(),
        }
    }

    pub fn size(&self) -> usize {
        varintrs::vint_size!(self.field_values.len()) as usize
            + self.field_values.iter().map(|f| f.size()).sum::<usize>()
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

    fn from(field_values: Vec<FieldValue>) -> Document {
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

    #[inline]
    pub(crate) fn size(&self) -> usize {
        4
    }
}

impl BinarySerialize for FieldID {
    fn serialize<W: Write>(&self, writer: &mut W) -> GyResult<()> {
        self.0.serialize(writer)
    }

    fn deserialize<R: Read>(reader: &mut R) -> GyResult<FieldID> {
        u32::deserialize(reader).map(FieldID)
    }
}

//域定义
#[derive(Debug)]
pub struct FieldValue {
    field_id: FieldID,
    pub(crate) value: Value,
}

impl BinarySerialize for FieldValue {
    fn serialize<W: Write>(&self, writer: &mut W) -> GyResult<()> {
        self.field_id.serialize(writer)?;
        self.value.serialize(writer)
    }
    fn deserialize<R: Read>(reader: &mut R) -> GyResult<FieldValue> {
        // let field_id =
        todo!()
    }
}

impl FieldValue {
    fn text(text: &str) -> Self {
        Self {
            field_id: FieldID(0),
            value: Value::Str(text.to_string()),
        }
    }

    fn size(&self) -> usize {
        self.field_id.size() + self.value.size()
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

const STR_ENCODE: u8 = 0;
const I64_ENCODE: u8 = 1;
const U64_ENCODE: u8 = 2;
const I32_ENCODE: u8 = 3;
const U32_ENCODE: u8 = 4;
const F32_ENCODE: u8 = 5;
const F64_ENCODE: u8 = 6;
const DATE_ENCODE: u8 = 7;
const BYTES_ENCODE: u8 = 8;

#[derive(Debug)]
//域 值类型
pub enum Value {
    Str(String),
    I64(i64),
    U64(u64),
    I32(i32),
    U32(u32),
    F64(f64),
    F32(f32),
    Date(DateTime),
    Bytes(Vec<u8>),
}

impl BinarySerialize for Value {
    fn serialize<W: Write>(&self, writer: &mut W) -> GyResult<()> {
        match &self {
            Value::Str(s) => {
                STR_ENCODE.serialize(writer)?;
                s.serialize(writer)?;
            }
            Value::I64(i) => {
                I64_ENCODE.serialize(writer)?;
                i.serialize(writer)?;
            }
            Value::U64(u) => {
                U64_ENCODE.serialize(writer)?;
                u.serialize(writer)?;
            }
            Value::I32(i) => {
                I32_ENCODE.serialize(writer)?;
                i.serialize(writer)?;
            }
            Value::U32(u) => {
                U32_ENCODE.serialize(writer)?;
                u.serialize(writer)?;
            }
            Value::F64(f) => {
                F64_ENCODE.serialize(writer)?;
                f.serialize(writer)?;
            }
            Value::F32(f) => {
                F32_ENCODE.serialize(writer)?;
                f.serialize(writer)?;
            }
            Value::Date(d) => {
                DATE_ENCODE.serialize(writer)?;
                d.timestamp().serialize(writer)?;
            }
            Value::Bytes(b) => {
                BYTES_ENCODE.serialize(writer)?;
                b.serialize(writer)?;
            }
        }
        Ok(())
    }

    fn deserialize<R: Read>(reader: &mut R) -> GyResult<Self> {
        todo!();
        // let type_code = u8::deserialize(reader)?;
        // match type_code {
        //     STR_ENCODE => {}
        //     I64_ENCODE => {}
        //     U64_ENCODE => {}
        //     I32_ENCODE => {}
        //     U32_ENCODE => {}
        //     F64_ENCODE => {}
        //     F32_ENCODE => {}
        //     DATE_ENCODE => {}
        // }
        //  Ok(v)
    }
}

impl Value {
    pub fn size(&self) -> usize {
        match &self {
            Value::Str(s) => {
                let str_length = varintrs::vint_size!(s.as_bytes().len()) as usize;
                1 + str_length + s.as_bytes().len()
            }
            Value::I64(i) => 9,
            Value::U64(u) => 9,
            Value::I32(i) => 5,
            Value::U32(u) => 5,
            Value::F64(f) => 9,
            Value::F32(f) => 5,
            Value::Date(d) => 9,
            Value::Bytes(b) => {
                let str_length = varintrs::vint_size!(b.len()) as usize;
                1 + str_length + b.len()
            }
            _ => 0,
        }
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

impl<T: BinarySerialize> BinarySerialize for Vec<T> {
    fn serialize<W: Write>(&self, writer: &mut W) -> GyResult<()> {
        VUInt(self.len() as u64).serialize(writer)?;
        for it in self {
            it.serialize(writer)?;
        }
        Ok(())
    }
    fn deserialize<R: Read>(reader: &mut R) -> GyResult<Vec<T>> {
        let num_items = VUInt::deserialize(reader)?.val();
        let mut items: Vec<T> = Vec::with_capacity(num_items as usize);
        for _ in 0..num_items {
            let item = T::deserialize(reader)?;
            items.push(item);
        }
        Ok(items)
    }
}

impl BinarySerialize for String {
    fn serialize<W: Write>(&self, writer: &mut W) -> GyResult<()> {
        let data: &[u8] = self.as_bytes();
        VUInt(data.len() as u64).serialize(writer)?;
        writer.write_all(data)?;
        Ok(())
    }

    fn deserialize<R: Read>(reader: &mut R) -> GyResult<String> {
        let str_len = VInt::deserialize(reader)?.val() as usize;
        let mut result = String::with_capacity(str_len);
        reader.take(str_len as u64).read_to_string(&mut result)?;
        Ok(result)
    }
}

impl BinarySerialize for u8 {
    fn serialize<W: Write>(&self, writer: &mut W) -> GyResult<()> {
        writer.write_u8(*self)?;
        Ok(())
    }

    fn deserialize<R: Read>(reader: &mut R) -> GyResult<Self> {
        let v = reader.read_u8()?;
        Ok(v)
    }
}

impl BinarySerialize for i32 {
    fn serialize<W: Write>(&self, writer: &mut W) -> GyResult<()> {
        writer.write_i32::<BigEndian>(*self)?;
        Ok(())
    }

    fn deserialize<R: Read>(reader: &mut R) -> GyResult<Self> {
        let v = reader.read_i32::<BigEndian>()?;
        Ok(v as i32)
    }
}

impl BinarySerialize for u32 {
    fn serialize<W: Write>(&self, writer: &mut W) -> GyResult<()> {
        writer.write_u32::<BigEndian>(*self)?;
        Ok(())
    }

    fn deserialize<R: Read>(reader: &mut R) -> GyResult<Self> {
        let v = reader.read_u64::<BigEndian>()?;
        Ok(v as u32)
    }
}

impl BinarySerialize for i64 {
    fn serialize<W: Write>(&self, writer: &mut W) -> GyResult<()> {
        writer.write_i64::<BigEndian>(*self)?;
        Ok(())
    }

    fn deserialize<R: Read>(reader: &mut R) -> GyResult<Self> {
        let v = reader.read_i64::<BigEndian>()?;
        Ok(v)
    }
}

impl BinarySerialize for u64 {
    fn serialize<W: Write>(&self, writer: &mut W) -> GyResult<()> {
        writer.write_u64::<BigEndian>(*self)?;
        Ok(())
    }

    fn deserialize<R: Read>(reader: &mut R) -> GyResult<Self> {
        let v = reader.read_u64::<BigEndian>()?;
        Ok(v)
    }
}

impl BinarySerialize for f64 {
    fn serialize<W: Write>(&self, writer: &mut W) -> GyResult<()> {
        writer.write_f64::<BigEndian>(*self)?;
        Ok(())
    }

    fn deserialize<R: Read>(reader: &mut R) -> GyResult<Self> {
        let v = reader.read_f64::<BigEndian>()?;
        Ok(v)
    }
}

impl BinarySerialize for f32 {
    fn serialize<W: Write>(&self, writer: &mut W) -> GyResult<()> {
        writer.write_f32::<BigEndian>(*self)?;
        Ok(())
    }

    fn deserialize<R: Read>(reader: &mut R) -> GyResult<Self> {
        let v = reader.read_f32::<BigEndian>()?;
        Ok(v)
    }
}

#[derive(Debug)]
pub struct VUInt(pub u64);

impl VUInt {
    fn val(&self) -> u64 {
        self.0
    }
}

impl BinarySerialize for VUInt {
    fn serialize<W: Write>(&self, writer: &mut W) -> GyResult<()> {
        writer.write_vu64::<Binary>(self.0)?;
        Ok(())
    }

    fn deserialize<R: Read>(reader: &mut R) -> GyResult<Self> {
        let (v, _) = reader.read_vu64::<Binary>();
        Ok(VUInt(v))
    }
}

#[derive(Debug)]
pub struct VInt(pub i64);

impl BinarySerialize for VInt {
    fn serialize<W: Write>(&self, writer: &mut W) -> GyResult<()> {
        writer.write_vi64::<Binary>(self.0)?;
        Ok(())
    }

    fn deserialize<R: Read>(reader: &mut R) -> GyResult<Self> {
        let (v, _) = reader.read_vi64::<Binary>();
        Ok(VInt(v))
    }
}

impl VInt {
    fn val(&self) -> i64 {
        self.0
    }
}
