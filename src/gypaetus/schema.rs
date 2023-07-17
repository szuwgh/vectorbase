// 每一行数据
use super::ann::BoxedAnnIndex;

use std::collections::HashMap;
use std::sync::atomic::AtomicU64;

//pub type VecID = u64;
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
    docID: DocID,
    pub field_values: Vec<FieldValue>,
}

impl Document {
    fn new() -> Document {
        Self {
            docID: 0,
            field_values: Vec::new(),
        }
    }

    fn with(field_values: Vec<FieldValue>) -> Document {
        Self {
            docID: 0,
            field_values: field_values,
        }
    }

    pub fn add_field_value(mut self, field: FieldValue) -> Self {
        self.field_values.push(field);
        self
    }

    pub fn get_sort_fieldvalues(&mut self, field_id: &HashMap<String, FieldID>) -> &[FieldValue] {
        self.field_values
            .sort_by_key(|field_value| field_id.get(field_value.field()).unwrap().0);
        &self.field_values
    }
}

pub struct FieldID(pub u32);

//域定义
pub struct FieldValue {
    field_id: FieldID,
    pub(crate) name: String,
    pub(crate) value: Value,
}

impl FieldValue {
    fn text(name: &str, text: &str) -> Self {
        Self {
            name: name.to_string(),
            field_id: FieldID(0),
            value: Value::Str(text.to_string()),
        }
    }

    fn new(name: &str, value: Value) -> Self {
        Self {
            name: name.to_string(),
            field_id: FieldID(0),
            value: value,
        }
    }

    pub fn field_id(&self) -> &FieldID {
        &self.field_id
    }
    pub fn field(&self) -> &str {
        &self.name
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
        match self {
            Value::Str(s) => s.to_string(),
            Value::I64(i) => i.to_string(),
            Value::I32(i) => i.to_string(),
            Value::U64(i) => i.to_string(),
            Value::U32(i) => i.to_string(),
            Value::F64(i) => i.to_string(),
            Value::F32(i) => i.to_string(),
            _ => "".to_string(),
        }
    }

    pub fn to_vec(&self) -> Vec<u8> {
        match self {
            Value::Str(s) => Vec::new(),
            Value::I64(i) => Vec::new(),
            Value::I32(i) => Vec::new(),
            Value::U64(i) => Vec::new(),
            Value::U32(i) => Vec::new(),
            Value::F64(i) => Vec::new(),
            Value::F32(i) => Vec::new(),
            _ => Vec::new(),
        }
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
