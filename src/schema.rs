// 每一行数据
use crate::ann::BoxedAnnIndex;

use std::collections::HashMap;

pub struct Schema {
    pub vector: VectorEntry,
    pub fields: Vec<FieldEntry>,
    pub fields_map: HashMap<String, FieldID>,
}

pub enum VectorEntry {
    Hnsw,
    Annoy,
    IvfFlat,
    LSH,
    PQ,
    Flat,
}

pub enum FieldEntry {
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
    pub fn set_vector(&mut self, vector: VectorEntry) {
        self.vector = vector
    }

    //添加一个域
    pub fn add_field(&mut self, field_entry: FieldEntry) {}

    //注册索引方案
    pub fn register_index() {}
}

pub struct Vector<V> {
    pub v: V,
    pub field_values: Vec<FieldValue>,
}

impl<V> Vector<V> {
    pub fn with(v: V) -> Vector<V> {
        Self {
            v: v,
            field_values: Vec::new(),
        }
    }

    pub fn into() {}

    pub fn with_fields(v: V, field_values: Vec<FieldValue>) -> Vector<V> {
        Self {
            v: v,
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
    pub(crate) field_id: FieldID,
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

pub enum Column {
    Str(String),
    I64(i64),
    I32(i32),
    U64(u64),
    U32(u32),
    F64(f64),
    F32(f32),
}
