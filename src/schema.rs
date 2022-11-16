// 每一行数据
pub struct Document {
    pub(crate) fields: Vec<Field>,
}

impl Document {
    pub fn with_field(fields: Vec<Field>) -> Document {
        Self { fields: fields }
    }
    pub fn new() -> Document {
        Self { fields: Vec::new() }
    }

    pub fn add(&mut self, field: Field) {
        self.fields.push(field);
    }
}

//域定义
pub struct Field {
    pub(crate) name: String,
    pub(crate) value: Value,
}

impl Field {
    fn text(name: &str, text: &str) -> Self {
        Self {
            name: name.to_string(),
            value: Value::Str(text.to_string()),
        }
    }

    fn new(name: &str, value: Value) -> Self {
        Self {
            name: name.to_string(),
            value: value,
        }
    }

    pub fn value(&self) -> &Value {
        &self.value
    }
}

//域 值类型
pub enum Value {
    Str(String),
    Tag(String),
    Column(Column),
    Bytes(Vec<u8>),
    U64(u64),
    Vector32(Vec<f32>),
    Vector64(Vec<f64>),
    Matrix(Matrix), //后期实现
}

pub struct Matrix {}

pub enum Column {
    Str(String),
    I64(i64),
    I32(i32),
    U64(u64),
    U32(u32),
    F64(f64),
    F32(f32),
}
