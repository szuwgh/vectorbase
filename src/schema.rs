// 每一行数据
pub(crate) struct Document {
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
}

pub enum Column {
    Str(String),
    I64(i64),
    U64(u64),
    F64(f64),
}
