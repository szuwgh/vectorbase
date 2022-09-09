//域接口定义
pub struct Field {
    name: String,
    value: Value,
}

impl Field {
    fn text(name: &str) {}

    fn new(name: &str, value: Value) -> Self {
        Self {
            name: name.to_string(),
            value: value,
        }
    }
}
pub enum Value {
    Str(String),
    Tag(String),
    Column(Column),
    Vector(Vec<f32>),
    Bytes(Vec<u8>),
    U64(u64),
}

pub enum Column {
    Str(String),
    U64(u64),
    F64(f64),
    I64(i64),
}
