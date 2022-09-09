use super::field::Field;

pub(crate) struct Document {
    fields: Vec<Field>,
}

impl Document {
    pub fn with_field(fields: Vec<Field>) -> Document {
        Self { fields: fields }
    }
    pub fn new() -> Document {
        Self { fields: Vec::new() }
    }

    pub fn add(field: Field) {}

    pub(crate) fn build() {}
}
