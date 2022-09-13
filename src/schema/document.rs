use super::Field;

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
