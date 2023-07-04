use crate::schema::FieldValue;
pub struct Query {
    And: Vec<FieldValue>,
    Should: Vec<FieldValue>,
}
