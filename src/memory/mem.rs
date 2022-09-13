use crate::knn::KnnIndex;
use crate::schema::field::Value;
use crate::schema::Document;

use std::collections::HashMap;

pub(crate) trait TextIndex {
    fn insert(&mut self, k: Vec<u8>, v: u64);
}

#[derive(Default)]
pub(crate) struct MemTable {
    indexs: HashMap<String, Box<dyn TextIndex + 'static>>,
    vectors: HashMap<String, Box<dyn KnnIndex + 'static>>,
    doc_id: u64,
}

impl MemTable {
    pub(crate) fn index_document(&mut self, doc: &Document) {
        for field in doc.fields.iter() {
            match &field.value {
                Value::Vector(v) => {
                    //  let index = self.vectors.get(&field.name).get_or_insert(value);
                }
                _ => {}
            };
        }
    }
}
