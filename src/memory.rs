use crate::ann::AnnIndex;
use crate::schema::Row;
use crate::schema::Value;

use std::collections::HashMap;

pub(crate) trait TextIndex {
    fn insert(&mut self, k: Vec<u8>, v: u64);
}

#[derive(Default)]
pub(crate) struct MemTable {
    indexs: HashMap<String, Box<dyn TextIndex + 'static>>,
    doc_id: u64,
}

impl MemTable {
    pub(crate) fn index_row(&mut self, doc: &Row) {
        for field in doc.fields.iter() {
            match &field.value {
                _ => {}
            };
        }
    }
}
