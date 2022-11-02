use crate::ann::AnnIndex;
use crate::schema::Row;
use crate::schema::Value;

use std::collections::HashMap;

pub(crate) trait TextIndex {
    fn insert(&mut self, k: Vec<u8>, v: u64);
}

pub(crate) struct MemTable {
    indexs: HashMap<String, Box<dyn TextIndex + 'static>>,
    doc_id: u64,
    store_field: StoreWriter,
}

pub(crate) struct StoreWriter {}

impl MemTable {
    pub(crate) fn new() -> MemTable {
        Self {
            indexs: HashMap::new(),
            doc_id: 0,
            store_field: StoreWriter {},
        }
    }
    pub(crate) fn index_row(&mut self, doc: &Row) {
        for field in doc.fields.iter() {
            match &field.value {
                _ => {}
            };
        }
    }
}
