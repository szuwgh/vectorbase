use std::collections::HashMap;
use std::fmt::Display;
pub(crate) trait TextIndex {
    fn insert(&mut self, k: &str, v: u64);
}

pub(crate) trait VectorIndex {
    fn insert(&mut self, k: Vec<f32>, v: u64);
}

#[derive(Default)]
pub(crate) struct MemTable {
    indexs: HashMap<String, Box<dyn TextIndex + 'static>>,
    vectors: HashMap<String, Box<dyn VectorIndex + 'static>>,
}

impl MemTable {
    fn index_document() {}
}
