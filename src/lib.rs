pub mod ann;
mod disk;
mod memory;
mod query;
mod schema;
mod tokenize;
mod util;

use crate::ann::AnnIndex;
use crate::memory::BytesBlock;
use crate::query::Query;
use crate::schema::Document;
use crate::schema::Value;
use std::collections::HashMap;

pub struct IndexConfig {}

pub struct IndexWriter {
    field_writers: HashMap<String, FieldWriter>,
    doc_id: usize,
    store_writer: StoreWriter,
    share_bytes_block: BytesBlock,
}

impl IndexWriter {
    fn new() -> IndexWriter {
        Self {
            field_writers: HashMap::new(),
            doc_id: 0,
            store_writer: StoreWriter {},
            share_bytes_block: BytesBlock {},
        }
    }

    pub(crate) fn add(&mut self, doc: &Document) {
        for field in doc.fields.iter() {
            let fw = self
                .field_writers
                .entry(field.name.clone())
                .or_insert(FieldWriter {
                    indexs: HashMap::new(),
                });
            match &field.value {
                // 这里要进行分词
                Value::Str(s) => fw.add(s),
                _ => {}
            };
        }
    }

    // commit 能搜索得到
    fn commit(&mut self) {}

    // flush到磁盘中
    fn flush(&mut self) {}
}

struct PostingList {}

impl PostingList {
    fn add_doc(&mut self) {}
}

pub(crate) struct FieldWriter {
    indexs: HashMap<String, PostingList>, // term --> posting list
}

impl FieldWriter {
    fn add(&mut self, token: &str) {
        if !self.indexs.contains_key(token) {
            self.indexs.insert(token.to_string(), PostingList {});
        }
        let posting_list = self
            .indexs
            .get_mut(token)
            .expect("get term posting list fail");
        posting_list.add_doc()
    }
}

pub(crate) struct StoreWriter {}

struct IndexDiskWriter {}

struct IndexReader {}

impl IndexReader {
    fn search(query: &Query) {}
}
