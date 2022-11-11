mod analyzer;
pub mod ann;
mod disk;
mod memory;
mod query;
mod schema;
mod tokenize;
mod util;

use crate::ann::AnnIndex;
use crate::memory::ByteBlockPool;
use crate::query::Query;
use crate::schema::Document;
use crate::schema::Value;
use std::collections::HashMap;

pub struct IndexConfig {}

pub struct IndexWriter {
    field_writers: HashMap<String, FieldWriter>,
    doc_id: usize,
    store_writer: StoreWriter,
    share_bytes_block: ByteBlockPool,
}

impl IndexWriter {
    fn new() -> IndexWriter {
        Self {
            field_writers: HashMap::new(),
            doc_id: 0,
            store_writer: StoreWriter {},
            share_bytes_block: ByteBlockPool::new(),
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
            match field.value() {
                // 这里要进行分词
                Value::Str(s) => fw.add(self.doc_id, &s),
                _ => {}
            };
        }
    }

    // commit 能搜索得到
    fn commit(&mut self) {}

    // flush到磁盘中
    fn flush(&mut self) {}
}

// 倒排表
struct Posting {
    last_doc_id: usize,
}

impl Posting {
    fn new() -> Posting {
        Self { last_doc_id: 0 }
    }

    fn add_doc(&mut self, doc_id: usize) {}
}

pub(crate) struct FieldWriter {
    indexs: HashMap<String, Posting>, // term --> posting list 后续换成 radix-tree
}

impl FieldWriter {
    fn add(&mut self, doc_id: usize, token: &str) {
        if !self.indexs.contains_key(token) {
            self.indexs.insert(token.to_string(), Posting::new());
        }
        // 获取词典的倒排表
        let posting_list = self
            .indexs
            .get_mut(token)
            .expect("get term posting list fail");
        // 倒排表中加入文档id
        posting_list.add_doc(doc_id)
    }
}

pub(crate) struct StoreWriter {}

struct IndexDiskWriter {}

struct IndexReader {}

impl IndexReader {
    fn search(query: &Query) {}
}
