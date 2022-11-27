mod analyzer;
pub mod ann;
mod block;
mod disk;
mod query;
mod schema;
mod tokenize;
mod util;

use crate::block::ByteBlockPool;
use crate::query::Query;
use crate::schema::Document;
use crate::schema::Value;

use std::cell::RefCell;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Weak};

pub struct IndexConfig {}

pub struct IndexWriter {
    field_cache: HashMap<String, FieldCache>,
    doc_id: u64,
    store_writer: StoreWriter,
    share_bytes_block: Arc<RefCell<ByteBlockPool>>,
}

impl IndexWriter {
    fn new() -> IndexWriter {
        Self {
            field_cache: HashMap::new(),
            doc_id: 0,
            store_writer: StoreWriter {},
            share_bytes_block: Arc::new(RefCell::new(ByteBlockPool::new())),
        }
    }

    pub fn add(&mut self, doc: &Document) -> Result<(), std::io::Error> {
        for field in doc.fields.iter() {
            let fw = self
                .field_cache
                .entry(field.name.clone())
                .or_insert(FieldCache::new(Arc::downgrade(&self.share_bytes_block)));
            match field.value() {
                // 这里要进行分词
                Value::Str(s) => {
                    fw.add(self.doc_id, &s)?;
                }
                _ => {}
            };
        }
        Ok(())
    }

    // commit 之后文档能搜索得到
    fn commit(&mut self) {}

    // 自动flush到磁盘中
    fn set_auto_flush(&mut self, path: PathBuf) {}

    // flush到磁盘中
    fn flush(&mut self, path: PathBuf) {}
}

// 倒排表
struct Posting {
    last_doc_id: usize,
    doc_freq_index: usize,
}

impl Posting {
    fn new(doc_freq_index: usize) -> Posting {
        Self {
            last_doc_id: 0,
            doc_freq_index: doc_freq_index,
        }
    }
}

pub(crate) struct FieldCache {
    indexs: HashMap<String, Posting>, // term --> posting list 后续换成 radix-tree
    share_bytes_block: Weak<RefCell<ByteBlockPool>>,
}

impl FieldCache {
    fn new(pool: Weak<RefCell<ByteBlockPool>>) -> FieldCache {
        Self {
            indexs: HashMap::new(),
            share_bytes_block: pool,
        }
    }

    fn add(&mut self, doc_id: u64, token: &str) -> Result<(), std::io::Error> {
        if !self.indexs.contains_key(token) {
            let pool = self.share_bytes_block.upgrade().unwrap();
            let pos = (*pool).borrow_mut().new_bytes(10);
            self.indexs.insert(token.to_string(), Posting::new(pos));
        }
        // 获取词典的倒排表
        let posting_list = self
            .indexs
            .get_mut(token)
            .expect("get term posting list fail");

        let pool = self.share_bytes_block.upgrade().unwrap();
        // 倒排表中加入文档id
        posting_list.doc_freq_index = (*pool)
            .borrow_mut()
            .write_u64(posting_list.doc_freq_index, doc_id)?;
        Ok(())
        // posting_list.add_doc(doc_id)
    }
}

pub(crate) struct StoreWriter {}

struct IndexReader {}

impl IndexReader {
    fn search(query: &Query) {}
}
