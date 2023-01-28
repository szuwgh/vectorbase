mod analyzer;
pub mod ann;
mod block;
mod disk;
mod query;
mod schema;
mod tokenize;
mod util;

use crate::block::ByteBlockPool;
use crate::block::SIZE_CLASS;
use crate::query::Query;
use crate::schema::Document;
use crate::schema::Value;

use crate::disk::StoreWriter;
use jiebars::Jieba;
use std::cell::RefCell;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Weak};
use std::usize;

pub struct IndexConfig {}

pub struct IndexWriter {
    field_cache: HashMap<String, FieldCache>,
    doc_id: usize,
    store_writer: StoreWriter,
    share_bytes_block: Arc<RefCell<ByteBlockPool>>,
    tokenizer: Jieba,
}

impl IndexWriter {
    fn new(config: IndexConfig) -> IndexWriter {
        Self {
            field_cache: HashMap::new(),
            doc_id: 0,
            store_writer: StoreWriter {},
            share_bytes_block: Arc::new(RefCell::new(ByteBlockPool::new())),
            tokenizer: Jieba::new().unwrap(),
        }
    }

    pub fn search(&mut self, name: &str, term: &str) {}

    pub fn add(&mut self, doc: &Document) -> Result<(), std::io::Error> {
        for field in doc.fields.iter() {
            let fw = self
                .field_cache
                .entry(field.name.clone())
                .or_insert(FieldCache::new(Arc::downgrade(&self.share_bytes_block)));
            match field.value() {
                // 这里要进行分词
                Value::Str(s) => {
                    //初步分词
                    self.tokenizer.cut_for_search(s).iter().try_for_each(
                        |token| -> Result<(), std::io::Error> {
                            //将每个词添加到倒排表中
                            fw.add(self.doc_id, token)?;
                            Ok(())
                        },
                    )?;
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
    pos_index: usize,
    log_num: usize,
    is_commit: bool,
    freq: u32,
}

impl Posting {
    fn new(doc_freq_index: usize, pos_index: usize) -> Posting {
        Self {
            last_doc_id: 0,
            doc_freq_index: doc_freq_index,
            pos_index: pos_index,
            log_num: 0,
            is_commit: false,
            freq: 0,
        }
    }
}

pub(crate) struct FieldCache {
    indexs: HashMap<String, Arc<RefCell<Posting>>>, // term --> posting list 后续换成 radix-tree
    share_bytes_block: Weak<RefCell<ByteBlockPool>>,
    commit_posting: Vec<Arc<RefCell<Posting>>>,
}

impl FieldCache {
    fn new(pool: Weak<RefCell<ByteBlockPool>>) -> FieldCache {
        Self {
            indexs: HashMap::new(),
            share_bytes_block: pool,
            commit_posting: Vec::new(),
        }
    }

    fn commit(&mut self) {}

    fn add(&mut self, doc_id: usize, token: &str) -> Result<(), std::io::Error> {
        if !self.indexs.contains_key(token) {
            let pool = self.share_bytes_block.upgrade().unwrap();
            let pos = (*pool).borrow_mut().new_bytes(SIZE_CLASS[1] * 2);
            self.indexs.insert(
                token.to_string(),
                Arc::new(RefCell::new(Posting::new(pos, pos - SIZE_CLASS[1]))),
            );
        }
        // 获取词典的倒排表
        let p = self.indexs.get(token).expect("get term posting list fail");
        // 获取bytes 池
        let pool = self.share_bytes_block.upgrade().unwrap();
        // 倒排表中加入文档id
        Self::add_doc(doc_id, &mut *p.borrow_mut(), &mut *pool.borrow_mut())?;
        let posting = (*p).borrow_mut();
        if !posting.is_commit {
            self.commit_posting.push(p.clone());
        }
        Ok(())
    }

    fn add_doc(
        doc_id: usize,
        posting: &mut Posting,
        block_pool: &mut ByteBlockPool,
    ) -> Result<(), std::io::Error> {
        if posting.last_doc_id == doc_id {
            posting.freq += 1;
        } else {
            Self::write_doc_freq(doc_id, posting, block_pool)?;
            posting.last_doc_id = doc_id;
        }
        Ok(())
    }

    fn write_doc_freq(
        doc_id: usize,
        posting: &mut Posting,
        block_pool: &mut ByteBlockPool,
    ) -> Result<(), std::io::Error> {
        if posting.freq == 1 {
            posting.doc_freq_index = block_pool.write_vusize(
                posting.doc_freq_index,
                (doc_id - posting.last_doc_id) << 1 | 1,
            )?;
        } else {
            let index = block_pool
                .write_vusize(posting.doc_freq_index, (doc_id - posting.last_doc_id) << 1)?;
            posting.doc_freq_index = block_pool.write_vu32(index, posting.freq)?;
        }
        Ok(())
    }

    fn add_pos(
        pos: usize,
        posting: &mut Posting,
        pool: Arc<RefCell<ByteBlockPool>>,
    ) -> Result<(), std::io::Error> {
        Ok(())
    }
}

struct IndexReader {}

impl IndexReader {
    fn search(query: &Query) {}
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tokenizer() {
        let jieba = Jieba::new().unwrap();
        //搜索引擎模式
        let words = jieba.cut_for_search("小明硕士，毕业于中国科学院计算所，后在日本京都大学深造");

        println!("【搜索引擎模式】:{}\n", words.join(" / "));
    }
}
