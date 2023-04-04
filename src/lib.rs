mod ann;
mod block;
mod disk;
mod query;
mod schema;
mod tokenize;
mod util;
use crate::ann::BoxedAnnIndex;
use crate::block::{ByteBlockPool, ByteBlockReader, SIZE_CLASS};
use crate::query::Query;
use crate::schema::{Schema, Value, Vector, VectorEntry};
use std::marker::PhantomData;

use crate::ann::{Create, Metric, HNSW};
use jiebars::Jieba;
use std::cell::RefCell;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Weak};
use std::usize;

type VecID = usize;

pub struct IndexConfig {}

pub type Index = IndexBase<Vec<f32>>;

//#[derive(Clone)]
pub struct IndexBase<T: 'static>
where
    T: Metric<T> + Create,
{
    vector_index: BoxedAnnIndex<T>,
    field_cache: Vec<FieldCache>,
    vec_id: VecID,
    buffer: Arc<RefCell<ByteBlockPool>>,
    schema: Schema,
    tokenizer: Jieba,
}

trait DocPosting {}

impl<T: 'static> IndexBase<T>
where
    T: Metric<T> + Create,
{
    fn new(schema: Schema) -> IndexBase<T> {
        let buffer_pool = Arc::new(RefCell::new(ByteBlockPool::new()));
        let mut field_cache: Vec<FieldCache> = Vec::new();
        for s in schema.fields.iter() {
            field_cache.push(FieldCache::new(Arc::downgrade(&buffer_pool)));
        }

        Self {
            vector_index: Self::get_vector_index(&schema.vector),
            field_cache: field_cache,
            vec_id: 0,
            buffer: buffer_pool,
            schema: schema,
            tokenizer: Jieba::new().unwrap(),
        }
    }

    pub fn get_vector_index(vec_type: &VectorEntry) -> BoxedAnnIndex<T> {
        BoxedAnnIndex(Box::new(HNSW::<T>::new(32)))
    }

    //add vector
    pub fn add(&mut self, vec: Vector<T>) -> Result<(), std::io::Error> {
        //添加向量标签
        for field in vec.field_values.iter() {
            let fw = self
                .field_cache
                .get_mut(field.field_id().0 as usize)
                .unwrap();
            fw.add(self.vec_id, field.value())?;
        }
        //添加向量
        self.vector_index.0.insert(vec.into(), self.vec_id);
        self.vec_id += 1;
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
    byte_addr: usize,
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
            byte_addr: doc_freq_index,
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

    // fn search(&mut self) -> FieldCache {}

    fn commit(&mut self) -> Result<(), std::io::Error> {
        let pool = self.share_bytes_block.upgrade().unwrap();
        self.commit_posting
            .iter()
            .try_for_each(|posting| -> Result<(), std::io::Error> {
                let p = &mut *posting.borrow_mut();
                Self::write_doc_freq(p.last_doc_id, p, &mut *pool.borrow_mut())?;
                Ok(())
            })?;
        self.commit_posting.clear();
        Ok(())
    }

    //添加 token 单词
    fn add(&mut self, doc_id: usize, value: &Value) -> Result<(), std::io::Error> {
        match value {
            Value::Str(s) => {
                if !self.indexs.contains_key(s) {
                    let pool = self.share_bytes_block.upgrade().unwrap();
                    let pos = (*pool).borrow_mut().new_bytes(SIZE_CLASS[1] * 2);
                    self.indexs.insert(
                        s.to_string(),
                        Arc::new(RefCell::new(Posting::new(pos, pos + SIZE_CLASS[1]))),
                    );
                }
                // 获取词典的倒排表
                let p = self.indexs.get(s).expect("get term posting list fail");
                // 获取bytes 池
                let pool = self.share_bytes_block.upgrade().unwrap();
                // 倒排表中加入文档id
                Self::add_doc(doc_id, &mut *p.borrow_mut(), &mut *pool.borrow_mut())?;
                if !(*p).borrow_mut().is_commit {
                    self.commit_posting.push(p.clone());
                }
            }
            _ => {}
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

pub struct PostingReader {
    id_reader: ByteBlockReader,
    lastVecID: VecID,
}

impl PostingReader {}

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

    #[test]
    fn test_fieldcache() {
        let mut b = ByteBlockPool::new();
        // let jieba = Jieba::new().unwrap();
        // //搜索引擎模式
        // let words = jieba.cut_for_search("小明硕士，毕业于中国科学院计算所，后在日本京都大学深造");

        // println!("【搜索引擎模式】:{}\n", words.join(" / "));

        // let jieba = Jieba::new().unwrap();
        // //搜索引擎模式
        // let words = jieba.cut_for_search("小明硕士，毕业于中国科学院计算所，后在日本京都大学深造");

        // println!("【搜索引擎模式】:{}\n", words.join(" / "));
    }
}
