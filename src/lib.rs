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
type Addr = usize;

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
    last_vec_id: VecID,
    byte_addr: Addr,
    vec_freq_addr: Addr,
    pos_addr: Addr,
    vec_num: usize,
    freq: u32,
    add_commit: bool,
}

impl Posting {
    fn new(vec_freq_addr: Addr, pos_addr: Addr) -> Posting {
        Self {
            last_vec_id: 0,
            byte_addr: vec_freq_addr,
            vec_freq_addr: vec_freq_addr,
            pos_addr: pos_addr,
            vec_num: 0,
            add_commit: false,
            freq: 0,
        }
    }
}

pub(crate) struct FieldCache {
    indexs: HashMap<String, Arc<RefCell<Posting>>>,
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
                Self::write_vec_freq(p.last_vec_id, p, &mut *pool.borrow_mut())?;
                p.add_commit = false;
                Ok(())
            })?;
        self.commit_posting.clear();
        Ok(())
    }

    //添加 token 单词
    fn add(&mut self, vec_id: VecID, value: &Value) -> Result<(), std::io::Error> {
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
                Self::add_vec(vec_id, &mut *p.borrow_mut(), &mut *pool.borrow_mut())?;
                if !(*p).borrow_mut().add_commit {
                    self.commit_posting.push(p.clone());
                    (*p).borrow_mut().add_commit = true;
                }
            }
            _ => {}
        }

        Ok(())
    }

    // 添加vec
    fn add_vec(
        vec_id: VecID,
        posting: &mut Posting,
        block_pool: &mut ByteBlockPool,
    ) -> Result<(), std::io::Error> {
        if posting.last_vec_id == vec_id {
            posting.freq += 1;
        } else {
            Self::write_vec_freq(vec_id, posting, block_pool)?;
            posting.last_vec_id = vec_id;
            posting.vec_num += 1;
        }
        Ok(())
    }

    fn write_vec_freq(
        vec_id: VecID,
        posting: &mut Posting,
        block_pool: &mut ByteBlockPool,
    ) -> Result<(), std::io::Error> {
        if posting.freq == 1 {
            posting.vec_freq_addr = block_pool.write_vusize(
                posting.vec_freq_addr,
                (vec_id - posting.last_vec_id) << 1 | 1,
            )?;
        } else {
            let addr = block_pool
                .write_vusize(posting.vec_freq_addr, (vec_id - posting.last_vec_id) << 1)?;
            posting.vec_freq_addr = block_pool.write_vu32(addr, posting.freq)?;
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

// 快照读写
pub struct SnapshotReader {
    start_addr: Addr,
    end_addr: Addr,
    reader: ByteBlockReader,
}

impl SnapshotReader {
    fn new(start_addr: Addr, end_addr: Addr, reader: ByteBlockReader) -> SnapshotReader {
        Self {
            start_addr: start_addr,
            end_addr: end_addr,
            reader: reader,
        }
    }
}

//倒排索引读写
pub struct PostingReader {
    lastVecID: VecID,
}

impl PostingReader {
    fn new() {}
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

    #[test]
    fn test_fieldcache() {
        let buffer_pool = Arc::new(RefCell::new(ByteBlockPool::new()));
        let mut field = FieldCache::new(Arc::downgrade(&buffer_pool));
        let vec_id: VecID = 0;
        let value1 = Value::Str("aa".to_string());
        let value2 = Value::Str("bb".to_string());
        let value3 = Value::Str("cc".to_string());
        let value4 = Value::Str("dd".to_string());
        field.add(vec_id, &value1).unwrap();
        field.add(vec_id, &value2).unwrap();
        field.add(vec_id, &value3).unwrap();
        field.add(vec_id, &value4).unwrap();

        field.commit().unwrap();

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
