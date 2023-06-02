mod ann;
mod cache;
mod disk;
mod query;
mod schema;
mod tokenize;
mod util;
use util::error::GyResult;
mod wal;

use crate::ann::BoxedAnnIndex;
use crate::cache::{Addr, ByteBlockPool, RingBuffer, RingBufferReader, BLOCK_SIZE_CLASS};
use crate::query::Query;
use crate::schema::{FieldValue, Schema, Value, VecID, Vector, VectorType};

use crate::ann::{Create, Metric, HNSW};
//use jiebars::Jieba;
use std::cell::RefCell;
use std::collections::HashMap;
use std::io::{Read, Write};
use std::path::PathBuf;
use std::sync::atomic::AtomicU64;
use std::sync::{Arc, RwLock, Weak};
use std::usize;

pub struct IndexConfig {}

pub struct Index(Arc<IndexBase>);

impl Index {
    //pub fn new(schema: Schema) -> Index {}

    pub fn reader(&self) -> GyResult<IndexReader> {
        Ok(IndexReader::new(self.0.clone()))
    }

    pub fn create_vector_collection<V>(self, vector: VectorType) -> Collection<V>
    where
        V: Metric<V> + Create,
    {
        Collection::new(self, &vector)
    }
}

pub struct Collection<V: 'static>
where
    V: Metric<V> + Create,
{
    vector_field: RwLock<BoxedAnnIndex<V>>,
    index: Index,
}

impl<V: 'static> Collection<V>
where
    V: Metric<V> + Create,
{
    fn new(index: Index, vec_type: &VectorType) -> Collection<V> {
        Collection {
            vector_field: RwLock::new(Self::get_vector_index(&vec_type)),
            index: index,
        }
    }

    pub fn get_vector_index(vec_type: &VectorType) -> BoxedAnnIndex<V> {
        BoxedAnnIndex(Box::new(HNSW::<V>::new(32)))
    }

    pub fn add(&mut self, vec: Vector<V>) -> GyResult<()> {
        self.index.0.add(&vec.field_values)?;
        self.vector_field
            .write()
            .unwrap()
            .0
            .insert(vec.into(), *self.index.0.vec_id.read().unwrap() as usize);
        Ok(())
    }
}

unsafe impl Send for IndexBase {}
unsafe impl Sync for IndexBase {}

//#[derive(Clone)]
pub struct IndexBase {
    fields: Vec<FieldCache>,
    vec_id: RwLock<VecID>,
    buffer: Arc<RingBuffer>,
    schema: Schema,
    // tokenizer: Jieba,
}

impl IndexBase {
    fn new(schema: Schema) -> IndexBase {
        let buffer_pool = Arc::new(RingBuffer::new());
        let mut field_cache: Vec<FieldCache> = Vec::new();
        for s in schema.fields.iter() {
            field_cache.push(FieldCache::new(Arc::downgrade(&buffer_pool)));
        }

        Self {
            fields: field_cache,
            vec_id: RwLock::new(0),
            buffer: buffer_pool,
            schema: schema,
        }
    }

    //add vector
    pub fn add(&self, field_values: &[FieldValue]) -> GyResult<()> {
        //添加向量标签
        for field in field_values.iter() {
            let fw = self.fields.get(field.field_id().0 as usize).unwrap();
            fw.add(*self.vec_id.read().unwrap(), field.value())?;
        }
        //添加向量
        *self.vec_id.write().unwrap() += 1;
        Ok(())
    }

    // fn search(&self, v: V) -> GyResult<impl Iterator<Item = GyResult<Vector<V>>>> {
    //     // self.vector_index.0.search(&v, 10);
    //     todo!();
    //     //  Ok(())
    // }

    // commit 之后文档能搜索得到
    fn commit(&mut self) {}

    // 手动flush到磁盘中
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
    indexs: RwLock<HashMap<String, Arc<RefCell<Posting>>>>,
    share_bytes_block: Weak<RingBuffer>,
    commit_posting: RwLock<Vec<Arc<RefCell<Posting>>>>,
}

impl FieldCache {
    fn new(pool: Weak<RingBuffer>) -> FieldCache {
        Self {
            indexs: RwLock::new(HashMap::new()),
            share_bytes_block: pool,
            commit_posting: RwLock::new(Vec::new()),
        }
    }

    // fn search(&mut self) -> FieldCache {}

    fn commit(&mut self) -> Result<(), std::io::Error> {
        let pool = self.share_bytes_block.upgrade().unwrap();
        self.commit_posting.write().unwrap().iter().try_for_each(
            |posting| -> Result<(), std::io::Error> {
                let p = &mut *posting.borrow_mut();
                Self::write_vec_freq(p.last_vec_id, p, &mut *pool.borrow_mut())?;
                p.add_commit = false;
                Ok(())
            },
        )?;
        self.commit_posting.write().unwrap().clear();
        Ok(())
    }

    //添加 token 单词
    pub fn add(&self, vec_id: VecID, value: &Value) -> GyResult<()> {
        match value {
            Value::Str(s) => {
                if !self.indexs.read().unwrap().contains_key(s) {
                    let pool = self.share_bytes_block.upgrade().unwrap();
                    let pos = (*pool).borrow_mut().new_bytes(BLOCK_SIZE_CLASS[1] * 2);
                    self.indexs.write().unwrap().insert(
                        s.to_string(),
                        Arc::new(RefCell::new(Posting::new(pos, pos + BLOCK_SIZE_CLASS[1]))),
                    );
                }
                // 获取词典的倒排表
                let p = self
                    .indexs
                    .read()
                    .unwrap()
                    .get(s)
                    .expect("get term posting list fail")
                    .clone();
                // 获取bytes 池
                let pool = self.share_bytes_block.upgrade().unwrap();
                // 倒排表中加入文档id
                Self::add_vec(vec_id, &mut *p.borrow_mut(), &mut *pool.borrow_mut())?;
                if !(*p).borrow().add_commit {
                    self.commit_posting.write().unwrap().push(p.clone());
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
            posting.vec_freq_addr = block_pool.write_vec_id(
                posting.vec_freq_addr,
                (vec_id - posting.last_vec_id) << 1 | 1,
            )?;
        } else {
            let addr = block_pool
                .write_vec_id(posting.vec_freq_addr, (vec_id - posting.last_vec_id) << 1)?;
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

pub struct VectorIter {}

// pub struct PostingIter<'a> {
//     reader: ByteBlockReader,
//     snap: SnapshotReader<'a>,
// }

// impl<'a> PostingIter<'a> {
//     fn new() {}
// }

// impl<'b, 'a> Iterator for PostingIter<'_, 'a> {
//     type Item = VecID;
//     fn next(&mut self) -> Option<Self::Item> {
//         todo!();
//     }
// }

pub struct IndexReader {
    reader: Arc<IndexBase>,
}

impl IndexReader {
    fn new(reader: Arc<IndexBase>) -> IndexReader {
        IndexReader { reader: reader }
    }

    fn from() {}

    fn search(query: &Query) {}
}

pub struct Searcher {
    readers: Vec<IndexReader>,
}

impl Searcher {
    fn new(reader: IndexReader) -> Searcher {
        Searcher {
            readers: vec![reader],
        }
    }

    pub fn search(query: &Query) {}
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    // fn test_tokenizer() {
    //     let jieba = Jieba::new().unwrap();
    //     //搜索引擎模式
    //     let words = jieba.cut_for_search("小明硕士，毕业于中国科学院计算所，后在日本京都大学深造");

    //     println!("【搜索引擎模式】:{}\n", words.join(" / "));
    // }
    #[test]
    fn test_fieldcache() {
        let buffer_pool = Arc::new(RingBuffer::new());
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
    }
}
