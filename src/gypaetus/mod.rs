mod ann;
mod buffer;
mod disk;
mod query;
mod schema;
mod searcher;
mod tokenize;
mod util;
use query::Term;
use util::error::GyResult;
use util::index::{CacheIndex, DefaultCache};
mod wal;

use ann::BoxedAnnIndex;
use ann::{Create, Metric, HNSW};
use buffer::{
    Addr, ByteBlockPool, RingBuffer, RingBufferReader, SnapshotReader, SnapshotReaderIter,
    BLOCK_SIZE_CLASS,
};

use query::Query;
use schema::{DocID, Document, FieldValue, Schema, Value, Vector, VectorType};
use varintrs::{Binary, ReadBytesVarExt};
//use jiebars::Jieba;
use art_tree::Art;
use lock_api::RawMutex;
use parking_lot::Mutex;
use std::cell::RefCell;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, RwLock, Weak};
use std::usize;
use wal::{MmapWal, Wal};

pub struct IndexConfig {}

// 实现基础搜索能力
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

//时序搜索
pub struct Series {}

impl Series {}

unsafe impl<V: 'static> Send for Collection<V> where V: Metric<V> + Create {}
unsafe impl<V: 'static> Sync for Collection<V> where V: Metric<V> + Create {}

//向量搜索
pub struct Collection<V: 'static>
where
    V: Metric<V> + Create,
{
    vector_field: RwLock<BoxedAnnIndex<V>>,
    index: Index,
    rw_lock: Mutex<()>,
}

impl<V: 'static> Collection<V>
where
    V: Metric<V> + Create,
{
    fn new(index: Index, vec_type: &VectorType) -> Collection<V> {
        Collection {
            vector_field: RwLock::new(Self::get_vector_index(&vec_type).unwrap()),
            index: index,
            rw_lock: Mutex::new(()),
        }
    }

    pub fn get_vector_index(vec_type: &VectorType) -> Option<BoxedAnnIndex<V>> {
        match vec_type {
            VectorType::HNSW => Some(BoxedAnnIndex(Box::new(HNSW::<V>::new(32)))),
            _ => None,
        }
    }

    pub fn add(&mut self, vec: Vector<V>) -> GyResult<()> {
        unsafe {
            self.rw_lock.raw().lock();
        }
        self.index.0._add(&vec.d)?;
        self.vector_field
            .write()
            .unwrap()
            .0
            .insert(vec.into(), *self.index.0.doc_id.read().unwrap() as usize);
        *self.index.0.doc_id.write().unwrap() += 1;
        Ok(())
    }
}

unsafe impl Send for IndexBase {}
unsafe impl Sync for IndexBase {}

pub struct IndexBase {
    fields: Vec<FieldCache>,
    doc_id: RwLock<DocID>,
    buffer: Arc<RingBuffer>,
    schema: Schema,
    rw_lock: Mutex<()>,
    wal: Arc<dyn Wal>, // tokenizer: Jieba,
}

impl IndexBase {
    //fn with_wal() -> IndexBase {}

    fn new(schema: Schema) -> IndexBase {
        let buffer_pool = Arc::new(RingBuffer::new());
        let mut field_cache: Vec<FieldCache> = Vec::new();
        for s in schema.fields.iter() {
            field_cache.push(FieldCache::new(Arc::downgrade(&buffer_pool)));
        }

        Self {
            fields: field_cache,
            doc_id: RwLock::new(0),
            buffer: buffer_pool,
            schema: schema,
            rw_lock: Mutex::new(()),
            wal: Arc::new(MmapWal::new()),
        }
    }

    fn _add(&self, doc: &Document) -> GyResult<()> {
        for field in doc.field_values.iter() {
            let fw = self.fields.get(field.field_id().0 as usize).unwrap();
            fw.add(*self.doc_id.read().unwrap(), field.value())?;
        }
        Ok(())
    }

    //add vector
    pub fn add(&self, doc: &Document) -> GyResult<()> {
        unsafe {
            self.rw_lock.raw().lock();
        }

        self._add(doc)?;
        //添加向量
        *self.doc_id.write().unwrap() += 1;
        Ok(())
    }

    fn search(&self, query: &dyn Query) {
        // self.vector_index.0.search(&v, 10);
        todo!();
        //  Ok(())
    }

    // commit 之后文档能搜索得到
    fn commit(&mut self) {}

    // 手动flush到磁盘中
    fn flush(&mut self, path: PathBuf) {}
}

// 倒排表
struct Posting {
    last_doc_id: DocID,
    doc_delta: DocID,
    byte_addr: Addr,
    doc_freq_addr: Addr,
    doc_num: usize,
    freq: u32,
    add_commit: bool,
}

impl Posting {
    fn new(doc_id: DocID, doc_freq_addr: Addr, pos_addr: Addr) -> Posting {
        Self {
            last_doc_id: doc_id,
            doc_delta: doc_id,
            byte_addr: doc_freq_addr,
            doc_freq_addr: doc_freq_addr,
            doc_num: 0,
            add_commit: false,
            freq: 0,
        }
    }
}

pub(crate) struct FieldCache {
    indexs: Box<RwLock<dyn CacheIndex<Vec<u8>, Arc<RefCell<Posting>>>>>,
    share_bytes_block: Weak<RingBuffer>,
    commit_posting: RwLock<Vec<Arc<RefCell<Posting>>>>,
}

impl FieldCache {
    fn new(pool: Weak<RingBuffer>) -> FieldCache {
        Self {
            indexs: Box::new(RwLock::new(DefaultCache::new())),
            share_bytes_block: pool,
            commit_posting: RwLock::new(Vec::new()),
        }
    }

    fn get(&self, term: &Term) -> GyResult<PostingReader> {
        let (start_addr, end_addr) = {
            let index = self.indexs.read().unwrap();
            let posting = index.get(&term.0).unwrap();
            let (start_addr, end_addr) = (
                (*posting).borrow().byte_addr.clone(),
                (*posting).borrow().doc_freq_addr.clone(),
            );
            (start_addr, end_addr)
        };
        let reader = RingBufferReader::new(
            self.share_bytes_block.upgrade().unwrap(),
            start_addr,
            end_addr,
        );
        let r = SnapshotReader::new(reader);
        Ok(PostingReader::new(r))
    }

    fn commit(&mut self) -> Result<(), std::io::Error> {
        let pool = self.share_bytes_block.upgrade().unwrap();
        self.commit_posting.write().unwrap().iter().try_for_each(
            |posting| -> Result<(), std::io::Error> {
                let p = &mut *posting.borrow_mut();
                Self::write_vec_freq(p.doc_delta, p, &mut *pool.borrow_mut())?;
                p.add_commit = false;
                Ok(())
            },
        )?;
        self.commit_posting.write().unwrap().clear();
        Ok(())
    }

    //添加 token 单词
    pub fn add(&self, doc_id: DocID, value: &Value) -> GyResult<()> {
        let v = value.to_vec();
        if !self.indexs.read().unwrap().contains_key(&v) {
            let pool = self.share_bytes_block.upgrade().unwrap();
            let pos = (*pool).borrow_mut().alloc_bytes(0, None);
            self.indexs.write().unwrap().insert(
                v.clone(),
                Arc::new(RefCell::new(Posting::new(
                    doc_id,
                    pos,
                    pos + BLOCK_SIZE_CLASS[1],
                ))),
            );
        }
        // 获取词典的倒排表
        let p = self
            .indexs
            .read()
            .unwrap()
            .get(&v)
            .expect("get term posting list fail")
            .clone();
        // 获取bytes 池
        let pool = self.share_bytes_block.upgrade().unwrap();
        // 倒排表中加入文档id
        Self::add_vec(doc_id, &mut *p.borrow_mut(), &mut *pool.borrow_mut())?;
        if !(*p).borrow().add_commit {
            self.commit_posting.write().unwrap().push(p.clone());
            (*p).borrow_mut().add_commit = true;
        }

        Ok(())
    }

    // 添加vec
    fn add_vec(
        doc_id: DocID,
        posting: &mut Posting,
        block_pool: &mut ByteBlockPool,
    ) -> Result<(), std::io::Error> {
        if posting.last_doc_id == doc_id {
            posting.freq += 1;
        } else {
            Self::write_vec_freq(posting.doc_delta, posting, block_pool)?;
            posting.doc_delta = doc_id - posting.last_doc_id;
            posting.last_doc_id = doc_id;
            posting.doc_num += 1;
        }
        Ok(())
    }

    fn write_vec_freq(
        doc_delta: DocID,
        posting: &mut Posting,
        block_pool: &mut ByteBlockPool,
    ) -> Result<(), std::io::Error> {
        if posting.freq == 1 {
            let doc_code = doc_delta << 1 | 1;
            let addr = block_pool.write_u64(posting.doc_freq_addr, doc_code)?;
            posting.doc_freq_addr = addr;
        } else {
            let addr = block_pool.write_u64(posting.doc_freq_addr, doc_delta << 1)?;
            posting.doc_freq_addr = block_pool.write_vu32(addr, posting.freq)?;
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

pub struct PostingReader {
    snap: SnapshotReader,
}

impl PostingReader {
    fn new(snap: SnapshotReader) -> PostingReader {
        PostingReader { snap: snap }
    }

    pub fn iter<'a>(&'a self) -> PostingReaderIter<'a> {
        PostingReaderIter {
            last_docid: 0,
            snap_iter: self.snap.iter(),
        }
    }
}

pub struct PostingReaderIter<'a> {
    last_docid: DocID,
    snap_iter: SnapshotReaderIter<'a>,
}

impl<'b, 'a> Iterator for PostingReaderIter<'a> {
    type Item = (DocID, u32);
    fn next(&mut self) -> Option<Self::Item> {
        let doc_code = self.snap_iter.next()?;
        self.last_docid += doc_code >> 1;
        let freq = if doc_code & 1 > 0 {
            1
        } else {
            self.snap_iter.next()? as u32
        };
        Some((self.last_docid, freq))
    }
}

pub struct IndexReader {
    reader: Arc<IndexBase>,
}

impl IndexReader {
    fn new(reader: Arc<IndexBase>) -> IndexReader {
        IndexReader { reader: reader }
    }

    fn from() {}

    fn search(query: &dyn Query) {}
}

#[cfg(test)]
mod tests {

    use super::*;

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
        let mut doc_id: DocID = 0;
        let valuea = Value::Str("aa".to_string());
        let valueb = Value::Str("bb".to_string());
        let valuec = Value::Str("cc".to_string());
        let valued = Value::Str("dd".to_string());
        field.add(doc_id, &valuea).unwrap(); // aa
        field.add(doc_id, &valueb).unwrap(); //
                                             // field.add(doc_id, &value3).unwrap();
                                             // field.add(doc_id, &value4).unwrap();

        doc_id = 1;
        field.add(doc_id, &valuea).unwrap();
        field.add(doc_id, &valuec).unwrap();

        doc_id = 2;
        field.add(doc_id, &valuea).unwrap();
        field.add(doc_id, &valueb).unwrap();
        field.add(doc_id, &valuec).unwrap();
        field.add(doc_id, &valued).unwrap();

        doc_id = 3;
        field.add(doc_id, &valuea).unwrap();
        field.add(doc_id, &valuec).unwrap();
        field.add(doc_id, &valued).unwrap();

        doc_id = 4;
        field.add(doc_id, &valuea).unwrap();
        field.add(doc_id, &valued).unwrap();

        doc_id = 5;
        field.add(doc_id, &valuec).unwrap();
        field.add(doc_id, &valued).unwrap();
        field.commit().unwrap();

        println!("search aa");
        let mut p = field.get(&Term::from_str("aa")).unwrap();
        for x in p.iter() {
            println!("{:?}", x);
        }

        println!("search bb");
        p = field.get(&Term::from_str("bb")).unwrap();
        for x in p.iter() {
            println!("{:?}", x);
        }

        println!("search cc");
        p = field.get(&Term::from_str("cc")).unwrap();
        for x in p.iter() {
            println!("{:?}", x);
        }

        println!("search dd");
        p = field.get(&Term::from_str("dd")).unwrap();
        for x in p.iter() {
            println!("{:?}", x);
        }
    }
}
