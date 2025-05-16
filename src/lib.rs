pub mod ann;
mod buffer;
pub mod collection;
mod compaction;
pub mod config;
pub mod disk;
pub mod query;
pub mod schema;
mod searcher;
use crate::ann::{AnnFilter, AnnPrioritizer};
use ann::Emptyer;

use wwml::similarity::TensorSimilar;
mod tokenize;
pub mod util;
use crate::config::Config;
use crate::config::EngineConfig;
use crate::searcher::PostingReader;
use ann::Neighbor;
use art_tree::{Art, ByteString};
use core::cell::UnsafeCell;
use disk::GyRead;
use jiebars::Jieba;
use query::Term;
use schema::TensorEntry;
use schema::ValueSized;
use searcher::BlockReader;
use std::io::{Cursor, Write};
use std::option;
use std::sync::atomic::AtomicU64;
use util::asyncio::WaitGroup;
use util::error::{GyError, GyResult};
use wal::ThreadWal;
use wal::WalIter;
use wwml::Tensor;
mod macros;
use crate::buffer::SafeAddr;
use crate::schema::VectorBase;
use crate::util::asyncio::Worker;
pub mod wal;
use crate::ann::Ann;
use crate::ann::HNSW;
use crate::disk::GyWrite;
use crate::schema::VectorFrom;
use crate::schema::VectorSerialize;
use ann::Metric;
use buffer::{
    Addr, ByteBlockPool, RingBuffer, RingBufferReader, SnapshotReader, SnapshotReaderIter,
    BLOCK_SIZE_CLASS,
};
use schema::{BinarySerialize, DocID, Document, Schema, Value};
use serde::{Deserialize, Serialize};
//use jiebars::Jieba;
use self::schema::DocFreq;
use self::util::fs;
use crate::schema::FieldEntry;
use crate::schema::Vector;
use crate::util::time::Time;
use crate::wal::WalReader;
use lock_api::RawMutex;
use once_cell::sync::Lazy;
use parking_lot::Mutex;
use std::cell::RefCell;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, RwLock, Weak};
use tokio::runtime::Builder;
use wal::{IOType, Wal, DEFAULT_WAL_FILE_SIZE};

// 单例的 Tokio runtime
pub(crate) static RUNTIME: Lazy<tokio::runtime::Runtime> = Lazy::new(|| {
    Builder::new_multi_thread()
        .worker_threads(2)
        .build()
        .expect("Failed to create runtime")
});

unsafe impl Send for Index {}
unsafe impl Sync for Index {}

#[derive(Clone)]
// 实现基础搜索能力
pub struct Index(Arc<IndexBase>);

impl Index {
    pub fn new(schema: &Schema, config: EngineConfig) -> GyResult<Index> {
        Ok(Self(Arc::new(IndexBase::new(schema, config)?)))
    }

    pub fn reader(&self) -> GyResult<IndexReader> {
        Ok(IndexReader::new(self.0.clone()))
    }

    pub fn writer(&mut self) -> GyResult<IndexWriter> {
        Ok(IndexWriter::new(self.0.clone()))
    }

    pub fn flush_disk(&self) -> GyResult<()> {
        Ok(())
    }

    pub fn close() {}
}

unsafe impl<V: VectorSerialize + ValueSized + VectorFrom + Clone + TensorSimilar + 'static> Send
    for VectorEngine<V>
where
    V: Metric<V>,
{
}

unsafe impl<V: VectorSerialize + ValueSized + VectorFrom + Clone + TensorSimilar + 'static> Sync
    for VectorEngine<V>
where
    V: Metric<V>,
{
}

unsafe impl Send for EngineReader {}

pub struct EngineReader {
    pub(crate) vector_field: Arc<VectorIndexBase<Tensor>>,
    index_reader: IndexReader,
    entry: TensorEntry,
    w: Worker,
}

impl BlockReader for EngineReader {
    fn query(&self, v: &Tensor, k: usize, term: &Option<Term>) -> GyResult<Vec<Neighbor>> {
        if let Some(t) = term {
            let posing = self.search(t)?;
            todo!()
        } else {
            self.vector_field
                .query::<Emptyer, Emptyer>(v, k, None, None)
        }
    }

    fn search(&self, term: &Term) -> GyResult<PostingReader> {
        Ok(PostingReader::Mem(self._search(term)?))
    }

    fn vector(&self, doc_id: DocID) -> GyResult<Vector> {
        let doc_offset = self.index_reader.index_base.doc_offset(doc_id)?;
        let v: Vector = {
            let wal = self.index_reader.wal.get_borrow();
            let mut wal_read = WalReader::new(wal, doc_offset);
            Vector::vector_deserialize(&mut wal_read, self.tensor_entry())?
        };
        Ok(v)
    }
}

impl EngineReader {
    pub fn doc_num(&self) -> usize {
        self.index_reader.doc_num()
    }

    pub(crate) fn _search(&self, term: &Term) -> GyResult<MemPostingReader> {
        self.index_reader.search(term)
    }

    pub fn vector_iter<'a>(&'a self, entry: TensorEntry) -> WalIter<'a, Vector> {
        self.index_reader
            .get_index_base()
            .get_wal_mut()
            .iter::<Vector>(entry)
    }

    fn tensor_entry(&self) -> &TensorEntry {
        &self.entry
    }

    pub fn index_reader(&self) -> &IndexReader {
        &self.index_reader
    }
}

unsafe impl Sync for Engine {}
unsafe impl Send for Engine {}

#[derive(Clone)]
pub struct Engine(Arc<VectorEngine<Tensor>>);

impl Engine {
    pub fn reader(&self) -> EngineReader {
        EngineReader {
            vector_field: self.0.vector_field.clone(),
            index_reader: IndexReader::new(self.0.index_base.clone()),
            entry: self.0.entry.clone(),
            w: self.0.wg.worker(),
        }
    }

    pub fn new(schema: &Schema, config: EngineConfig) -> GyResult<Engine> {
        Ok(Engine(Arc::new(VectorEngine::new(schema, config)?)))
    }

    pub fn open(schema: &Schema, config: EngineConfig) -> GyResult<Engine> {
        Ok(Engine(Arc::new(VectorEngine::open(schema, config)?)))
    }

    pub fn add(&self, v: Vector) -> GyResult<DocID> {
        self.0.add(v)
    }

    pub async fn wait(&self) {
        self.0.wg.wait().await
    }

    fn get_wal_fname(&self) -> &Path {
        self.0.index_base.wal.get_borrow().get_fname()
    }

    fn rename_wal(&self, new_fname: &Path) -> GyResult<()> {
        self.0.index_base.wal.get_borrow_mut().rename(new_fname)
    }

    pub(crate) fn check_room_for_write(&self, size: usize) -> bool {
        self.0.check_room_for_write(size)
    }
}

struct VectorIndexBase<V: VectorSerialize + TensorSimilar + Clone>(RwLock<Ann<V>>);

impl<V: VectorSerialize + TensorSimilar + Clone> VectorIndexBase<V>
where
    V: Metric<V>,
{
    pub fn query<P: AnnPrioritizer, F: AnnFilter>(
        &self,
        v: &V,
        k: usize,
        prioritizer: Option<P>,
        filter: Option<F>,
    ) -> GyResult<Vec<Neighbor>> {
        self.0.read()?.query(&v, k, prioritizer, filter)
    }

    pub fn insert(&self, v: V) -> GyResult<usize> {
        self.0.write()?.insert(v)
    }

    pub fn merge(&self, other: &Self) -> Self {
        todo!()
    }
}

impl<V: VectorSerialize + TensorSimilar + Clone> VectorSerialize for VectorIndexBase<V> {
    fn vector_deserialize<R: std::io::Read + GyRead>(
        reader: &mut R,
        entry: &TensorEntry,
    ) -> GyResult<Self> {
        let a = Ann::<V>::vector_deserialize(reader, entry)?;
        Ok(VectorIndexBase(RwLock::new(a)))
    }

    fn vector_serialize<W: Write + GyWrite>(&self, writer: &mut W) -> GyResult<()> {
        self.0.read()?.vector_serialize(writer)
    }

    fn vector_nommap_deserialize<R: std::io::Read + GyRead>(
        reader: &mut R,
        entry: &TensorEntry,
    ) -> GyResult<Self> {
        todo!()
    }
}

//向量搜索
pub struct VectorEngine<
    V: VectorSerialize + TensorSimilar + ValueSized + VectorFrom + Clone + 'static,
> where
    V: Metric<V>,
{
    vector_field: Arc<VectorIndexBase<V>>,
    index_base: Arc<IndexBase>,
    entry: TensorEntry,
    wg: WaitGroup,
    rw_lock: Mutex<()>,
}

impl<V: VectorSerialize + TensorSimilar + ValueSized + VectorFrom + Clone + 'static> VectorEngine<V>
where
    V: Metric<V>,
{
    fn new(schema: &Schema, config: EngineConfig) -> GyResult<VectorEngine<V>> {
        Ok(Self {
            vector_field: Arc::new(VectorIndexBase(RwLock::new(Ann::HNSW(HNSW::<V>::new(32))))),
            index_base: Arc::new(IndexBase::new(schema, config)?),
            entry: schema.tensor_entry().clone(),
            wg: WaitGroup::new(),
            rw_lock: Mutex::new(()),
        })
    }

    pub fn open(schema: &Schema, config: EngineConfig) -> GyResult<VectorEngine<V>> {
        let tensor_entry = schema.tensor_entry().clone();
        let colletion = Self {
            vector_field: Arc::new(VectorIndexBase(RwLock::new(Ann::HNSW(HNSW::<V>::new(32))))),
            index_base: Arc::new(IndexBase::open(schema, config)?),
            entry: schema.tensor_entry().clone(),
            wg: WaitGroup::new(),
            rw_lock: Mutex::new(()),
        };
        {
            let wal = colletion.index_base.get_wal_mut();
            let offset = {
                let mut wal_iter = wal.iter::<VectorBase<V>>(tensor_entry);
                while let Some((doc_offset, v)) = wal_iter.next() {
                    colletion.quick_add(doc_offset, v)?;
                }
                wal_iter.offset() - 4
            };
            wal.set_position(offset);
        }
        Ok(colletion)
    }

    fn quick_add(&self, doc_offset: usize, v: VectorBase<V>) -> GyResult<DocID> {
        let doc_id = self.index_base.doc_id.load(Ordering::SeqCst);
        {
            self.index_base.doc_offset.get_borrow_mut().push(doc_offset)
        }
        self.index_base
            .last_offset
            .store(doc_offset, Ordering::SeqCst);
        let id = self.vector_field.insert(v.v)? as DocID;
        assert!(id == doc_id);
        self.index_base.doc_id.fetch_add(1, Ordering::SeqCst);
        self.index_base.inner_add(id, &v.payload)?;
        self.index_base.commit()?;
        Ok(doc_id)
    }

    pub fn batch_add(&self, v: VectorBase<V>) -> GyResult<()> {
        unsafe {
            self.rw_lock.raw().lock();
        }
        let doc_offset = self.write_vector_to_wal(&v)?;
        self.quick_add(doc_offset, v)?;
        unsafe {
            self.rw_lock.raw().unlock();
        }
        Ok(())
    }

    pub fn add(&self, v: VectorBase<V>) -> GyResult<DocID> {
        // unsafe {
        //     self.rw_lock.raw().lock();
        // }
        let doc_offset = self.write_vector_to_wal(&v)?;
        let doc_id = self.quick_add(doc_offset, v)?;
        // unsafe {
        //     self.rw_lock.raw().unlock();
        // }
        Ok(doc_id)
    }

    fn check_room_for_write(&self, size: usize) -> bool {
        self.index_base.wal.get_borrow().check_room(size)
    }

    fn write_vector_to_wal(&self, v: &VectorBase<V>) -> GyResult<usize> {
        assert!(self.index_base.wal.get_borrow().check_rotate(v));
        let offset = {
            let w = self.index_base.wal.get_borrow_mut();
            let offset = w.offset();
            v.vector_serialize(w)?;
            w.flush()?;
            offset
        };
        Ok(offset)
    }

    fn rename_wal(&self, new_fname: &Path) -> GyResult<()> {
        self.index_base.get_wal_mut().rename(new_fname)
    }
}

unsafe impl Send for IndexBase {}
unsafe impl Sync for IndexBase {}

unsafe impl Send for ThreadVec {}
unsafe impl Sync for ThreadVec {}

#[derive(Default)]
struct ThreadVec(UnsafeCell<Vec<usize>>);

impl ThreadVec {
    pub(crate) fn new(w: Vec<usize>) -> ThreadVec {
        Self(UnsafeCell::new(w))
    }
    pub(crate) fn get_borrow(&self) -> &Vec<usize> {
        unsafe { &*self.0.get() }
    }
    pub(crate) fn get_borrow_mut(&self) -> &mut Vec<usize> {
        unsafe { &mut *self.0.get() }
    }
}

pub struct IndexBase {
    fields: Vec<FieldCache>,
    doc_id: AtomicU64,
    buffer: Arc<RingBuffer>,
    wal: Arc<ThreadWal>,
    doc_offset: ThreadVec,
    rw_lock: Mutex<()>,
    last_offset: AtomicUsize,
    jieba: Jieba,
}

#[derive(Serialize, Deserialize, Debug)]
pub(crate) struct Meta {
    schema: Schema,
    create_time: i64,
}

impl Meta {
    fn new(schema: Schema) -> Meta {
        Self {
            schema: schema,
            create_time: Time::now(),
        }
    }

    fn get_schema(&self) -> &Schema {
        &self.schema
    }

    pub fn vector_name(&self) -> &str {
        self.schema.vector_field.name()
    }

    pub fn get_fields(&self) -> &[FieldEntry] {
        &self.schema.fields
    }

    pub fn tensor_entry(&self) -> &TensorEntry {
        self.schema.vector_field.tensor_entry()
    }
}

impl IndexBase {
    fn new(schema: &Schema, config: EngineConfig) -> GyResult<IndexBase> {
        let buffer_pool = Arc::new(RingBuffer::new());
        let mut field_cache: Vec<FieldCache> = Vec::new();
        for _ in 0..schema.fields.len() {
            field_cache.push(FieldCache::new(Arc::downgrade(&buffer_pool)));
        }
        Ok(Self {
            fields: field_cache,
            doc_id: AtomicU64::new(0),
            buffer: buffer_pool,
            rw_lock: Mutex::new(()),
            wal: Arc::new(ThreadWal::new(Wal::new(
                &config.get_wal_path(), //&index_path.join(&config.wal_fname),
                config.get_fsize(),
                config.get_io_type(),
            )?)),
            doc_offset: ThreadVec::new(Vec::with_capacity(1024)), // Max memory doc
            last_offset: AtomicUsize::new(0),
            jieba: Jieba::new().unwrap(),
        })
    }

    fn open(schema: &Schema, config: EngineConfig) -> GyResult<IndexBase> {
        let index_path = config.get_wal_path();
        if !index_path.exists() || index_path.is_dir() {
            return Err(GyError::IndexDirNotExist(index_path.to_path_buf()));
        }
        let buffer_pool = Arc::new(RingBuffer::new());
        let mut field_cache: Vec<FieldCache> = Vec::new();
        for _ in 0..schema.fields.len() {
            field_cache.push(FieldCache::new(Arc::downgrade(&buffer_pool)));
        }
        let wal = Wal::open(
            index_path, //&index_path.join(&config.wal_fname),
            config.get_fsize(),
            config.get_io_type(),
        )?;
        Ok(Self {
            fields: field_cache,
            doc_id: AtomicU64::new(0),
            buffer: buffer_pool,
            rw_lock: Mutex::new(()),
            wal: Arc::new(ThreadWal::new(wal)),
            doc_offset: ThreadVec::new(Vec::with_capacity(1024)), // Max memory doc
            last_offset: AtomicUsize::new(0),
            jieba: Jieba::new().unwrap(),
        })
    }

    fn inner_add(&self, doc_id: DocID, doc: &Document) -> GyResult<()> {
        for field in doc.get_field_values().iter() {
            let fw = self.fields.get(field.field_id().id() as usize).unwrap();
            // 分词
            match field.value() {
                Value::Str(s) => {
                    let word = self.jieba.cut_for_search(*s);
                    for v in word.into_iter() {
                        fw.add_with_bytes(doc_id, v.as_bytes())?;
                    }
                }
                Value::String(s) => {
                    let word = self.jieba.cut_for_search(s);
                    for v in word.into_iter() {
                        fw.add_with_bytes(doc_id, v.as_bytes())?;
                    }
                }
                _ => {
                    fw.add(doc_id, field.value())?;
                }
            }
        }
        Ok(())
    }

    pub fn write_doc_to_wal(&self, doc: &Document) -> GyResult<usize> {
        assert!(self.wal.get_borrow().check_rotate(doc));
        let offset = {
            let mut w = self.wal.get_borrow_mut();
            let offset = w.offset();
            doc.binary_serialize(&mut *w)?;
            w.flush()?;
            offset
        };
        Ok(offset)
    }

    //add doc
    pub fn add(&self, doc_id: DocID, doc: &Document) -> GyResult<()> {
        unsafe {
            self.rw_lock.raw().lock();
        }
        let doc_offset = self.write_doc_to_wal(doc)?;
        {
            self.doc_offset.get_borrow_mut().push(doc_offset);
        }
        self.inner_add(doc_id, doc)?;
        self.commit()?;
        unsafe {
            self.rw_lock.raw().unlock();
        }
        Ok(())
    }

    pub fn field_reader(&self, field_id: u32) -> GyResult<FieldReader> {
        Ok(self.fields[field_id as usize].reader())
    }

    // commit 之后文档能搜索得到
    fn commit(&self) -> GyResult<()> {
        self.fields
            .iter()
            .try_for_each(|field| -> Result<(), std::io::Error> {
                field
                    .commit()
                    .map_err(|e| std::io::Error::from(std::io::ErrorKind::InvalidData))?;
                Ok(())
            })?;
        Ok(())
    }

    fn doc_offset(&self, doc_id: DocID) -> GyResult<usize> {
        let offset = {
            let doc_offset = self.doc_offset.get_borrow();
            // if doc_id as usize >= doc_offset.len() {
            //     return Err(GyError::ErrDocumentNotFound);
            // }
            let offset = doc_offset.get(doc_id as usize).unwrap().clone();
            offset
        };
        Ok(offset)
    }

    fn get_wal_mut(&self) -> &mut Wal {
        self.wal.get_borrow_mut()
    }
}

type Posting = Arc<RwLock<_Posting>>;

// 倒排表
pub struct _Posting {
    last_doc_id: DocID,
    doc_delta: DocID,
    byte_addr: SafeAddr,
    doc_freq_addr: SafeAddr,
    doc_count: usize,
    freq: u32,
    add_commit: bool,
}

impl _Posting {
    fn new(doc_freq_addr: Addr, pos_addr: Addr) -> _Posting {
        Self {
            last_doc_id: 0,
            doc_delta: 0,
            byte_addr: SafeAddr::new(doc_freq_addr),
            doc_freq_addr: SafeAddr::new(doc_freq_addr),
            doc_count: 0,
            add_commit: false,
            freq: 0,
        }
    }
}

// 用于数值索引
pub(crate) struct TrieIntCache {}

//用于字符串索引
pub(crate) struct ArtCache {
    cache: Art<ByteString, Posting>,
}

impl ArtCache {
    pub(crate) fn new() -> ArtCache {
        Self { cache: Art::new() }
    }
}

impl ArtCache {
    fn contains_key(&self, k: &[u8]) -> bool {
        let a = self.cache.iter();
        match self.cache.get(&ByteString::new(&k)) {
            Some(_) => true,
            None => false,
        }
    }

    fn insert(&mut self, k: &[u8], v: Posting) -> Option<Posting> {
        self.cache.upsert(ByteString::new(k), v.clone());
        Some(v)
    }

    fn get(&self, k: &[u8]) -> Option<&Posting> {
        self.cache.get(&ByteString::new(&k))
    }

    fn iter(&self) -> impl DoubleEndedIterator<Item = (&ByteString, &Posting)> {
        self.cache.iter()
    }
}

pub(crate) struct FieldCache {
    indexs: Arc<RwLock<ArtCache>>,
    share_bytes_block: Weak<RingBuffer>,
    commit_posting: RefCell<Vec<Posting>>,
    term_count: AtomicUsize,
}

impl FieldCache {
    fn new(pool: Weak<RingBuffer>) -> FieldCache {
        Self {
            indexs: Arc::new(RwLock::new(ArtCache::new())),
            share_bytes_block: pool,
            commit_posting: RefCell::new(Vec::new()),
            term_count: AtomicUsize::new(0),
        }
    }

    fn reader(&self) -> FieldReader {
        FieldReader::new(
            self.indexs.clone(),
            self.share_bytes_block.clone(),
            self.term_count.load(Ordering::Acquire),
        )
    }

    fn commit(&self) -> GyResult<()> {
        let pool = self.share_bytes_block.upgrade().unwrap();
        self.commit_posting
            .borrow_mut()
            .iter()
            .try_for_each(|posting| -> GyResult<()> {
                let p = &mut posting.write().unwrap();
                Self::write_doc_freq(p, &mut *pool.get_borrow_mut())?;
                p.add_commit = false;
                p.freq = 0;
                Ok(())
            })?;
        self.commit_posting.borrow_mut().clear();
        Ok(())
    }

    pub fn add_with_bytes(&self, doc_id: DocID, v: &[u8]) -> GyResult<()> {
        if !self.indexs.read()?.contains_key(v) {
            let pool = self.share_bytes_block.upgrade().unwrap();
            let pos = (*pool).get_borrow_mut().alloc_bytes(0, None);
            self.indexs.write()?.insert(
                v,
                Arc::new(RwLock::new(_Posting::new(pos, pos + BLOCK_SIZE_CLASS[1]))),
            );

            self.term_count.fetch_add(1, Ordering::Acquire);
            // println!("get term_count:{}", self.term_count.load(Ordering::Acquire))
        }
        // 获取词典的倒排表
        let p: Posting = self
            .indexs
            .read()?
            .get(&v)
            .expect("get term posting list fail")
            .clone();
        // 获取bytes 池
        let pool = self.share_bytes_block.upgrade().unwrap();
        // 倒排表中加入文档id
        Self::add_doc(doc_id, &mut *p.write()?, &mut *pool.get_borrow_mut())?;
        if !(*p).read()?.add_commit {
            self.commit_posting.borrow_mut().push(p.clone());
            (*p).write()?.add_commit = true;
        }

        Ok(())
    }

    //添加 token 单词
    pub fn add(&self, doc_id: DocID, value: &Value) -> GyResult<()> {
        let mut share_bytes = Cursor::new([0u8; 8]);
        let v = value.to_slice(&mut share_bytes)?;
        self.add_with_bytes(doc_id, v)?;
        Ok(())
    }

    // 添加vec
    fn add_doc(
        doc_id: DocID,
        posting: &mut _Posting,
        block_pool: &mut ByteBlockPool,
    ) -> GyResult<()> {
        if !posting.add_commit {
            posting.doc_count += 1;
            posting.doc_delta = doc_id - posting.last_doc_id;
            posting.freq += 1;
            posting.last_doc_id = doc_id;
        } else if posting.last_doc_id == doc_id {
            posting.freq += 1;
        } else {
            Self::write_doc_freq(posting, block_pool)?;
            posting.doc_delta = doc_id - posting.last_doc_id;
            posting.last_doc_id = doc_id;
            posting.doc_count += 1;
            posting.freq = 1;
        }
        Ok(())
    }

    fn write_doc_freq(posting: &mut _Posting, block_pool: &mut ByteBlockPool) -> GyResult<()> {
        block_pool.set_pos(posting.doc_freq_addr.load(Ordering::SeqCst));
        DocFreq(posting.doc_delta, posting.freq).binary_serialize(block_pool)?;
        posting
            .doc_freq_addr
            .store(block_pool.get_pos(), Ordering::SeqCst);
        Ok(())
    }

    fn add_pos(
        pos: usize,
        posting: &mut Posting,
        pool: Arc<RefCell<ByteBlockPool>>,
    ) -> GyResult<()> {
        Ok(())
    }
}

pub struct FieldReader {
    indexs: Arc<RwLock<ArtCache>>,
    share_bytes_block: Weak<RingBuffer>,
    term_count: usize,
}

impl FieldReader {
    fn new(
        indexs: Arc<RwLock<ArtCache>>,
        share_bytes_block: Weak<RingBuffer>,
        term_count: usize,
    ) -> FieldReader {
        Self {
            indexs: indexs,
            share_bytes_block: share_bytes_block,
            term_count: term_count,
        }
    }

    fn posting_buffer(&self, start_addr: Addr, end_addr: Addr) -> GyResult<RingBufferReader> {
        Ok(RingBufferReader::new(
            self.share_bytes_block.upgrade().unwrap(),
            start_addr,
            end_addr,
        ))
    }

    fn posting(&self, start_addr: Addr, end_addr: Addr) -> GyResult<MemPostingReader> {
        let reader = RingBufferReader::new(
            self.share_bytes_block.upgrade().unwrap(),
            start_addr,
            end_addr,
        );
        let r = SnapshotReader::new(reader);
        Ok(MemPostingReader::new(r))
    }

    fn get(&self, term: &[u8]) -> GyResult<MemPostingReader> {
        let (start_addr, end_addr) = {
            let index = self.indexs.read()?;
            let posting = index.get(term).ok_or(GyError::ErrTermNotFound(
                String::from_utf8_lossy(term).to_string(),
            ))?;
            let (start_addr, end_addr) = (
                (*posting).read()?.byte_addr.load(Ordering::SeqCst),
                (*posting).read()?.doc_freq_addr.load(Ordering::SeqCst),
            );
            (start_addr, end_addr)
        };
        self.posting(start_addr, end_addr)
    }

    fn get_term_count(&self) -> usize {
        self.term_count
    }
}

pub struct MemPostingReader {
    snap: SnapshotReader,
}

impl MemPostingReader {
    fn new(snap: SnapshotReader) -> MemPostingReader {
        MemPostingReader { snap: snap }
    }

    pub fn iter<'a>(&'a self) -> MemPostingReaderIter<'a> {
        MemPostingReaderIter {
            last_docid: 0,
            snap_iter: self.snap.iter(),
        }
    }
}

pub struct MemPostingReaderIter<'a> {
    last_docid: DocID,
    snap_iter: SnapshotReaderIter<'a>,
}

impl<'b, 'a> Iterator for MemPostingReaderIter<'a> {
    type Item = DocFreq;
    fn next(&mut self) -> Option<Self::Item> {
        return match DocFreq::binary_deserialize(&mut self.snap_iter) {
            Ok(mut doc_freq) => {
                self.last_docid += doc_freq.doc_id() >> 1;
                doc_freq.0 = self.last_docid;
                Some(doc_freq)
            }
            Err(_) => None,
        };
    }
}

pub struct IndexReaderIter {
    reader: Arc<IndexBase>,
    i: usize,
}

impl IndexReaderIter {
    fn new(reader: Arc<IndexBase>) -> IndexReaderIter {
        Self {
            reader: reader,
            i: 0,
        }
    }
}

impl Iterator for IndexReaderIter {
    type Item = FieldReader;
    fn next(&mut self) -> Option<Self::Item> {
        if self.i >= self.reader.fields.len() {
            return None;
        }
        let r = self.reader.fields[self.i].reader();
        self.i += 1;
        Some(r)
    }
}

pub struct IndexReader {
    pub(crate) index_base: Arc<IndexBase>,
    pub(crate) wal: Arc<ThreadWal>,
    pub(crate) last_doc_offset: usize,
    pub(crate) doc_count: u64,
}

impl IndexReader {
    fn new(index_base: Arc<IndexBase>) -> IndexReader {
        let wal = index_base.wal.clone();
        let last_doc_offset = index_base.last_offset.load(Ordering::SeqCst);
        let doc_count = index_base.doc_id.load(Ordering::SeqCst);
        IndexReader {
            index_base: index_base,
            wal: wal,
            last_doc_offset: last_doc_offset,
            doc_count: doc_count,
        }
    }

    fn doc_num(&self) -> usize {
        self.doc_count as usize
    }

    fn reopen_wal(&self, fsize: usize) -> GyResult<()> {
        self.wal.reopen(fsize)
    }

    pub(crate) fn get_index_base(&self) -> &IndexBase {
        &self.index_base
    }

    pub fn get_wal_path(&self) -> &Path {
        self.wal.get_borrow().get_fname()
    }

    pub fn iter(&self) -> IndexReaderIter {
        IndexReaderIter::new(self.index_base.clone())
    }

    fn search(&self, term: &Term) -> GyResult<MemPostingReader> {
        let field_id = term.field_id().id();
        let field_reader = self.index_base.field_reader(field_id)?;
        field_reader.get(term.bytes_value())
    }

    pub fn doc(&self, doc_id: DocID) -> GyResult<Document> {
        assert!(doc_id < self.doc_count);
        let doc_offset = self.index_base.doc_offset(doc_id)?;
        let doc: Document = {
            let wal = self.wal.get_borrow();
            let mut wal_read = WalReader::new(wal, doc_offset);
            Document::binary_deserialize(&mut wal_read)?
        };
        Ok(doc)
    }

    pub(crate) fn offset(&self) -> GyResult<usize> {
        let i = self.wal.get_borrow().offset();
        Ok(i)
    }

    pub(crate) fn get_doc_offset(&self) -> &Vec<usize> {
        self.index_base.doc_offset.get_borrow()
    }
}

pub struct IndexWriter {
    writer: Arc<IndexBase>,
}

impl IndexWriter {
    fn new(writer: Arc<IndexBase>) -> IndexWriter {
        IndexWriter { writer: writer }
    }

    pub fn add(&mut self, doc_id: DocID, doc: &Document) -> GyResult<()> {
        self.writer.add(doc_id, doc)
    }

    pub fn commit(&mut self) -> GyResult<()> {
        self.writer.commit()
    }
}

#[cfg(test)]
mod tests {

    use super::{schema::FieldEntry, *};
    use crate::config::ConfigBuilder;
    use config::{DATA_FILE, WAL_FILE};
    use schema::{BinarySerialize, VectorEntry};
    use std::thread;
    use tests::disk::DiskStoreReader;
    use wal::WalIter;
    use wwml::Shape;
    #[test]
    fn test_add_doc() {
        let mut schema = Schema::new();
        schema.add_field(FieldEntry::str("body"));
        schema.add_field(FieldEntry::i32("title"));
        let field_id_title = schema.get_field("title").unwrap();
        let config = ConfigBuilder::default()
            .data_path(PathBuf::from("./data2"))
            .build();
        let mut index = Index::new(
            &schema,
            config.get_engine_config(PathBuf::from("./data2").join(WAL_FILE)),
        )
        .unwrap();
        let mut writer1 = index.writer().unwrap();
        {
            let mut d = Document::new();
            d.add_text(field_id_title.clone(), "bb aa");
            writer1.add(1, &d).unwrap();

            let mut d1 = Document::new();
            d1.add_text(field_id_title.clone(), "aa");
            writer1.add(2, &d1).unwrap();

            let mut d2 = Document::new();
            d2.add_text(field_id_title.clone(), "cc aa");
            writer1.add(3, &d2).unwrap();

            let mut d3 = Document::new();
            d3.add_text(field_id_title.clone(), "aa");
            writer1.add(4, &d3).unwrap();

            let mut d1 = Document::new();
            d1.add_text(field_id_title.clone(), "aa");
            writer1.add(5, &d1).unwrap();

            let mut d2 = Document::new();
            d2.add_text(field_id_title.clone(), "cc aa ff gg");
            writer1.add(6, &d2).unwrap();

            let mut d3 = Document::new();
            d3.add_text(field_id_title.clone(), "ff");
            writer1.add(7, &d3).unwrap();

            writer1.commit().unwrap();
        }

        let reader = index.reader().unwrap();
        let p = reader
            .search(&Term::from_field_text(field_id_title, "aa"))
            .unwrap();

        for doc_freq in p.iter() {
            println!("docid:{}", doc_freq.doc_id());
            let doc = reader.doc(doc_freq.doc_id()).unwrap();
            println!("docid:{},doc{:?}", doc_freq.doc_id(), doc);
        }
        //  println!("doc vec:{:?}", reader.get_doc_offset().read().unwrap());
        //disk::persist_collection(&reader).unwrap();
    }

    // &[0.0, 0.0, 0.0, 1.0],
    // &[0.0, 0.0, 1.0, 0.0],
    // &[0.0, 1.0, 0.0, 0.0],
    // &[1.0, 0.0, 0.0, 0.0],
    // &[0.0, 0.0, 1.0, 1.0],
    // &[0.0, 1.0, 1.0, 0.0],
    // &[1.0, 1.0, 0.0, 0.0],
    // &[1.0, 0.0, 0.0, 1.0],
    use crate::ann::AnnType;
    #[test]
    fn test_add_vector1() {
        let mut schema = Schema::with_vector(VectorEntry::new(
            "vector1",
            AnnType::HNSW,
            TensorEntry::new(1, [4], schema::VectorType::F32),
        ));
        schema.add_field(FieldEntry::str("body"));
        schema.add_field(FieldEntry::i32("title"));

        let field_id_title = schema.get_field("title").unwrap();
        let config = ConfigBuilder::default()
            .data_path(PathBuf::from("./data1"))
            .build();
        let mut collect = Engine::new(
            &schema,
            config.get_engine_config(PathBuf::from("./data1").join(WAL_FILE)),
        )
        .unwrap();
        //let mut writer1 = index.writer().unwrap();
        {
            //writer1.add(1, &d).unwrap();

            let mut d1 = Document::new();
            d1.add_text(field_id_title.clone(), "Can I sign up for Medicare Part B if I am working and have health insurance through an employer?");

            let v1 = Vector::from_array([0.0, 0.0, 0.0, 1.0], d1).unwrap();

            collect.add(v1).unwrap();

            let mut d2 = Document::new();
            d2.add_text(
                field_id_title.clone(),
                "Will my Medicare premiums be higher because of my higher income?",
            );
            let v2 = Vector::from_array([0.0, 0.0, 1.0, 0.0], d2).unwrap();

            collect.add(v2).unwrap();

            let mut d3 = Document::new();
            d3.add_text(field_id_title.clone(), "ff cc aa");

            let v3 = Vector::from_array([0.0, 1.0, 0.0, 0.0], d3).unwrap();

            collect.add(v3).unwrap();

            let mut d4 = Document::new();
            d4.add_text(
                field_id_title.clone(),
                "Should I sign up for Medicare Part B if I have Veterans' Benefits?",
            );
            let v4 = Vector::from_array([1.0, 0.0, 0.0, 0.0], d4).unwrap();
            collect.add(v4).unwrap();

            // let mut d5 = Document::new();
            // d5.add_text(field_id_title.clone(), "cc");
            // let v5 = Vector::from_array([0.0, 0.0, 1.0, 1.0], d5);
            // collect.add(v5).unwrap();

            // let mut d6 = Document::new();
            // d6.add_text(field_id_title.clone(), "aa");
            // let v6 = Vector::from_array([0.0, 1.0, 1.0, 0.0], d6);
            // collect.add(v6).unwrap();

            // let mut d7 = Document::new();
            // d7.add_text(field_id_title.clone(), "ff");
            // let v7 = Vector::from_array([1.0, 0.0, 0.0, 1.0], d7);
            // collect.add(v7).unwrap();

            //  collect.commit().unwrap();
        }

        let reader = collect.reader();

        let posting = reader
            .search(&Term::from_field_text(field_id_title, "Medicare"))
            .unwrap();
        // println!("doc_size:{:?}", p.get_doc_count());
        match posting {
            PostingReader::Mem(p) => {
                for n in p.iter() {
                    let doc = reader.vector(n.doc_id()).unwrap();
                    println!(
                        "docid:{},vector{:?},doc:{:?}",
                        n.doc_id(),
                        unsafe { doc.v.to_vec::<f32>() },
                        doc.doc()
                    );
                }
            }
            _ => {}
        }

        // let p = reader
        //     .query(
        //         &Tensor::from_vec(vec![1.0f32, 0.0, 0.0, 1.0], 1, Shape::from_array([4])),
        //         4,
        //     )
        //     .unwrap();

        // for n in p.iter() {
        //     let doc = reader.vector(n.doc_id()).unwrap();
        //     println!("docid:{},doc{:?}", n.doc_id(), doc.v.to_vec::<f32>());
        // }
        // //  println!("doc vec:{:?}", reader.get_doc_offset().read().unwrap());
        disk::persist_collection(reader, &schema, &PathBuf::from("./data1").join(DATA_FILE))
            .unwrap();
    }
    use wwml::TensorProto;
    #[test]
    fn test_add_vector2() {
        let mut schema = Schema::with_vector(VectorEntry::new(
            "vector1",
            AnnType::HNSW,
            TensorEntry::new(1, [4], schema::VectorType::F32),
        ));
        schema.add_field(FieldEntry::str("body"));
        schema.add_field(FieldEntry::i32("title"));

        let field_id_title = schema.get_field("title").unwrap();
        let config = ConfigBuilder::default()
            .data_path(PathBuf::from("./data2"))
            .build();
        let mut collect = Engine::new(
            &schema,
            config.get_engine_config(PathBuf::from("./data2").join(WAL_FILE)),
        )
        .unwrap();

        {
            let mut d5 = Document::new();
            d5.add_text(field_id_title.clone(), "cc");
            let v5 = Vector::from_array([0.0, 0.0, 1.0, 1.0], d5).unwrap();
            collect.add(v5).unwrap();

            let mut d6 = Document::new();
            d6.add_text(field_id_title.clone(), "aa");
            let v6 = Vector::from_array([0.0, 1.0, 1.0, 0.0], d6).unwrap();
            collect.add(v6).unwrap();

            let mut d7 = Document::new();
            d7.add_text(field_id_title.clone(), "ff");
            let v7 = Vector::from_array([1.0, 0.0, 0.0, 1.0], d7).unwrap();
            collect.add(v7).unwrap();

            let mut d8 = Document::new();
            d8.add_text(field_id_title.clone(), "gg");
            let d8 = Vector::from_array([1.0, 1.0, 0.0, 0.0], d8).unwrap();
            collect.add(d8).unwrap();

            //  collect.commit().unwrap();
        }

        let reader = collect.reader();
        let p = reader
            .query(
                &Tensor::from_vec(
                    vec![0.0f32, 0.0, 1.0, 0.0],
                    1,
                    Shape::from_array([4]),
                    &wwml::Device::Cpu,
                )
                .unwrap(),
                4,
                &None,
            )
            .unwrap();

        for n in p.iter() {
            let doc = reader.vector(n.doc_id()).unwrap();
            println!("docid:{},doc{:?}", n.doc_id(), unsafe {
                doc.v.as_slice::<f32>()
            });
        }
        //  println!("doc vec:{:?}", reader.get_doc_offset().read().unwrap());
        disk::persist_collection(reader, &schema, &PathBuf::from("./data2").join(DATA_FILE))
            .unwrap();
    }

    #[test]
    fn test_search_doc() {
        let mut schema = Schema::with_vector(VectorEntry::new(
            "vector1",
            AnnType::HNSW,
            TensorEntry::new(1, [4], schema::VectorType::F32),
        ));
        schema.add_field(FieldEntry::str("body"));
        schema.add_field(FieldEntry::i32("title"));

        let field_id_title = schema.get_field("title").unwrap();
        let disk_reader = DiskStoreReader::open(PathBuf::from("./data1")).unwrap();
        let p = disk_reader
            .search(&Term::from_field_text(field_id_title, "Medicare"))
            .unwrap();
        // println!("doc_size:{:?}", p.get_doc_count());
        for n in p.iter() {
            let doc = disk_reader.vector(n.doc_id()).unwrap();
            println!(
                "docid:{},vector{:?},doc:{:?}",
                n.doc_id(),
                unsafe { doc.v.to_vec::<f32>() },
                doc.doc()
            );
        }

        let p = disk_reader
            .query(
                &Tensor::from_vec(
                    vec![0.0f32, 0.0, 1.0, 0.0],
                    1,
                    Shape::from_array([4]),
                    &wwml::Device::Cpu,
                )
                .unwrap(),
                4,
                &None,
            )
            .unwrap();

        for n in p.iter() {
            let doc = disk_reader.vector(n.doc_id()).unwrap();
            println!(
                "docid:{},vector{:?},doc:{:?}",
                n.doc_id(),
                unsafe { doc.v.to_vec::<f32>() },
                doc.doc()
            );
        }
    }

    #[test]
    fn test_search_vector() {
        let mut schema = Schema::with_vector(VectorEntry::new(
            "vector1",
            AnnType::HNSW,
            TensorEntry::new(1, [4], schema::VectorType::F32),
        ));
        schema.add_field(FieldEntry::str("body"));
        schema.add_field(FieldEntry::i32("title"));

        let field_id_title = schema.get_field("title").unwrap();
        let disk_reader = DiskStoreReader::open(PathBuf::from("./data3/my_index")).unwrap();
        let p = disk_reader
            .query(
                &Tensor::from_vec(
                    vec![1.0f32, 0.0, 0.0, 1.0],
                    1,
                    Shape::from_array([4]),
                    &wwml::Device::Cpu,
                )
                .unwrap(),
                4,
                &None,
            )
            .unwrap();
        // println!("doc_size:{:?}", p.get_doc_count());
        for n in p.iter() {
            let doc = disk_reader.vector(n.doc_id()).unwrap();
            println!("docid:{},doc{:?}", n.doc_id(), unsafe {
                doc.v.as_slice::<f32>()
            });
        }
    }

    #[test]
    fn test_add_doc2() {
        let mut schema = Schema::new();
        schema.add_field(FieldEntry::str("body"));
        schema.add_field(FieldEntry::i32("title"));
        let field_id_title = schema.get_field("title").unwrap();
        let config = ConfigBuilder::default()
            .data_path(PathBuf::from("/opt/rsproject/chappie/vectorvase/data2"))
            .build();
        let mut index = Index::new(&schema, config.get_engine_config(PathBuf::from(""))).unwrap();
        let mut writer1 = index.writer().unwrap();
        {
            let mut d = Document::new();
            d.add_text(field_id_title.clone(), "bb");
            writer1.add(1, &d).unwrap();

            let mut d1 = Document::new();
            d1.add_text(field_id_title.clone(), "aa");
            writer1.add(2, &d1).unwrap();

            let mut d2 = Document::new();
            d2.add_text(field_id_title.clone(), "cc");
            writer1.add(3, &d2).unwrap();

            let mut d3 = Document::new();
            d3.add_text(field_id_title.clone(), "aa");
            writer1.add(4, &d3).unwrap();

            let mut d1 = Document::new();
            d1.add_text(field_id_title.clone(), "aa");
            writer1.add(5, &d1).unwrap();

            let mut d2 = Document::new();
            d2.add_text(field_id_title.clone(), "cc");
            writer1.add(6, &d2).unwrap();

            let mut d3 = Document::new();
            d3.add_text(field_id_title.clone(), "aa");
            writer1.add(7, &d3).unwrap();

            // writer1.commit().unwrap();
        }

        let reader = index.reader().unwrap();
        let p = reader
            .search(&Term::from_field_text(field_id_title, "aa"))
            .unwrap();

        for doc_freq in p.iter() {
            let doc = reader.doc(doc_freq.doc_id()).unwrap();
            println!("docid:{},doc{:?}", doc_freq.doc_id(), doc);
        }
        println!("doc vec:{:?}", reader.get_doc_offset());
        //  disk::persist_index(&reader).unwrap();
    }
    use crate::fs::FileManager;
    #[test]
    fn test_merge_store() {
        let disk_reader1 = DiskStoreReader::open(PathBuf::from(
            "/opt/rsproject/chappie/vectorvase/data1/my_index",
        ))
        .unwrap();

        let disk_reader2 = DiskStoreReader::open(PathBuf::from(
            "/opt/rsproject/chappie/vectorvase/data2/my_index",
        ))
        .unwrap();
        FileManager::mkdir(&PathBuf::from(
            "/opt/rsproject/chappie/vectorvase/data3/my_index",
        ))
        .unwrap();
        // disk::merge_two(
        //     &disk_reader1,
        //     &disk_reader2,
        //     &PathBuf::from("/opt/rsproject/chappie/vectorvase/data3/my_index/data.gy"),
        // )
        // .unwrap();
    }
    use crate::disk::VectorStore;
    use crate::disk::VectorStoreReader;
    #[test]
    fn test_merge_much() {
        let disk_reader1 =
            VectorStore::open(PathBuf::from("/opt/rsproject/chappie/vectorbase/data1")).unwrap();

        let disk_reader2 =
            VectorStore::open(PathBuf::from("/opt/rsproject/chappie/vectorbase/data2")).unwrap();
        FileManager::mkdir(&PathBuf::from("/opt/rsproject/chappie/vectorbase/data3")).unwrap();
        disk::merge_much(
            &[disk_reader1, disk_reader2],
            &PathBuf::from("/opt/rsproject/chappie/vectorbase/data3/data.gy"),
            1,
        )
        .unwrap();
    }

    #[test]
    fn test_merge_3() {
        let disk_reader1 =
            VectorStore::open(PathBuf::from("/opt/rsproject/chappie/vectorbase/data1")).unwrap();

        let disk_reader2 =
            VectorStore::open(PathBuf::from("/opt/rsproject/chappie/vectorbase/data2")).unwrap();

        let disk_reader3 =
            VectorStore::open(PathBuf::from("/opt/rsproject/chappie/vectorbase/data3")).unwrap();

        FileManager::mkdir(&PathBuf::from("/opt/rsproject/chappie/vectorbase/data4")).unwrap();

        disk::merge_much(
            &[disk_reader1, disk_reader2, disk_reader3],
            &PathBuf::from("/opt/rsproject/chappie/vectorbase/data4/data.gy"),
            1,
        )
        .unwrap();
    }

    #[test]
    fn test_read() {
        let mut schema = Schema::new();
        schema.add_field(FieldEntry::str("body"));
        schema.add_field(FieldEntry::i32("title"));
        let field_id_title = schema.get_field("title").unwrap();
        let disk_reader =
            DiskStoreReader::open(PathBuf::from("/opt/rsproject/chappie/vectorbase/data4"))
                .unwrap();
        let p = disk_reader
            .search(&Term::from_field_text(field_id_title, "aa"))
            .unwrap();
        println!("doc_size:{:?}", p.get_doc_count());
        for doc_freq in p.iter() {
            println!("{:?}", doc_freq);
        }
        //    let doc_reader = disk_reader.doc_reader().unwrap();
    }

    #[test]
    fn test_read_iter() {
        let mut schema = Schema::new();
        schema.add_field(FieldEntry::str("body"));
        schema.add_field(FieldEntry::i32("title"));
        let field_id_title = schema.get_field("title").unwrap();
        let disk_reader = DiskStoreReader::open(PathBuf::from(
            "/opt/rsproject/chappie/vectorvase/data/my_index",
        ))
        .unwrap();

        for field in disk_reader.iter() {
            println!(
                "field_name:{},field_id:{:?}",
                field.get_field_name(),
                field.get_field_id()
            );
            for v in field.iter() {
                println!("cow:{}", String::from_utf8_lossy(v.term().as_ref()));
                for doc in v.posting_reader().iter() {
                    println!("doc:{:?}", doc);
                    println!("content:{:?}", disk_reader.doc(doc.doc_id()).unwrap())
                }
            }
        }
    }

    #[test]
    fn test_iter() {
        let mut schema = Schema::new();
        schema.add_field(FieldEntry::str("body"));
        schema.add_field(FieldEntry::i32("title"));
        let field_id_title = schema.get_field("title").unwrap();
        let config = ConfigBuilder::default().build();
        let mut index = Index::new(&schema, config.get_engine_config(PathBuf::from(""))).unwrap();
        let mut writer1 = index.writer().unwrap();

        let mut d = Document::new();
        d.add_text(field_id_title.clone(), "bb");
        writer1.add(1, &d).unwrap();

        let mut d1 = Document::new();
        d1.add_text(field_id_title.clone(), "aa");
        writer1.add(2, &d1).unwrap();

        let reader = index.reader().unwrap();
        let p = reader
            .search(&Term::from_field_text(field_id_title, "aa"))
            .unwrap();

        for doc_freq in p.iter() {
            let doc = reader.doc(doc_freq.doc_id()).unwrap();
            println!("docid:{},doc{:?}", doc_freq.doc_id(), doc);
        }
    }

    #[test]
    fn test_search() {
        let mut schema = Schema::new();
        schema.add_field(FieldEntry::str("body"));
        schema.add_field(FieldEntry::i32("title"));
        let field_id_title = schema.get_field("title").unwrap();
        println!("field_id_title:{:?}", field_id_title.clone());
        let config = ConfigBuilder::default().build();
        let mut index = Index::new(&schema, config.get_engine_config(PathBuf::from(""))).unwrap();
        let mut writer1 = index.writer().unwrap();
        let t1 = thread::spawn(move || loop {
            let mut d = Document::new();
            d.add_i32(field_id_title.clone(), 2);
            writer1.add(1, &d).unwrap();
            break;
        });
        let mut writer2 = index.writer().unwrap();
        let t2 = thread::spawn(move || loop {
            let mut d = Document::new();
            d.add_i32(field_id_title.clone(), 2);
            writer2.add(2, &d).unwrap();
            break;
        });

        t1.join();
        t2.join();
        // let mut writer3 = index.writer().unwrap();
        // writer3.commit();
        let reader = index.reader().unwrap();
        let p = reader
            .search(&Term::from_field_i32(field_id_title, 2))
            .unwrap();

        for x in p.iter() {
            println!("{:?}", x);
        }
    }

    #[test]
    fn test_fieldcache1() {
        let buffer_pool = Arc::new(RingBuffer::new());
        let mut field = FieldCache::new(Arc::downgrade(&buffer_pool));
        let mut doc_id: DocID = 0;
        let valuea = Value::Str("aa");

        doc_id = 2;
        field.add(doc_id, &valuea).unwrap();

        doc_id = 4;
        field.add(doc_id, &valuea).unwrap();
        field.add(doc_id, &valuea).unwrap();
        doc_id = 5;
        field.add(doc_id, &valuea).unwrap();
        field.add(doc_id, &valuea).unwrap();
        field.add(doc_id, &valuea).unwrap();
        doc_id = 8;
        field.add(doc_id, &valuea).unwrap();
        field.commit().unwrap();

        let field_reader = FieldReader::new(
            field.indexs.clone(),
            field.share_bytes_block.clone(),
            field.term_count.load(Ordering::Acquire),
        );

        println!("search aa");
        let mut p = field_reader.get("aa".as_bytes()).unwrap();
        for x in p.iter() {
            println!("{:?}", x);
        }
    }

    #[test]
    fn test_fieldcache2() {
        let buffer_pool = Arc::new(RingBuffer::new());
        let mut field = FieldCache::new(Arc::downgrade(&buffer_pool));
        let mut doc_id: DocID = 0;
        let valuea = Value::Str("aa");
        let valueb = Value::Str("bb");
        let valuec = Value::Str("cc");
        let valued = Value::Str("dd");
        let value1 = Value::I32(1);
        let value2 = Value::I32(2);
        // field.add(doc_id, &valuea).unwrap(); // aa
        field.add(doc_id, &valueb).unwrap(); //
        field.add(doc_id, &value1).unwrap();

        doc_id = 1;
        field.add(doc_id, &valuea).unwrap();
        field.add(doc_id, &valuec).unwrap();

        doc_id = 2;
        field.add(doc_id, &valuea).unwrap();
        field.add(doc_id, &valueb).unwrap();
        field.add(doc_id, &valuec).unwrap();
        field.add(doc_id, &valued).unwrap();
        field.add(doc_id, &value1).unwrap();
        field.add(doc_id, &value2).unwrap();

        doc_id = 3;
        field.add(doc_id, &valuea).unwrap();
        field.add(doc_id, &valuec).unwrap();
        field.add(doc_id, &valued).unwrap();

        doc_id = 4;
        field.add(doc_id, &valuea).unwrap();
        field.add(doc_id, &valued).unwrap();
        field.add(doc_id, &value1).unwrap();
        field.add(doc_id, &value2).unwrap();

        doc_id = 5;
        field.add(doc_id, &valuec).unwrap();
        field.add(doc_id, &valued).unwrap();
        field.commit().unwrap();

        let field_reader = field.reader();

        println!("search aa");
        let mut p = field_reader.get("aa".as_bytes()).unwrap();
        for x in p.iter() {
            println!("{:?}", x);
        }

        println!("search bb");
        p = field_reader.get("bb".as_bytes()).unwrap();
        for x in p.iter() {
            println!("{:?}", x);
        }

        println!("search cc");
        p = field_reader.get("cc".as_bytes()).unwrap();
        for x in p.iter() {
            println!("{:?}", x);
        }

        println!("search dd");
        p = field_reader.get("dd".as_bytes()).unwrap();
        for x in p.iter() {
            println!("{:?}", x);
        }

        let mut v = vec![0u8; 0];
    }

    #[test]
    fn test_wal_iter() {
        let mut schema = Schema::with_vector(VectorEntry::new(
            "vector1",
            AnnType::HNSW,
            TensorEntry::new(1, [4], schema::VectorType::F32),
        ));
        schema.add_field(FieldEntry::str("body"));
        schema.add_field(FieldEntry::i32("title"));

        let field_id_title = schema.get_field("title").unwrap();
        let config = ConfigBuilder::default()
            .data_path(PathBuf::from("./data_wal"))
            .build();
        let mut collect =
            Engine::new(&schema, config.get_engine_config(PathBuf::from(""))).unwrap();
        //let mut writer1 = index.writer().unwrap();
        {
            //writer1.add(1, &d).unwrap();

            let mut d1 = Document::new();
            d1.add_text(field_id_title.clone(), "aa");

            let v1 = Vector::from_array([0.0, 0.0, 0.0, 1.0], d1).unwrap();

            collect.add(v1).unwrap();

            let mut d2 = Document::new();
            d2.add_text(field_id_title.clone(), "cc");
            let v2 = Vector::from_array([0.0, 0.0, 1.0, 0.0], d2).unwrap();

            collect.add(v2).unwrap();

            let mut d3 = Document::new();
            d3.add_text(field_id_title.clone(), "aa");

            let v3 = Vector::from_array([0.0, 1.0, 0.0, 0.0], d3).unwrap();

            collect.add(v3).unwrap();

            let mut d4 = Document::new();
            d4.add_text(field_id_title.clone(), "bb");
            let v4 = Vector::from_array([1.0, 0.0, 0.0, 0.0], d4).unwrap();
            collect.add(v4).unwrap();
        }
        println!(
            "offset:{}",
            collect.0.index_base.wal.get_borrow().offset() // offset:128
        );
    }

    #[test]
    fn test_wal_read() {
        let mut schema = Schema::with_vector(VectorEntry::new(
            "vector1",
            AnnType::HNSW,
            TensorEntry::new(1, [4], schema::VectorType::F32),
        ));
        schema.add_field(FieldEntry::str("body"));
        schema.add_field(FieldEntry::i32("title"));
        let p = PathBuf::from("./data_wal/my_index/data.wal");
        let wal = Wal::open(&p, DEFAULT_WAL_FILE_SIZE, &IOType::MMAP).unwrap();

        let mut wal_iter = wal.iter::<Vector>(TensorEntry::new(1, [4], schema::VectorType::F32));
        while let Some((doc_offset, v)) = wal_iter.next() {
            println!("{},{:?},{:?}", doc_offset, v.v.as_bytes(), v.payload);
        }
        println!("offset:{}", wal_iter.offset());
    }

    #[test]
    fn test_open_collect() {
        let mut schema = Schema::with_vector(VectorEntry::new(
            "vector1",
            AnnType::HNSW,
            TensorEntry::new(1, [4], schema::VectorType::F32),
        ));
        let field_id_title = schema.get_field("title").unwrap();
        schema.add_field(FieldEntry::str("body"));
        schema.add_field(FieldEntry::i32("title"));
        //  let p = PathBuf::from("./data_wal/my_index/data.wal");

        let config = ConfigBuilder::default()
            .data_path(PathBuf::from("./data_wal"))
            .build();
        let collect = Engine::open(&schema, config.get_engine_config(PathBuf::from(""))).unwrap();
        let reader = collect.reader();
        let p = reader
            .query(
                &Tensor::from_vec(
                    vec![0.0f32, 0.0, 1.0, 0.0],
                    1,
                    Shape::from_array([4]),
                    &wwml::Device::Cpu,
                )
                .unwrap(),
                4,
                &None,
            )
            .unwrap();

        for n in p.iter() {
            let doc = reader.vector(n.doc_id()).unwrap();
            println!("docid:{},doc{:?}", n.doc_id(), unsafe {
                doc.v.as_slice::<f32>()
            });
        }

        let mut d5 = Document::new();
        d5.add_text(field_id_title.clone(), "cc");
        let v5 = Vector::from_array([0.0, 0.0, 1.0, 1.0], d5).unwrap();
        collect.add(v5).unwrap();

        let mut d6 = Document::new();
        d6.add_text(field_id_title.clone(), "aa");
        let v6 = Vector::from_array([0.0, 1.0, 1.0, 0.0], d6).unwrap();
        collect.add(v6).unwrap();

        let mut d7 = Document::new();
        d7.add_text(field_id_title.clone(), "ff");
        let v7 = Vector::from_array([1.0, 0.0, 0.0, 1.0], d7).unwrap();
        collect.add(v7).unwrap();

        let mut d8 = Document::new();
        d8.add_text(field_id_title.clone(), "gg");
        let d8 = Vector::from_array([1.0, 1.0, 0.0, 0.0], d8).unwrap();
        collect.add(d8).unwrap();
    }
}
