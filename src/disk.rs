use super::schema::{BinarySerialize, FieldID, Schema, TensorEntry, VectorSerialize};
use super::schema::{DocID, Document};
use super::util::error::{GyError, GyResult};
use super::util::fs::{self};
use super::util::fst::{FstBuilder, FstReader, FstReaderIter};
use super::{EngineReader, Meta};
use crate::ann::Emptyer;
use crate::config::DiskFileMeta;
use crate::config::TermRange;
use crate::config::DATA_FILE;
use crate::config::META_FILE;
use crate::fs::FileManager;
use crate::iocopy;
use crate::schema::VUInt;
use crate::schema::VarIntSerialize;
use crate::searcher::BlockReader;
use crate::util::asyncio::{WaitGroup, Worker};
use crate::util::bloom::GyBloom;
use crate::util::fs::GyFile;
use crate::util::fst::FstCow;
use crate::Ann;
use crate::DocFreq;
use crate::FieldEntry;
use crate::Neighbor;
use crate::PostingReader;
use crate::Term;
use crate::Vector;
use art_tree::Key;
use bytes::BytesMut;
use bytes::{Buf, BufMut};
use memmap2::Mmap;
use std::borrow::Borrow;
use std::fs::{read, File};
use std::io::{BufWriter, Read};
use std::io::{Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;
use std::sync::Arc;
use wwml::similarity::TensorSimilar;
use wwml::TensorView;
use wwml::{Tensor, TensorProto};

use std::cmp::Ordering as CmpOrdering;
use std::iter::Peekable;

//  +---------------------+
//  |                     |
//  |       Doc           |
//  |                     |
//  +---------------------+
//  |                     |
//  |       vector        |
//  |                     |
//  +---------------------+
//  |       ......        |
//  +---------------------+
//  |                     |
//  |       Posting       |
//  |                     |
//  +---------------------+
//  |                     |
//  |       Bloom         |
//  |                     |
//  +---------------------+
//  |                     |
//  |       Fst           |
//  |                     |
//  +---------------------+
//  |       ......        |
//  +---------------------+
//  |                     |
//  |       doc_meta      |
//  |                     |
//  +---------------------+
//  |                     |
//  |       field_meta    |
//  |                     |
//  +---------------------+
//  |                     |
//  |       footer        |
//  |                     |
//  +---------------------+

const KB: usize = 1 << 10;
const MB: usize = KB * KB;
const DEFAULT_BLOCK_SIZE: usize = 4 * KB + KB;
const FOOTER_LEN: usize = 64;

const MAGIC: &'static [u8] = b"\xD4\x56\x3F\x35\xE0\xEF\x09\x7A";

//压缩类型
enum CompressionType {
    No,
    LZ4,
    LowercaseAscii,
    Zstandard,
    Snappy,
}

impl GyWrite for File {
    fn get_pos(&mut self) -> GyResult<usize> {
        let pos = self.seek(SeekFrom::Current(0))?;
        Ok(pos as usize)
    }
}

struct CompactionMergeMuch<T: Iterator> {
    readers: Vec<Peekable<T>>,
}

impl<T: Iterator> CompactionMergeMuch<T>
where
    T::Item: Ord,
{
    fn new(readers: Vec<Peekable<T>>) -> CompactionMergeMuch<T> {
        Self { readers }
    }

    fn merge(mut self) -> impl Iterator<Item = Vec<Option<T::Item>>> {
        std::iter::from_fn(move || {
            let mut min_indices = Vec::new();
            let mut min_value: Option<&T::Item> = None;

            for (i, reader) in self.readers.iter_mut().enumerate() {
                if let Some(peek) = reader.peek() {
                    match min_value {
                        Some(value) => match peek.cmp(value) {
                            CmpOrdering::Less => {
                                min_value = Some(peek);
                                min_indices.clear();
                                min_indices.push(i);
                            }
                            CmpOrdering::Equal => {
                                min_indices.push(i);
                            }
                            CmpOrdering::Greater => {}
                        },
                        None => {
                            min_value = Some(peek);
                            min_indices.push(i);
                        }
                    }
                }
            }

            if min_indices.is_empty() {
                None
            } else {
                let mut result = Vec::with_capacity(self.readers.len());
                (0..self.readers.len()).for_each(|_| result.push(None));
                for &i in &min_indices {
                    result[i] = self.readers[i].next();
                }
                Some(result)
            }
        })
    }
}

struct CompactionMergerTwo<T: Iterator> {
    a: Peekable<T>,
    b: Peekable<T>,
}

//你好
impl<T: Iterator> CompactionMergerTwo<T>
where
    T::Item: Ord,
{
    fn new(a: Peekable<T>, b: Peekable<T>) -> CompactionMergerTwo<T> {
        Self { a, b }
    }

    fn merge(mut self) -> impl Iterator<Item = (Option<T::Item>, Option<T::Item>)>
    where
        Self: Sized,
    {
        std::iter::from_fn(move || match (self.a.peek(), self.b.peek()) {
            (Some(v1), Some(v2)) => match v1.cmp(v2) {
                CmpOrdering::Less => Some((self.a.next(), None)),
                CmpOrdering::Greater => Some((None, self.b.next())),
                CmpOrdering::Equal => Some((self.a.next(), self.b.next())),
            },
            (Some(_), None) => Some((self.a.next(), None)),
            (None, Some(_)) => Some((None, self.b.next())),
            (None, None) => None,
        })
    }
}

// struct BufferFileWriter {
//     BufWriter<File>
// }

fn open_file_stream(fname: &str) -> GyResult<BufWriter<File>> {
    let file = File::open(fname)?;
    let buf_writer = BufWriter::new(file);
    Ok(buf_writer)
}

struct MergeStore {}

pub fn merge_much(readers: &[VectorStore], new_fname: &Path, level: usize) -> GyResult<()> {
    let mut schema = readers[0].get_store().meta.get_schema();
    let mut new_schema: Option<Schema> = None;
    for e in &readers[1..] {
        new_schema = Some(schema.merge(&e.get_store().meta.get_schema()));
        schema = new_schema.as_ref().unwrap();
    }

    let mut writer = DiskStoreWriter::new(new_fname)?;
    let doc_num = readers.iter().map(|r| r.get_store().doc_num()).sum();
    let parant: Vec<String> = readers
        .iter()
        .map(|r| r.get_store().meta.get_collection_name().to_string())
        .collect();
    let mut doc_meta: Vec<usize> =
        Vec::with_capacity(readers.iter().map(|r| r.get_store().doc_size()).sum());
    // Write the doc blocks from all readers sequentially.
    let mut total_doc_end = 0;
    for reader in readers {
        writer.write_doc_block(reader.get_store().doc_block())?;
        total_doc_end += reader.get_store().doc_block().len();
    }
    // Collect all doc meta information.
    let mut accumulated_end = 0;
    for reader in readers {
        for &meta in &reader.get_store().doc_meta {
            doc_meta.push(meta + accumulated_end);
        }
        accumulated_end += reader.get_store().doc_end;
    }
    writer.doc_end = total_doc_end;

    // Merge the vector fields across all readers.
    let mut v: Option<Ann<TensorView<'static>>> = None;
    let mut x: &Ann<TensorView<'static>> = Arc::as_ref(&readers[0].get_store().vector_field);
    for e in &readers[1..] {
        let merged: Ann<TensorView<'static>> = x.merge(&e.get_store().vector_field)?;
        v = Some(merged);
        x = v.as_ref().unwrap();
    }

    writer.write_vector(x)?;

    let mut bytes = BytesMut::with_capacity(4 * 1024).writer();
    let mut fst_buf: Vec<u8> = Vec::with_capacity(4 * KB);
    let reader_iters = readers
        .iter()
        .map(|r| r.get_store().iter().peekable())
        .collect::<Vec<_>>();
    let reader_merger = CompactionMergeMuch::new(reader_iters);
    let term_range_list: Vec<TermRange> = Vec::new();
    for field_readers in reader_merger.merge() {
        assert!(
            field_readers
                .iter()
                .filter_map(|x| x.as_ref()) // 过滤出 Some 值
                .fold((true, None), |(equal, first), current| {
                    match first {
                        Some(f) => (equal && f == current, Some(f)), // 比较当前值与第一个值
                        None => (true, Some(current)),               // 记录第一个值
                    }
                })
                .0
        );

        let mut bloom = GyBloom::new(
            field_readers
                .iter()
                .filter_map(|item| item.as_ref().map(|r| r.get_term_count()))
                .sum::<usize>()
                .max(1),
        );
        let mut term_count: usize = 0;
        let mut fst = IndexBlock::new(fst_buf);
        let field_merger = CompactionMergeMuch::new(
            field_readers
                .iter()
                .enumerate()
                .filter_map(|(idx, r)| r.as_ref().map(|r1| r1.iter_with_index(idx).peekable()))
                .collect(),
        );

        field_merger
            .merge()
            .try_for_each(|term_item| -> GyResult<()> {
                assert!(
                    term_item
                        .iter()
                        .filter_map(|x| x.as_ref()) // 过滤出 Some 值
                        .fold((true, None), |(equal, first), current| {
                            match first {
                                Some(f) => (equal && f == current, Some(f)), // 比较当前值与第一个值
                                None => (true, Some(current)),               // 记录第一个值
                            }
                        })
                        .0
                );

                let mut idx = 0;
                let mut disk_posting_writer = DiskPostingWriter::new(&mut bytes);
                let mut doc_count: usize = 0;
                //    let term_iter = term_item.iter();
                for (i, item) in term_item.iter().enumerate() {
                    if let Some(iterm) = item {
                        let posting_reader = iterm.posting_reader();
                        for doc_freq in posting_reader.iter() {
                            let adjusted_doc_id = doc_freq.doc_id()
                                + readers[..iterm.idx()]
                                    .iter()
                                    .map(|r| r.get_store().doc_size() as u64)
                                    .sum::<u64>();
                            disk_posting_writer.add(adjusted_doc_id, doc_freq.freq())?;
                        }
                        doc_count += posting_reader.get_doc_count();
                        idx = i;
                    }
                }

                let offset = writer.write_posting(doc_count, bytes.get_ref())?;
                fst.add(term_item[idx].as_ref().unwrap().term(), offset as u64)?;
                bloom.set(term_item[idx].as_ref().unwrap().term());
                term_count += 1;
                bytes.get_mut().clear();
                Ok(())
            })?;
        fst_buf = fst.finish()?;
        let bh1 = writer.write_bloom(&bloom)?;
        let bh2 = writer.write_fst(&fst_buf)?;
        writer.finish_field(term_count, bh1, bh2)?;
        fst_buf.clear();
    }
    writer.write_doc_meta(&doc_meta)?;
    // 写入每个域的 meta
    writer.write_field_meta()?;
    let newfsize = writer.close()? as usize;
    drop(writer);
    if let Some(dir_path) = new_fname.parent() {
        let meta = DiskFileMeta::new(
            Meta::new(new_schema.unwrap()),
            dir_path.file_name().unwrap().to_str().unwrap(),
            parant,
            term_range_list,
            newfsize,
            doc_num,
            level,
        );
        FileManager::to_json_file(&meta, dir_path.join(META_FILE))?;
    }
    Ok(())
}

//合并索引
pub fn persist_collection(
    reader: EngineReader,
    schema: &Schema,
    refname: &Path,
) -> GyResult<usize> {
    let index_reader = reader.index_reader();
    let fname = index_reader.get_wal_path();
    let doc_end = index_reader.offset()?;
    let mut writer = DiskStoreWriter::with_offset(
        &fname,
        doc_end,
        index_reader.get_index_base().doc_offset.get_borrow().len(),
    )?;
    writer.write_vector(reader.vector_field.0.read()?.borrow())?;
    let mut buf = Vec::with_capacity(4 * KB);
    let mut fst_buf: Vec<u8> = Vec::with_capacity(4 * KB);
    // write field
    let mut term_range_list: Vec<TermRange> = Vec::new();
    for field in index_reader.iter() {
        // let mut term_offset_cache: HashMap<Vec<u8>, usize> = HashMap::new();
        let mut fst = IndexBlock::new(fst_buf);
        let mut bloom = GyBloom::new(field.get_term_count().max(1));
        let indexs = field.indexs.read()?;
        let mut iter = indexs.iter();
        if let Some((b_min, p)) = iter.next() {
            let mut b_max = b_min; // 临时将最大值设置为第一个元素

            let (doc_count, start_addr, end_addr) = {
                let posting = (*p).read()?;
                (
                    posting.doc_count,
                    posting.byte_addr.load(Ordering::SeqCst),
                    posting.doc_freq_addr.load(Ordering::SeqCst),
                )
            };
            // write posting
            let posting_buffer = field.posting_buffer(start_addr, end_addr)?;
            for rb in posting_buffer.iter() {
                buf.write(rb)?;
            }
            let offset = writer.write_posting(doc_count, buf.as_slice())?;
            bloom.set(b_min.borrow());
            fst.add(b_min.borrow(), offset as u64)?;
            //term_offset_cache.insert(b.to_bytes(), offset);
            buf.clear();
            // 迭代剩余的元素
            for (b, p) in iter {
                let (doc_count, start_addr, end_addr) = {
                    let posting = (*p).read()?;
                    (
                        posting.doc_count,
                        posting.byte_addr.load(Ordering::SeqCst),
                        posting.doc_freq_addr.load(Ordering::SeqCst),
                    )
                };
                // write posting
                let posting_buffer = field.posting_buffer(start_addr, end_addr)?;
                for rb in posting_buffer.iter() {
                    buf.write(rb)?;
                }
                let offset = writer.write_posting(doc_count, buf.as_slice())?;
                bloom.set(b.borrow());
                fst.add(b.borrow(), offset as u64)?;
                //term_offset_cache.insert(b.to_bytes(), offset);
                buf.clear();
                // 最后一次迭代的 b 就是最大值
                b_max = b;
            }
            // 你可以在这里使用 min_value 和 max_value
            term_range_list.push(TermRange::new(b_min.to_bytes(), b_max.to_bytes()));
        }
        let bh1 = writer.write_bloom(&bloom)?;
        fst_buf = fst.finish()?;
        let bh2 = writer.write_fst(&fst_buf)?;
        writer.finish_field(field.get_term_count(), bh1, bh2)?;
        fst_buf.clear();
    }

    // 写入文档和偏移量关系 meta
    writer.write_doc_meta(&index_reader.get_doc_offset())?;
    // 写入每个域的 meta
    writer.write_field_meta()?;
    let newfsize = writer.close()? as usize;
    //println!("newfsize:{}", newfsize);
    index_reader.reopen_wal(newfsize)?;
    drop(writer);
    if let Some(dir_path) = refname.parent() {
        //println!("fname:{:?},rename:{:?}", fname, refname);
        std::fs::rename(&fname, &refname)?;
        let meta = DiskFileMeta::new(
            Meta::new(schema.clone()),
            dir_path.file_name().unwrap().to_str().unwrap(),
            Vec::new(),
            term_range_list,
            newfsize,
            reader.doc_num(),
            0,
        );
        FileManager::to_json_file(&meta, dir_path.join(META_FILE))?;
    }

    Ok(newfsize)
}

unsafe impl Sync for VectorStore {}
unsafe impl Send for VectorStore {}

#[derive(Clone)]
pub struct VectorStore(Arc<DiskStoreReader>);

impl VectorStore {
    pub fn open<P: AsRef<Path>>(path: P) -> GyResult<VectorStore> {
        Ok(VectorStore(Arc::new(DiskStoreReader::open(path)?)))
    }

    pub fn reader(&self) -> VectorStoreReader {
        VectorStoreReader {
            reader: self.0.clone(),
            w: self.0.wg.worker(),
        }
    }

    pub async fn wait(&self) {
        self.0.wg.wait().await
    }

    pub fn level(&self) -> usize {
        self.0.level()
    }

    pub fn file_size(&self) -> usize {
        self.0.file_size()
    }

    pub fn file_path(&self) -> &Path {
        self.0.file_path()
    }

    pub fn get_store(&self) -> &DiskStoreReader {
        &self.0
    }

    pub fn collection_name(&self) -> &str {
        self.0.collection_name()
    }
}

unsafe impl Send for VectorStoreReader {}

pub struct VectorStoreReader {
    reader: Arc<DiskStoreReader>,
    w: Worker,
}

impl BlockReader for VectorStoreReader {
    fn query(&self, tensor: &Tensor, k: usize, term: &Option<Term>) -> GyResult<Vec<Neighbor>> {
        self.reader.query(tensor, k, term)
    }

    fn vector(&self, doc_id: DocID) -> GyResult<Vector> {
        self.reader.vector(doc_id)
    }

    fn search(&self, term: &Term) -> GyResult<PostingReader> {
        Ok(PostingReader::Disk(self.reader.search(term)?))
    }
}

unsafe impl Send for DiskStoreReader {}

pub struct DiskStoreReader {
    meta: DiskFileMeta,
    vector_field: Arc<Ann<TensorView<'static>>>,
    fields_meta: Vec<FieldHandle>,
    blooms: Vec<Arc<GyBloom>>,
    doc_meta: Vec<usize>,
    doc_end: usize,
    file: GyFile,
    fsize: usize,
    mmap: Arc<Mmap>,
    wg: WaitGroup,
}

impl DiskStoreReader {
    pub fn open<P: AsRef<Path>>(path: P) -> GyResult<DiskStoreReader> {
        let data_path = PathBuf::new().join(path.as_ref()).join(DATA_FILE);
        let meta_path = PathBuf::new().join(path).join(META_FILE);
        let meta: DiskFileMeta = FileManager::from_json_file(&meta_path)?;
        let file = GyFile::open(data_path)?; //OpenOptions::new().read(true).open(data_path)?;
        let file_size = file.fsize()?;
        if file_size < FOOTER_LEN {
            return Err(GyError::ErrFooter);
        }
        let footer_pos = file_size - FOOTER_LEN;
        let mut footer = [0u8; FOOTER_LEN as usize];
        file.read_at(&mut footer, footer_pos as u64)?;
        //判断魔数是否正确
        if &footer[FOOTER_LEN as usize - MAGIC.len()..FOOTER_LEN as usize] != MAGIC {
            return Err(GyError::ErrBadMagicNumber);
        }
        let mut c = std::io::Cursor::new(&footer[..]);
        let doc_end = usize::binary_deserialize(&mut c)?;
        let doc_meta_bh: BlockHandle = BlockHandle::binary_deserialize(&mut c)?;
        let field_meta_bh = BlockHandle::binary_deserialize(&mut c)?;
        let vector_meta_bh = BlockHandle::binary_deserialize(&mut c)?;
        let mmap: Mmap = unsafe {
            memmap2::MmapOptions::new()
                .map(file.file())
                .map_err(|e| format!("mmap failed: {}", e))?
        };

        let fields_meta = Self::read_at_bh::<Vec<FieldHandle>>(&mmap, field_meta_bh)?;
        let doc_meta = Self::read_at_bh::<Vec<usize>>(&mmap, doc_meta_bh)?;
        let mut blooms: Vec<Arc<GyBloom>> = Vec::with_capacity(fields_meta.len());
        for meta in fields_meta.iter() {
            let bloom = Self::read_at_bh::<GyBloom>(&mmap, meta.bloom_bh)?;
            blooms.push(Arc::new(bloom));
        }
        let mut mmap_reader = MmapReader::new(&mmap, vector_meta_bh.start(), vector_meta_bh.end());
        let vector_index =
            Self::read_vector_index::<Ann<TensorView>>(&mut mmap_reader, meta.tensor_entry())?;
        assert!(fields_meta.len() == meta.get_fields().len());
        assert!(doc_meta.len() == meta.get_doc_num());
        Ok(Self {
            meta: meta,
            vector_field: Arc::new(vector_index),
            fields_meta: fields_meta,
            blooms: blooms,
            doc_meta: doc_meta,
            doc_end: doc_end,
            file: file,
            fsize: file_size as usize,
            mmap: Arc::new(mmap),
            wg: WaitGroup::new(),
        })
    }

    fn read_vector_index<T: VectorSerialize>(
        r: &mut MmapReader,
        entry: &TensorEntry,
    ) -> GyResult<T> {
        let v = T::vector_deserialize(r, entry)?;
        Ok(v)
    }

    fn read_at_bh<T: BinarySerialize>(mmap: &Mmap, bh: BlockHandle) -> GyResult<T> {
        let mut r = mmap[bh.start()..bh.end()].reader();
        let v = T::binary_deserialize(&mut r)?;
        Ok(v)
    }

    fn read_at<T: BinarySerialize>(&self, offset: usize) -> GyResult<T> {
        let mut r = self.mmap[offset..].reader();
        let v = T::binary_deserialize(&mut r)?;
        Ok(v)
    }

    pub(crate) fn vector(&self, doc_id: DocID) -> GyResult<Vector> {
        let doc_offset = self.doc_meta[doc_id as usize];
        let doc = self.read_vector::<Vector>(doc_offset)?;
        Ok(doc)
    }

    pub(crate) fn query(
        &self,
        v: &Tensor,
        k: usize,
        term: &Option<Term>,
    ) -> GyResult<Vec<Neighbor>> {
        if let Some(t) = term {
            todo!()
        } else {
            self.vector_field
                .query::<Tensor, Emptyer, Emptyer>(v, k, None, None)
        }
    }

    pub(crate) fn search(&self, term: &Term) -> GyResult<DiskPostingReader> {
        let field_id = term.field_id().id();
        let field_reader = self.field_reader(field_id)?;
        field_reader.find(term.bytes_value())
    }

    fn read_vector<T: VectorSerialize>(&self, offset: usize) -> GyResult<T> {
        let mut mmap_reader = MmapReader::new(&self.mmap, offset, self.fsize);
        let v = T::vector_deserialize(&mut mmap_reader, self.meta.tensor_entry())?;
        Ok(v)
    }

    pub fn field_reader<'a>(&'a self, field_id: u32) -> GyResult<DiskFieldReader<'a>> {
        assert!((field_id as usize) < self.fields_meta.len());
        assert!((field_id as usize) < self.meta.get_fields().len());
        let field_entry = self.meta.get_fields()[field_id as usize].clone();
        assert!(field_id == field_entry.get_field_id().id());
        let field_handle = &self.fields_meta[field_id as usize];
        self.get_field_reader(field_entry, field_handle)
    }

    fn get_field_reader<'a>(
        &'a self,
        field_entry: FieldEntry,
        field_handle: &FieldHandle,
    ) -> GyResult<DiskFieldReader<'a>> {
        let fst =
            FstReader::load(&self.mmap[field_handle.fst_bh.start()..field_handle.fst_bh.end()]);
        Ok(DiskFieldReader {
            term_count: field_handle.term_count,
            fst: fst,
            bloom: self.blooms[field_entry.get_field_id().id() as usize].clone(),
            mmap: self.mmap.clone(),
            field_entry: field_entry,
        })
    }

    pub fn doc_size(&self) -> usize {
        self.doc_meta.len()
    }

    pub fn file_path(&self) -> &Path {
        self.file.path()
    }

    pub fn doc_num(&self) -> usize {
        self.meta.get_doc_num()
    }

    pub fn level(&self) -> usize {
        self.meta.get_level()
    }

    pub fn file_size(&self) -> usize {
        self.meta.file_size()
    }

    pub fn collection_name(&self) -> &str {
        self.meta.get_collection_name()
    }

    pub(crate) fn doc_block(&self) -> &[u8] {
        &self.mmap[0..self.doc_end]
    }

    pub fn doc_reader<'a>(&'a self) -> GyResult<DiskDocReader<'a>> {
        Ok(DiskDocReader { reader: self })
    }

    pub fn doc(&self, doc_id: DocID) -> GyResult<Document> {
        let doc_offset = self.doc_meta[doc_id as usize];
        let doc = self.read_at::<Document>(doc_offset)?;
        Ok(doc)
    }

    pub fn iter(&self) -> DiskStoreReaderIter {
        let handle_iter = self.fields_meta.iter();
        let field_iter = self.meta.get_fields().iter();
        DiskStoreReaderIter {
            reader: self,
            handle_iter: handle_iter,
            field_iter: field_iter,
        }
    }
}

use core::slice::Iter;
pub struct DiskStoreReaderIter<'a> {
    reader: &'a DiskStoreReader,
    handle_iter: Iter<'a, FieldHandle>,
    field_iter: Iter<'a, FieldEntry>,
}

impl<'a> PartialOrd for DiskFieldReader<'a> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.get_field_id().partial_cmp(other.get_field_id())
    }
}

impl<'a> PartialEq for DiskFieldReader<'a> {
    fn eq(&self, other: &Self) -> bool {
        self.get_field_id() == other.get_field_id()
    }
}

impl<'a> Eq for DiskFieldReader<'a> {}

impl<'a> Ord for DiskFieldReader<'a> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.get_field_id().cmp(other.get_field_id())
    }
}

impl<'a> Iterator for DiskStoreReaderIter<'a> {
    type Item = DiskFieldReader<'a>;
    fn next(&mut self) -> Option<Self::Item> {
        let handle = self.handle_iter.next()?;
        let field = self.field_iter.next()?;
        self.reader.get_field_reader(field.clone(), handle).ok()
    }
}

pub struct DiskDocReader<'a> {
    reader: &'a DiskStoreReader,
}

pub struct DiskDocReaderIter<'a> {
    reader: &'a DiskStoreReader,
    i: usize,
}

impl<'a> Iterator for DiskDocReaderIter<'a> {
    type Item = Document;
    fn next(&mut self) -> Option<Self::Item> {
        if self.i >= self.reader.doc_size() {
            return None;
        }
        let doc = self.reader.doc(self.i as u64).ok()?;
        self.i += 1;
        Some(doc)
    }
}

pub struct DiskFieldReader<'a> {
    term_count: usize,
    fst: FstReader<'a>,
    bloom: Arc<GyBloom>,
    mmap: Arc<Mmap>,
    field_entry: FieldEntry,
}

impl<'a> DiskFieldReader<'a> {
    pub fn find(&self, term: &[u8]) -> GyResult<DiskPostingReader> {
        if !self.bloom.check(term) {
            return Err(GyError::ErrNotFoundTermFromBloom(
                std::str::from_utf8(term).unwrap().to_string(),
            ));
        }
        let offset = self.fst.get(term)?;
        self.get(offset as usize)
    }

    pub fn iter(&self) -> DiskFieldReaderIter {
        DiskFieldReaderIter {
            iter: self.fst.iter(),
            mmap: self.mmap.clone(),
            i: 0,
        }
    }

    pub fn iter_with_index(&self, i: usize) -> DiskFieldReaderIter {
        DiskFieldReaderIter {
            iter: self.fst.iter(),
            mmap: self.mmap.clone(),
            i: i,
        }
    }

    pub fn get_field_name(&self) -> &str {
        &self.field_entry.get_name()
    }

    pub fn get_field_id(&self) -> &FieldID {
        &self.field_entry.get_field_id()
    }

    pub fn get_term_count(&self) -> usize {
        self.term_count
    }

    fn get(&self, offset: usize) -> GyResult<DiskPostingReader> {
        Ok(DiskPostingReader::new(self.mmap.clone(), offset)?)
    }
}

pub struct DiskFieldReaderIter<'a> {
    iter: FstReaderIter<'a>,
    mmap: Arc<Mmap>,
    i: usize,
}

pub struct TermItem(FstCow, DiskPostingReader, usize);

impl TermItem {
    pub fn term(&self) -> &[u8] {
        self.0.as_ref()
    }

    pub fn posting_reader(&self) -> &DiskPostingReader {
        &self.1
    }

    pub fn idx(&self) -> usize {
        self.2
    }
}

impl<'a> Iterator for DiskFieldReaderIter<'a> {
    type Item = TermItem;
    fn next(&mut self) -> Option<Self::Item> {
        let item = self.iter.next()?;
        Some(TermItem(
            item.0.clone(),
            DiskPostingReader::new(self.mmap.clone(), item.1 as usize).unwrap(),
            self.i,
        ))
    }
}

impl PartialOrd for TermItem {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.0.as_ref().partial_cmp(other.0.as_ref())
    }
}

impl PartialEq for TermItem {
    fn eq(&self, other: &Self) -> bool {
        self.0.as_ref() == other.0.as_ref()
    }
}

impl Eq for TermItem {}

impl Ord for TermItem {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0.as_ref().cmp(other.0.as_ref())
    }
}

pub struct DiskPostingWriter<'a, T: Write> {
    last_docid: DocID,
    w: &'a mut T,
}

impl<'a, T: Write> DiskPostingWriter<'a, T> {
    fn new(w: &'a mut T) -> DiskPostingWriter<'a, T> {
        DiskPostingWriter {
            last_docid: 0,
            w: w,
        }
    }
}
//xxyyzz
impl<'a, T: Write> DiskPostingWriter<'a, T> {
    fn add(&mut self, doc_id: DocID, freq: u32) -> GyResult<()> {
        DocFreq(doc_id - self.last_docid, freq).binary_serialize(&mut self.w)?;
        self.last_docid = doc_id;
        Ok(())
    }
}

pub struct DiskPostingReader {
    doc_count: usize,
    snapshot: DiskSnapshotReader,
}

impl DiskPostingReader {
    fn new(mmap: Arc<Mmap>, offset: usize) -> GyResult<DiskPostingReader> {
        let mut r = mmap[offset..].reader();
        let (doc_count, i) = VUInt::binary_deserialize(&mut r)?;
        Ok(DiskPostingReader {
            doc_count: doc_count.val() as usize,
            snapshot: DiskSnapshotReader::new(mmap, offset + i)?,
        })
    }

    pub fn get_doc_count(&self) -> usize {
        self.doc_count
    }

    pub fn iter(&self) -> DiskPostingReaderIter {
        DiskPostingReaderIter {
            last_docid: 0,
            snapshot: self.snapshot.clone(),
        }
    }
}

pub struct DiskPostingReaderIter {
    last_docid: DocID,
    snapshot: DiskSnapshotReader,
}

impl Iterator for DiskPostingReaderIter {
    type Item = DocFreq;
    fn next(&mut self) -> Option<Self::Item> {
        return match DocFreq::binary_deserialize(&mut self.snapshot) {
            Ok(mut doc_freq) => {
                self.last_docid += doc_freq.doc_id() >> 1;
                doc_freq.0 = self.last_docid;
                Some(doc_freq)
            }
            Err(_) => None,
        };
    }
}

#[derive(Clone)]
struct DiskSnapshotReader {
    mmap: Arc<Mmap>,
    offset: usize,
    end: usize,
}

impl DiskSnapshotReader {
    fn new(mmap: Arc<Mmap>, mut offset: usize) -> GyResult<DiskSnapshotReader> {
        let mut r = mmap[offset..].reader();
        let (length, i) = VUInt::binary_deserialize(&mut r)?;
        let l = length.val() as usize;
        offset += i;
        let snapshot = Self {
            mmap: mmap,
            offset: offset,
            end: offset + l,
        };
        Ok(snapshot)
    }
}

impl Read for DiskSnapshotReader {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        if self.offset >= self.end {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "io EOF",
            ));
        }
        let i = iocopy!(buf, &self.mmap[self.offset..]);
        self.offset += i;
        Ok(i)
    }
}

struct DiskSnapshotReaderIter {
    mmap: Arc<Mmap>,
    offset: usize,
    lenght: usize,
}

pub struct DiskStoreWriter {
    offset: usize,
    filter_block: FilterBlock, //布隆过滤器
    // index_block: IndexBlock,   //fst
    field_bhs: Vec<FieldHandle>,
    doc_meta_bh: BlockHandle,
    field_meta_bh: BlockHandle,
    vector_meta_bh: BlockHandle,
    file: File,
    fname: PathBuf,
    doc_end: usize,
}

#[derive(Debug)]
struct FieldHandle {
    term_count: usize,
    fst_bh: BlockHandle,
    bloom_bh: BlockHandle,
}

impl BinarySerialize for FieldHandle {
    fn binary_serialize<W: Write>(&self, writer: &mut W) -> GyResult<()> {
        self.term_count.binary_serialize(writer)?;
        self.fst_bh.binary_serialize(writer)?;
        self.bloom_bh.binary_serialize(writer)
    }

    fn binary_deserialize<R: Read>(reader: &mut R) -> GyResult<Self> {
        let term_count = usize::binary_deserialize(reader)?;
        let fst_bh = BlockHandle::binary_deserialize(reader)?;
        let bloom_bh = BlockHandle::binary_deserialize(reader)?;
        Ok(FieldHandle {
            term_count,
            fst_bh,
            bloom_bh,
        })
    }
}

#[derive(Default, Debug, Clone, Copy)]
struct BlockHandle(usize, usize);

impl BlockHandle {
    fn start(&self) -> usize {
        self.0
    }

    fn size(&self) -> usize {
        self.1
    }

    fn end(&self) -> usize {
        self.0 + self.1
    }
}

impl BinarySerialize for BlockHandle {
    fn binary_serialize<W: Write>(&self, writer: &mut W) -> GyResult<()> {
        VUInt(self.0 as u64).binary_serialize(writer)?;
        VUInt(self.1 as u64).binary_serialize(writer)?;
        Ok(())
    }

    fn binary_deserialize<R: Read>(reader: &mut R) -> GyResult<Self> {
        let u1 = VUInt::binary_deserialize(reader)?.0.val();
        let u2 = VUInt::binary_deserialize(reader)?.0.val();
        Ok(BlockHandle(u1 as usize, u2 as usize))
    }
}

impl DiskStoreWriter {
    fn new(fname: &Path) -> GyResult<DiskStoreWriter> {
        let file = FileManager::open_file(fname, true, true)?;
        let w = DiskStoreWriter {
            offset: 0,
            filter_block: FilterBlock::new(),
            // index_block: IndexBlock::new(),
            field_bhs: Vec::new(),
            doc_meta_bh: BlockHandle::default(),
            field_meta_bh: BlockHandle::default(),
            vector_meta_bh: BlockHandle::default(),
            file: file,
            fname: fname.to_path_buf(),
            doc_end: 0,
        };
        Ok(w)
    }

    fn with_offset(fname: &Path, offset: usize, length: usize) -> GyResult<DiskStoreWriter> {
        let mut w = DiskStoreWriter::new(fname)?;
        w.seek(offset as u64)?;
        w.offset = offset;
        w.doc_end = offset;
        Ok(w)
    }

    fn seek(&mut self, offset: u64) -> GyResult<()> {
        self.file.seek(SeekFrom::Start(offset))?;
        Ok(())
    }

    fn get_cursor(&mut self) -> GyResult<u64> {
        let cursor = self.file.seek(SeekFrom::Current(0))?;
        Ok(cursor)
    }

    // fn add_term(&mut self, key: &[u8], offset: usize) -> GyResult<()> {
    //     self.index_block.add(key, offset as u64)?;
    //     Ok(())
    // }

    fn write_vector<T: TensorSimilar + VectorSerialize + Clone>(
        &mut self,
        vector_index: &Ann<T>,
    ) -> GyResult<()> {
        let offset = self.offset;
        vector_index.vector_serialize(&mut self.file)?;
        self.flush()?;
        self.offset = self.get_cursor()? as usize;
        self.vector_meta_bh = BlockHandle(offset, self.offset - offset);
        Ok(())
    }

    // write document meta
    fn write_doc_meta(&mut self, meta: &[usize]) -> GyResult<()> {
        let offset = self.offset;
        meta.binary_serialize(self)?;
        self.flush()?;
        self.offset = self.get_cursor()? as usize;
        self.doc_meta_bh = BlockHandle(offset, self.offset - offset);
        Ok(())
    }

    fn write_field_meta(&mut self) -> GyResult<()> {
        let offset = self.offset;
        self.field_bhs.binary_serialize(&mut self.file)?;
        self.flush()?;
        self.offset = self.get_cursor()? as usize;
        self.field_meta_bh = BlockHandle(offset, self.offset - offset);
        Ok(())
    }

    fn write_bloom(&mut self, bloom: &GyBloom) -> GyResult<BlockHandle> {
        let offset = self.offset;
        bloom.binary_serialize(&mut self.file)?;
        self.flush()?;
        self.offset = self.get_cursor()? as usize;
        Ok(BlockHandle(offset, self.offset - offset))
    }

    fn write_fst(&mut self, v: &[u8]) -> GyResult<BlockHandle> {
        self.finish_index_block(v)
    }

    fn finish_field(
        &mut self,
        term_count: usize,
        bloom_bh: BlockHandle,
        fst_bh: BlockHandle,
    ) -> GyResult<()> {
        self.field_bhs.push(FieldHandle {
            term_count: term_count,
            fst_bh: fst_bh,
            bloom_bh: bloom_bh,
        });
        Ok(())
    }

    fn close(&mut self) -> GyResult<u64> {
        let mut footer = [0u8; FOOTER_LEN as usize];
        let mut c = std::io::Cursor::new(&mut footer[..]);
        self.doc_end.binary_serialize(&mut c)?;
        self.doc_meta_bh.binary_serialize(&mut c)?;
        self.field_meta_bh.binary_serialize(&mut c)?;
        self.vector_meta_bh.binary_serialize(&mut c)?;
        iocopy!(&mut footer[FOOTER_LEN as usize - MAGIC.len()..], MAGIC);
        self.write(&footer)?;
        self.flush()?;
        //self.truncate()?;
        Ok(self.get_cursor()?)
    }

    fn truncate(&mut self) -> GyResult<()> {
        let cursor = self.get_cursor()?;
        self.file.set_len(cursor)?;
        Ok(())
    }

    fn finish_index_block(&mut self, v: &[u8]) -> GyResult<BlockHandle> {
        let length = v.len();
        let offset = self.offset;
        self.file.write(v)?;
        self.flush()?;
        self.offset = offset + length;
        Ok(BlockHandle(offset, length))
    }

    fn write_posting(&mut self, doc_size: usize, b: &[u8]) -> GyResult<usize> {
        let offset = self.offset;
        VUInt(doc_size as u64).binary_serialize(&mut self.file)?;
        VUInt(b.len() as u64).binary_serialize(&mut self.file)?;
        self.file.write(b)?;
        self.flush()?;
        self.offset = self.get_cursor()? as usize;
        Ok(offset)
    }

    fn write_doc_block(&mut self, doc_content: &[u8]) -> GyResult<()> {
        let chunk_size = 4 * 1024;
        let mut offset = 0;
        while offset < doc_content.len() {
            let end = std::cmp::min(offset + chunk_size, doc_content.len());
            self.file.write_all(&doc_content[offset..end])?;
            offset = end;
        }
        self.offset += offset;
        Ok(())
    }
}

impl Write for DiskStoreWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.file.write(buf)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.file.flush()
    }
}
#[derive(Default)]
struct FilterBlock {}

impl FilterBlock {
    fn new() -> FilterBlock {
        FilterBlock {}
    }
}

struct IndexBlock {
    fst: FstBuilder<Vec<u8>>,
}

impl IndexBlock {
    fn new(w: Vec<u8>) -> IndexBlock {
        IndexBlock {
            fst: FstBuilder::new(w),
        }
    }

    fn add(&mut self, key: &[u8], val: u64) -> GyResult<()> {
        self.fst.add(key, val)
    }

    fn finish(self) -> GyResult<Vec<u8>> {
        self.fst.finish()
    }

    fn reset(&mut self) -> GyResult<()> {
        self.fst.reset()
    }
}

#[inline]
fn shared_prefix_len(a: &[u8], b: &[u8]) -> usize {
    let (mut i, mut n) = (0 as usize, a.len());
    if n > b.len() {
        n = b.len()
    }
    while i < n && a[i] == b[i] {
        i += 1;
    }
    return i;
}

pub trait GyRead {
    fn read_bytes(&mut self, n: usize) -> GyResult<&[u8]>;

    // // 返回当前 offset
    fn offset(&self) -> usize;

    fn cursor(&self) -> &[u8];
}

pub trait GyWrite {
    // 返回当前 offset
    fn get_pos(&mut self) -> GyResult<usize>;
}

pub struct MmapReader<'a> {
    mmap: &'a Mmap,
    offset: usize,
    file_size: usize,
}

impl<'a> GyRead for MmapReader<'a> {
    fn read_bytes(&mut self, n: usize) -> GyResult<&[u8]> {
        if self.offset + n > self.file_size {
            return Err(GyError::EOF);
        }
        let v = &self.mmap[self.offset..self.offset + n];
        self.offset += n;
        Ok(v)
    }

    fn cursor(&self) -> &[u8] {
        &self.mmap[self.offset..]
    }

    fn offset(&self) -> usize {
        self.offset
    }
}

impl<'a> MmapReader<'a> {
    pub fn new(mmap: &'a Mmap, offset: usize, file_size: usize) -> MmapReader {
        Self {
            mmap: mmap,
            offset: offset,
            file_size: file_size,
        }
    }
}

impl<'a> Read for MmapReader<'a> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let buf_len = buf.len();
        if self.offset + buf_len > self.file_size {
            return Err(std::io::ErrorKind::UnexpectedEof.into());
        }
        buf.copy_from_slice(&self.mmap[self.offset..self.offset + buf_len]);
        self.offset += buf_len;
        Ok(buf_len)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use byteorder::WriteBytesExt;
    use std::io::{BufWriter, Write};
    use varintrs::{Binary, WriteBytesVarExt};
    #[test]
    fn test_bytes() {
        let mut buffer: Vec<u8> = Vec::with_capacity(10);
        buffer.write_vi64::<Binary>(1024);
        buffer.write_vi64::<Binary>(1024);
        buffer.write_vi64::<Binary>(1024);
        buffer.write_vi64::<Binary>(1024);
        buffer.write_vi64::<Binary>(1024);
        buffer.write_vi64::<Binary>(1024);
        buffer.write_vi64::<Binary>(1024);
        buffer.write_vi64::<Binary>(1024);
        buffer.write_vi64::<Binary>(1024);
        buffer.write_vi64::<Binary>(1024);
        buffer.write_vi64::<Binary>(1024);
        buffer.write_vi64::<Binary>(1024);
        buffer.write_vi64::<Binary>(1024);
        //buffer.write_vi64::<Binary>(1024);
        println!("buf1:{:?}", buffer);
        buffer.clear();

        buffer.write_vi64::<Binary>(2048);
        buffer.write_vi64::<Binary>(2048);

        println!("buf2:{:?}", buffer);
        // buffer.put_f32(n)
    }

    #[test]
    fn test_disk_reader() {
        let mut buffer: Vec<u8> = Vec::with_capacity(10);
        buffer.write_vi64::<Binary>(1024);
        buffer.write_vi64::<Binary>(1024);
        buffer.write_vi64::<Binary>(1024);
        buffer.write_vi64::<Binary>(1024);
        buffer.write_vi64::<Binary>(1024);
        buffer.write_vi64::<Binary>(1024);
        buffer.write_vi64::<Binary>(1024);
        buffer.write_vi64::<Binary>(1024);
        buffer.write_vi64::<Binary>(1024);
        buffer.write_vi64::<Binary>(1024);
        buffer.write_vi64::<Binary>(1024);
        buffer.write_vi64::<Binary>(1024);
        buffer.write_vi64::<Binary>(1024);
        //buffer.write_vi64::<Binary>(1024);
        println!("buf1:{:?}", buffer);
        buffer.clear();

        buffer.write_vi64::<Binary>(2048);
        buffer.write_vi64::<Binary>(2048);

        println!("buf2:{:?}", buffer);
        // buffer.put_f32(n)
    }
    use std::io::Cursor;
    #[test]
    fn test_cursor() {
        let mut v: Vec<u8> = vec![];
        let mut c = Cursor::new(v);
        c.write_u8(1u8);
        c.write_u8(2u8);
        println!("{:?}", c.get_ref());

        c.get_mut().clear();
        c.set_position(0);
        c.write_u8(3u8);
        println!("{:?}", c.get_ref());
    }

    fn test_buf_writer() {
        let mut v: Vec<u8> = vec![];
        let mut c = Cursor::new(v);
        c.write_u8(1u8);
        c.write_u8(2u8);
        println!("{:?}", c.get_ref());
        c.write_u8(3u8);
        println!("{:?}", c.get_ref());
    }

    #[test]
    fn test_CompactionMergeMuch() {
        let iters = vec![
            vec![1, 4, 7, 11].into_iter().peekable(),
            vec![1, 5, 8, 10].into_iter().peekable(),
            vec![1, 5, 9].into_iter().peekable(),
            vec![5].into_iter().peekable(),
        ];

        let merger = CompactionMergeMuch::new(iters);
        for merged in merger.merge() {
            println!("{:?}", merged);
        }
    }
}
