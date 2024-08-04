use super::schema::{BinarySerialize, FieldID, Schema};
use super::schema::{DocID, Document};
use super::util::error::{GyError, GyResult};
use super::util::fs::{self, from_json_file};
use super::util::fst::{FstBuilder, FstReader, FstReaderIter};
use super::{IndexReader, Meta, DATA_FILE, META_FILE};
use crate::gypaetus::schema::VUInt;
use crate::gypaetus::schema::VarIntSerialize;
use crate::gypaetus::DocFreq;
use crate::gypaetus::FieldEntry;
use crate::gypaetus::Term;
use crate::iocopy;
use art_tree::Key;
use bytes::Buf;
use furze::fst::Cow;
use memmap2::Mmap;
use std::borrow::Borrow;
use std::collections::HashMap;
use std::fs::File;
use std::io::Read;
use std::os::unix::fs::{FileExt, MetadataExt};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::{
    fs::OpenOptions,
    io::{Seek, SeekFrom, Write},
};
const KB: usize = 1 << 10;
const MB: usize = KB * KB;
const DEFAULT_BLOCK_SIZE: usize = 4 * KB + KB;
const FOOTER_LEN: u64 = 48;

const MAGIC: &'static [u8] = b"\xD4\x56\x3F\x35\xE0\xEF\x09\x7A";

//压缩类型
enum CompressionType {
    No,
    LZ4,
    LowercaseAscii,
    Zstandard,
    Snappy,
}

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

mod Writer {}

mod Reader {}

use std::cmp::Ordering;
use std::iter::Peekable;

struct CompactionMerger<T: Iterator> {
    a: Peekable<T>,
    b: Peekable<T>,
}

impl<T: Iterator> CompactionMerger<T>
where
    T::Item: Ord,
{
    fn new(a: Peekable<T>, b: Peekable<T>) -> CompactionMerger<T> {
        Self { a, b }
    }

    fn merge(mut self) -> impl Iterator<Item = (Option<T::Item>, Option<T::Item>)>
    where
        Self: Sized,
    {
        std::iter::from_fn(move || match (self.a.peek(), self.b.peek()) {
            (Some(v1), Some(v2)) => match v1.cmp(v2) {
                Ordering::Less => Some((self.a.next(), None)),
                Ordering::Greater => Some((None, self.b.next())),
                Ordering::Equal => Some((self.a.next(), self.b.next())),
            },
            (Some(_), None) => Some((self.a.next(), None)),
            (None, Some(_)) => Some((None, self.b.next())),
            (None, None) => None,
        })
    }
}

struct TmpBufWriter(Vec<u8>);

impl TmpBufWriter {
    fn new(size: usize) -> TmpBufWriter {
        TmpBufWriter(Vec::with_capacity(size))
    }

    fn clear(&mut self) {
        self.0.clear();
    }
}

impl Write for TmpBufWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.write(buf)
    }
    fn flush(&mut self) -> std::io::Result<()> {
        self.0.flush()
    }
}

pub fn merge(a: &DiskStoreReader, b: &DiskStoreReader, new_fname: &Path) -> GyResult<()> {
    // let mut writer = DiskStoreWriter::new(new_fname)?;
    //  let doc_meta: Vec<usize> = Vec::with_capacity(a.doc_size() + b.doc_size());
    let reader_merger = CompactionMerger::new(a.iter().peekable(), b.iter().peekable());
    reader_merger.merge().for_each(|e| match (e.0, e.1) {
        (Some(a), Some(b)) => {
            // let field_merger = CompactionMerger::new(a.iter().peekable(), b.iter().peekable());
        }
        (None, Some(b)) => println!("a:{},b{}", "none", b.get_field_name()),
        (Some(a), None) => {
            println!("a:{},b{}", a.get_field_name(), "none")
        }
        (None, None) => {
            println!("none")
        }
    });
    // writer.write_posting(b);
    // let mut buf = Vec::with_capacity(4 * KB);
    // 定义缓冲区大小为 4KB

    Ok(())
}

//合并索引
pub fn flush_index(reader: &IndexReader) -> GyResult<()> {
    let fname = &reader.get_index_config().get_wal_path();
    let mut writer = DiskStoreWriter::with_offset(
        fname,
        reader.offset()?,
        reader.get_index_base().doc_offset.read()?.len(),
    )?;
    let mut buf = Vec::with_capacity(4 * KB);
    // write field
    for field in reader.iter() {
        let mut term_offset_cache: HashMap<Vec<u8>, usize> = HashMap::new();
        for (b, p) in field.indexs.read()?.iter() {
            let (doc_num, start_addr, end_addr) = {
                let posting = (*p).read()?;
                (
                    posting.doc_num,
                    posting.byte_addr.clone(),
                    posting.doc_freq_addr.clone(),
                )
            };
            // write posting
            let posting_buffer = field.posting_buffer(start_addr, end_addr)?;
            for rb in posting_buffer.iter() {
                buf.write(rb)?;
            }
            let offset = writer.write_posting(doc_num, buf.as_slice())?;
            term_offset_cache.insert(b.to_bytes(), offset);
            buf.clear();
        }
        // write bloom
        // write fst
        for (b, _) in field.indexs.read()?.iter() {
            let v: &[u8] = b.borrow();
            let offset = term_offset_cache.get(v).unwrap();
            writer.add_term(v, *offset)?;
        }
        writer.finish_field()?;
    }
    // 写入文档和偏移量关系 meta
    writer.write_doc_meta(&reader.get_doc_offset().read()?)?;
    // 写入每个域的 meta
    writer.write_field_meta()?;
    writer.close()?;
    drop(writer);
    if let Some(dir_path) = fname.parent() {
        std::fs::rename(fname, dir_path.join(DATA_FILE))?;
        crate::gypaetus::fs::to_json_file(
            reader.get_index_base().get_meta(),
            dir_path.join(META_FILE),
        )?;
    }
    Ok(())
}

pub struct DiskStoreReader {
    meta: Meta,
    fields_meta: Vec<FieldHandle>,
    doc_meta: Vec<usize>,
    doc_end: usize,
    file: File,
    mmap: Arc<Mmap>,
}

impl DiskStoreReader {
    pub fn open(index_path: PathBuf) -> GyResult<DiskStoreReader> {
        let data_path = index_path.join(DATA_FILE);
        let meta_path = index_path.join(META_FILE);
        let file = OpenOptions::new().read(true).open(data_path)?;
        let file_size = file.metadata()?.size();
        if file_size < FOOTER_LEN {}
        let footer_pos = file_size - FOOTER_LEN;
        let mut footer = [0u8; FOOTER_LEN as usize];
        file.read_at(&mut footer, footer_pos)?;
        //判断魔数是否正确
        if &footer[FOOTER_LEN as usize - MAGIC.len()..FOOTER_LEN as usize] != MAGIC {
            return Err(GyError::ErrBadMagicNumber);
        }
        let mut c = std::io::Cursor::new(&footer[..]);
        let doc_meta_bh: BlockHandle = BlockHandle::binary_deserialize(&mut c)?;
        let field_meta_bh = BlockHandle::binary_deserialize(&mut c)?;

        let mmap: Mmap = unsafe {
            memmap2::MmapOptions::new()
                .map(&file)
                .map_err(|e| format!("mmap failed: {}", e))?
        };
        let fields_meta = Self::read_at_bh::<Vec<FieldHandle>>(&mmap, field_meta_bh)?;
        let doc_meta = Self::read_at_bh::<Vec<usize>>(&mmap, doc_meta_bh)?;
        let meta: Meta = from_json_file(&meta_path)?;
        assert!(fields_meta.len() == meta.get_fields().len());
        Ok(Self {
            meta: meta,
            fields_meta: fields_meta,
            doc_meta: doc_meta,
            doc_end: 0,
            file: file,
            mmap: Arc::new(mmap),
        })
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

    pub fn field_reader<'a>(&'a self, field_id: u32) -> GyResult<DiskFieldReader<'a>> {
        let field_id = field_id as usize;
        assert!(field_id < self.fields_meta.len());
        assert!(field_id < self.meta.schema.fields.len());
        let field_handle = &self.fields_meta[field_id as usize];
        let field_entry = self.meta.schema.fields[field_id as usize].clone();
        self.get_field_reader(field_entry, field_handle)
    }

    fn get_field_reader<'a>(
        &'a self,
        field_entry: FieldEntry,
        field_handle: &FieldHandle,
    ) -> GyResult<DiskFieldReader<'a>> {
        // let field_handle = &self.fields_meta[field_id as usize];
        // println!(
        //     "get fst:{:?}",
        //     &self.mmap[field_handle.fst_bh.start()..field_handle.fst_bh.end()]
        // );
        let fst =
            FstReader::load(&self.mmap[field_handle.fst_bh.start()..field_handle.fst_bh.end()]);
        Ok(DiskFieldReader {
            fst: fst,
            mmap: self.mmap.clone(),
            field_entry: field_entry,
        })
    }

    pub fn search(&self, term: Term) -> GyResult<DiskPostingReader> {
        let field_id = term.field_id().0;
        let field_reader = self.field_reader(field_id)?;
        field_reader.find(term.bytes_value())
    }

    pub fn doc_size(&self) -> usize {
        self.doc_meta.len()
    }

    pub(crate) fn doc_block(&self) -> &[u8] {
        &self.mmap[0..self.doc_end]
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

pub struct DiskFieldReader<'a> {
    fst: FstReader<'a>,
    mmap: Arc<Mmap>,
    field_entry: FieldEntry,
}

impl<'a> DiskFieldReader<'a> {
    pub fn find(&self, term: &[u8]) -> GyResult<DiskPostingReader> {
        let offset = self.fst.get(term)?;
        self.get(offset as usize)
    }

    pub fn iter(&self) -> DiskFieldReaderIter {
        DiskFieldReaderIter {
            iter: self.fst.iter(),
            mmap: self.mmap.clone(),
        }
    }

    pub fn get_field_name(&self) -> &str {
        &self.field_entry.get_name()
    }

    pub fn get_field_id(&self) -> &FieldID {
        &self.field_entry.get_field_id()
    }

    fn get(&self, offset: usize) -> GyResult<DiskPostingReader> {
        Ok(DiskPostingReader::new(self.mmap.clone(), offset)?)
    }
}

pub struct DiskFieldReaderIter<'a> {
    iter: FstReaderIter<'a>,
    mmap: Arc<Mmap>,
}

impl<'a> Iterator for DiskFieldReaderIter<'a> {
    type Item = (Cow, DiskPostingReader);
    fn next(&mut self) -> Option<Self::Item> {
        let item = self.iter.next()?;
        Some((
            item.0.clone(),
            DiskPostingReader::new(self.mmap.clone(), item.1 as usize).unwrap(),
        ))
    }
}

pub struct DiskPostingWriter<'a, T: Write> {
    last_docid: DocID,
    w: &'a mut T,
}

impl<'a, T: Write> DiskPostingWriter<'a, T> {
    fn add(&mut self, doc_id: DocID, freq: u32) -> GyResult<()> {
        DocFreq(doc_id - self.last_docid, freq).binary_serialize(&mut self.w)?;
        self.last_docid = doc_id;
        Ok(())
    }
}

pub struct DiskPostingReader {
    doc_num: usize,
    snapshot: DiskSnapshotReader,
}

impl DiskPostingReader {
    fn new(mmap: Arc<Mmap>, offset: usize) -> GyResult<DiskPostingReader> {
        let mut r = mmap[offset..].reader();
        let (doc_num, i) = VUInt::binary_deserialize(&mut r)?;
        Ok(DiskPostingReader {
            doc_num: doc_num.val() as usize,
            snapshot: DiskSnapshotReader::new(mmap, offset + i)?,
        })
    }

    pub fn get_doc_size(&self) -> usize {
        self.doc_num
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
        println!(
            "length:{},p:{:?},offset:{}",
            length.val(),
            &mmap[offset..offset + l],
            offset
        );
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
    index_block: IndexBlock,   //fst
    field_bhs: Vec<FieldHandle>,
    doc_meta_bh: BlockHandle,
    field_meta_bh: BlockHandle,
    vector_meta_bh: BlockHandle,
    file: File,
    fname: PathBuf,
}

#[derive(Debug)]
struct FieldHandle {
    fst_bh: BlockHandle,
    filter_bh: BlockHandle,
}

impl BinarySerialize for FieldHandle {
    fn binary_serialize<W: Write>(&self, writer: &mut W) -> GyResult<()> {
        self.fst_bh.binary_serialize(writer)?;
        self.filter_bh.binary_serialize(writer)
    }

    fn binary_deserialize<R: Read>(reader: &mut R) -> GyResult<Self> {
        let fst_bh = BlockHandle::binary_deserialize(reader)?;
        let filter_bh = BlockHandle::binary_deserialize(reader)?;
        Ok(FieldHandle { fst_bh, filter_bh })
    }
}

#[derive(Default, Debug)]
struct BlockHandle(usize, usize);

impl BlockHandle {
    fn start(&self) -> usize {
        self.0
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
        let file = fs::open_file(fname, true, true)?;
        let w = DiskStoreWriter {
            offset: 0,
            filter_block: FilterBlock::new(),
            index_block: IndexBlock::new(),
            field_bhs: Vec::new(),
            doc_meta_bh: BlockHandle::default(),
            field_meta_bh: BlockHandle::default(),
            vector_meta_bh: BlockHandle::default(),
            file: file,
            fname: fname.to_path_buf(),
        };
        Ok(w)
    }

    fn with_offset(fname: &Path, offset: usize, length: usize) -> GyResult<DiskStoreWriter> {
        let mut w = DiskStoreWriter::new(fname)?;
        w.seek(offset as u64)?;
        w.offset = offset;
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

    fn add_term(&mut self, key: &[u8], offset: usize) -> GyResult<()> {
        self.index_block.add(key, offset as u64)?;
        Ok(())
    }

    // write document meta
    fn write_doc_meta(&mut self, meta: &[usize]) -> GyResult<()> {
        let offset = self.offset;
        println!("doc_meta:{}", offset);
        meta.binary_serialize(self)?;
        self.flush()?;
        self.offset = self.get_cursor()? as usize;
        println!("doc_meta cursor:{}", self.offset);
        self.doc_meta_bh = BlockHandle(offset, self.offset - offset);
        Ok(())
    }

    fn write_field_meta(&mut self) -> GyResult<()> {
        let offset = self.offset;
        println!("field_meta:{}", offset);
        self.field_bhs.binary_serialize(&mut self.file)?;
        self.flush()?;
        self.offset = self.get_cursor()? as usize;
        println!("field_meta cursor:{}", self.offset);
        self.field_meta_bh = BlockHandle(offset, self.offset - offset);
        Ok(())
    }

    fn finish_field(&mut self) -> GyResult<()> {
        let bh = self.finish_index_block()?;
        self.field_bhs.push(FieldHandle {
            fst_bh: bh,
            filter_bh: BlockHandle::default(),
        });
        Ok(())
    }

    fn close(&mut self) -> GyResult<()> {
        let mut footer = [0u8; FOOTER_LEN as usize];
        let mut c = std::io::Cursor::new(&mut footer[..]);
        self.doc_meta_bh.binary_serialize(&mut c)?;
        self.field_meta_bh.binary_serialize(&mut c)?;
        iocopy!(&mut footer[FOOTER_LEN as usize - MAGIC.len()..], MAGIC);
        self.write(&footer)?;
        self.flush()?;
        self.truncate()?;
        Ok(())
    }

    fn truncate(&mut self) -> GyResult<()> {
        let cursor = self.get_cursor()?;
        self.file.set_len(cursor)?;
        Ok(())
    }

    fn finish_index_block(&mut self) -> GyResult<BlockHandle> {
        let length = self.index_block.finish()?;
        let offset = self.offset;
        self.index_block.binary_serialize(&mut self.file)?;
        self.index_block.reset()?;
        self.flush()?;
        self.offset = offset + length;
        Ok(BlockHandle(offset, length))
    }

    fn write_posting(&mut self, doc_num: usize, b: &[u8]) -> GyResult<usize> {
        let offset = self.offset;
        VUInt(doc_num as u64).binary_serialize(&mut self.file)?;
        VUInt(b.len() as u64).binary_serialize(&mut self.file)?;
        println!("len:{},u8:{:?},offset:{}", b.len(), b, offset);
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
    fst: FstBuilder,
}

impl IndexBlock {
    fn new() -> IndexBlock {
        IndexBlock {
            fst: FstBuilder::new(),
        }
    }

    fn add(&mut self, key: &[u8], val: u64) -> GyResult<()> {
        self.fst.add(key, val)
    }

    fn finish(&mut self) -> GyResult<usize> {
        self.fst.finish()?;
        Ok(self.fst.get_ref().len())
    }

    fn reset(&mut self) -> GyResult<()> {
        self.fst.reset()
    }
}

impl BinarySerialize for IndexBlock {
    fn binary_serialize<W: Write>(&self, writer: &mut W) -> GyResult<()> {
        writer.write(self.fst.get_ref())?;
        writer.flush()?;
        Ok(())
    }

    fn binary_deserialize<R: Read>(reader: &mut R) -> GyResult<Self> {
        todo!()
    }
}

struct IndexBlockReader {}

struct DataBlock {
    buf: Vec<u8>,
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

impl DataBlock {
    fn new() -> DataBlock {
        Self {
            buf: Vec::with_capacity(DEFAULT_BLOCK_SIZE),
        }
    }

    fn reset(&mut self) {
        self.buf.clear();
    }

    // fn get(&mut self) -> GyResult<(&[u8], &[u8])> {
    //     self.buf1.write_vu64::<Binary>(self.buf2.len() as u64)?;
    //     Ok((self.buf1.as_slice(), self.buf2.as_slice()))
    // }

    fn write(&mut self, key: &[u8]) -> GyResult<()> {
        self.buf.write(key)?;
        Ok(())
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
}
