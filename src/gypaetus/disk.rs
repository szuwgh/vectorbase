use super::schema::{DocID, Document};
use super::util::error::GyResult;
use super::util::fs;
use super::util::fst::{FstBuilder, FstReader};
use super::IndexReader;
use art_tree::{Art, ByteString, Key};
use std::collections::HashMap;
use std::fs::File;
use std::path::PathBuf;
use std::{
    fs::OpenOptions,
    io::{Seek, SeekFrom, Write},
};
use varintrs::{Binary, WriteBytesVarExt};
const KB: usize = 1 << 10;
const MB: usize = KB * KB;
const DEFAULT_BLOCK_SIZE: usize = 4 * KB + KB;

//压缩类型
enum CompressionType {
    No,
    LZ4,
    LowercaseAscii,
    Zstandard,
    Snappy,
}

//合并索引
pub fn merge(reader: IndexReader, fname: PathBuf, offset: usize) -> GyResult<DiskStoreReader> {
    let mut writer = DiskStoreWriter::from(fname, offset)?;
    let mut buf = Vec::with_capacity(4 * KB);
    //写入文档位置信息
    let doc_offset = offset;
    let doc_offset_len = reader.reader.doc_offset.read()?.len();

    //写入每个域的信息
    for field in reader.iter() {
        let mut term_offset_cache: HashMap<Vec<u8>, usize> = HashMap::new();
        for (b, p) in field.indexs.read()?.iter() {
            let (start_addr, end_addr) = (
                (*p).borrow().byte_addr.clone(),
                (*p).borrow().doc_freq_addr.clone(),
            );
            let posting_buffer = field.posting_buffer(start_addr, end_addr)?;
            //写入倒排表
            buf.clear();
            for rb in posting_buffer.iter() {
                buf.write(rb)?;
            }
            let offset = writer.write_posting(buf.as_slice())?;
            term_offset_cache.insert(b.to_bytes(), offset);
        }
        let mut term_vec: Vec<(&Vec<u8>, &usize)> = term_offset_cache.iter().collect();
        term_vec.sort_by(|a, b| b.1.cmp(a.1));
        // 写入字典索引
        for (t, u) in term_vec {
            writer.add_term(t, *u)?;
        }
        writer.finish()?;
    }
}

pub struct DiskStoreReader {}

impl DiskStoreReader {
    fn open(name: PathBuf) -> GyResult<DiskStoreReader> {
        todo!()
    }

    pub fn field_reader(&self, field_id: u32) -> GyResult<DiskFieldReader> {
        todo!()
    }

    // pub(crate) fn doc(&self, doc_id: DocID) -> GyResult<Document> {
    //     let doc_offset = self.reader.doc_offset(doc_id)?;
    //     let doc: Document = {
    //         let mut wal = self.wal.read()?;
    //         let mut wal_read = WalReader::from(&mut wal, doc_offset);
    //         Document::deserialize(&mut wal_read)?
    //     };
    //     Ok(doc)
    // }
}

pub struct DiskFieldReader {}

pub struct DiskStoreWriter {
    offset: usize,
    filter_block: FilterBlock, //布隆过滤器s
    index_block: IndexBlock,   //fst
    // data_block: DataBlock,
    file: File,
}

impl DiskStoreWriter {
    fn new(name: PathBuf) {}

    fn from(fname: PathBuf, offset: usize) -> GyResult<DiskStoreWriter> {
        let file = fs::open_file(fname, true, true)?;
        //  file.seek(pos);
        todo!();
        //Self {}
    }

    fn add_term(&mut self, key: &[u8], offset: usize) -> GyResult<()> {
        self.index_block.add(key, offset as u64)?;
        Ok(())
    }

    fn finish(&mut self) -> GyResult<()> {
        self.index_block.finish()?;
        Ok(())
    }

    fn write_posting(&mut self, b: &[u8]) -> GyResult<usize> {
        let offset = self.offset;
        let i = self.file.write_vu64::<Binary>(b.len() as u64)?;
        self.file.write(b)?;
        self.offset += i + b.len();
        Ok(offset)
    }
}

struct FilterBlock {}

impl FilterBlock {}

struct IndexBlock {
    fst: FstBuilder,
}

struct IndexBlockReader {}

impl IndexBlock {
    fn add(&mut self, key: &[u8], val: u64) -> GyResult<()> {
        self.fst.add(key, val)
    }

    fn finish(&mut self) -> GyResult<()> {
        self.fst.finish()
    }
}

struct DataBlock {
    buf: Vec<u8>,
    restarts: Vec<u32>,
    restart_interval: u32,
    entries: u32,
    prev_key: Vec<u8>,
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
            restarts: Vec::new(),
            restart_interval: 2,
            entries: 0,
            prev_key: Vec::new(),
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

    fn add_term(&mut self, key: &[u8], offset: usize) -> GyResult<()> {
        let share_len = if self.entries % self.restart_interval == 0 {
            self.restarts.push(self.buf.len() as u32);
            0
        } else {
            shared_prefix_len(&self.prev_key, key)
        };
        self.buf.write_vu64::<Binary>(share_len as u64)?;
        self.buf.write_vu64::<Binary>(share_len as u64)?;
        self.buf.write(&key[share_len..])?;
        self.buf.write_vu64::<Binary>(offset as u64)?;
        self.prev_key.resize(share_len, 0);
        self.prev_key.write(&key[share_len..])?;
        self.entries += 1;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::io::Write;
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
}
