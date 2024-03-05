use super::schema::BinarySerialize;
use super::schema::{DocID, Document};
use super::util::error::GyResult;
use super::util::fs;
use super::util::fst::{FstBuilder, FstReader};
use super::IndexReader;
use crate::gypaetus::Term;
use art_tree::{Art, ByteString, Key};
use memmap2::Mmap;
use serde::Serialize;
use std::collections::HashMap;
use std::fs::File;
use std::io::Read;
use std::path::PathBuf;
use std::sync::Arc;
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

//  +---------------------+
//  |                     |
//  |       Doc           |
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
//  |       vector        |
//  |                     |
//  +---------------------+
//  |                     |
//  |       meta          |
//  |                     |
//  +---------------------+
//  |                     |
//  |       footer        |
//  |                     |
//  +---------------------+

//合并索引
pub fn flush_disk(reader: IndexReader, fname: PathBuf, offset: usize) -> GyResult<DiskStoreWriter> {
    let mut writer = DiskStoreWriter::with_offset(fname, offset)?;
    let mut buf = Vec::with_capacity(4 * KB);
    //写入文档位置信息
    let doc_offset = offset;
    let doc_offset_len = reader.reader.doc_offset.read()?.len();

    //写入每个域的信息
    for field in reader.iter() {
        let mut term_offset_cache: HashMap<Vec<u8>, usize> = HashMap::new();
        for (b, p) in field.indexs.read()?.iter() {
            let (start_addr, end_addr) = (
                (*p).read()?.byte_addr.clone(),
                (*p).read()?.doc_freq_addr.clone(),
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
        //排序
        term_vec.sort_by(|a, b| b.1.cmp(a.1));
        // 写入字典索引
        for (t, u) in term_vec {
            writer.add_term(t, *u)?;
        }
        writer.finish_field()?;
    }
    //写入文件和偏移量关系 meta
    reader.get_doc_offset().read()?.bin_serialize(&mut writer)?;

    Ok(writer)
}

pub struct DiskStoreReader {
    fields: Vec<(usize, usize)>,
    file: File,
    mmap: Arc<Mmap>,
}

impl DiskStoreReader {
    fn open(fname: PathBuf) -> GyResult<DiskStoreReader> {
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(fname)?;
        let nmmap = unsafe {
            memmap2::MmapOptions::new()
                .offset(0)
                .map(&file)
                .map_err(|e| format!("mmap failed: {}", e))?
        };
        Ok(Self {
            fields: Vec::new(),
            file: file,
            mmap: Arc::new(nmmap),
        })
    }

    pub fn field_reader<'a>(&'a self, field_id: u32) -> GyResult<DiskFieldReader<'a>> {
        let (offset, length) = self.fields[field_id as usize];
        let fst = FstReader::load(&self.mmap[offset..offset + length]);
        Ok(DiskFieldReader { fst: fst })
    }

    fn search(&self, term: Term) -> GyResult<DiskPostingReader> {
        let field_id = term.field_id().0;
        let field_reader = self.field_reader(field_id)?;
        //  field_reader.get(term.bytes_value())
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

pub struct DiskFieldReader<'a> {
    fst: FstReader<'a>,
}

// impl<'a> DiskFieldReader<'a> {}

struct DiskPostingReader {
    // last_docid: DocID,
    //  snap_iter: DiskSnapshotReader,
}

// impl Iterator for DiskPostingReader {
//     type Item = (DocID, u32);
//     fn next(&mut self) -> Option<Self::Item> {}
// }

// pub struct DiskPostingReaderIter<'a> {
//     last_docid: DocID,
//     snap_iter: SnapshotReaderIter<'a>,
// }

// impl<'b, 'a> Iterator for PostingReaderIter<'a> {
//     type Item = (DocID, u32);
//     fn next(&mut self) -> Option<Self::Item> {
//         let doc_code = self.snap_iter.next()?;
//         self.last_docid += doc_code >> 1;
//         let freq = if doc_code & 1 > 0 {
//             1
//         } else {
//             self.snap_iter.next()? as u32
//         };
//         Some((self.last_docid, freq))
//     }
// }

struct DiskSnapshotReader {
    mmap: Arc<Mmap>,
    offset: usize,
    lenght: usize,
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
    field_offset: Vec<usize>,
    scratch: [u8; 50],
    file: File,
}

impl DiskStoreWriter {
    fn new(name: PathBuf) {}

    fn with_offset(fname: PathBuf, offset: usize) -> GyResult<DiskStoreWriter> {
        let mut file = fs::open_file(fname, true, true)?;
        file.seek(SeekFrom::Current(offset as i64))?;
        todo!();
        //Self {}
    }

    fn add_term(&mut self, key: &[u8], offset: usize) -> GyResult<()> {
        self.index_block.add(key, offset as u64)?;
        Ok(())
    }

    fn finish_field(&mut self) -> GyResult<()> {
        self.index_block.finish()?;
        self.index_block.bin_serialize(&mut self.file)?;
        self.index_block.fst.reset();
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

impl Write for DiskStoreWriter {
    fn write(&mut self, mut buf: &[u8]) -> std::io::Result<usize> {
        self.file.write(buf)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.file.flush()
    }
}

struct FilterBlock {}

impl FilterBlock {}

struct IndexBlock {
    fst: FstBuilder,
}

impl IndexBlock {
    fn add(&mut self, key: &[u8], val: u64) -> GyResult<()> {
        self.fst.add(key, val)
    }

    fn finish(&mut self) -> GyResult<()> {
        self.fst.finish()
    }

    fn reset(&mut self) {
        self.fst.reset()
    }
}

impl BinarySerialize for IndexBlock {
    fn bin_serialize<W: Write>(&self, writer: &mut W) -> GyResult<()> {
        // self.finish()?;
        writer.write(self.fst.get_ref())?;
        writer.flush()?;
        // self.fst.reset();
        Ok(())
    }

    fn debin_serialize<R: Read>(reader: &mut R) -> GyResult<Self> {
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
}
