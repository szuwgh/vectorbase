use super::util::error::GyResult;
use super::IndexReader;
use art_tree::{Art, ByteString, Key};
use std::collections::HashMap;
use std::fs::{self, File};
use std::path::PathBuf;
use std::{
    fs::OpenOptions,
    io::{Seek, SeekFrom, Write},
};
use varintrs::{Binary, WriteBytesVarExt};
const DEFAULT_BLOCK_SIZE: usize = 4 * 1024 * 1024 + 1024;

//压缩类型
enum CompressionType {
    No,
    LZ4,
    LowercaseAscii,
    Zstandard,
    Snappy,
}

//合并索引
pub fn merge_index(
    reader: IndexReader,
    fname: PathBuf,
    offset: usize,
) -> GyResult<DiskStoreReader> {
    let mut writer = DiskStoreWriter::from(fname, offset)?;
    let mut field_cache: Vec<HashMap<Vec<u8>, usize>> =
        Vec::with_capacity(reader.reader.fields.len());
    for field in reader.iter() {
        let mut term_offset_cache: HashMap<Vec<u8>, usize> = HashMap::new();
        for (b, p) in field.indexs.read()?.iter() {
            let (start_addr, end_addr) = (
                (*p).borrow().byte_addr.clone(),
                (*p).borrow().doc_freq_addr.clone(),
            );
            let posting_buffer = field.posting_buffer(start_addr, end_addr)?;
            //写入倒排表
            for rb in posting_buffer.iter() {
                writer.write(rb)?;
            }
            let offset = writer.flush_data_block()?;
            term_offset_cache.insert(b.to_bytes(), offset);
            //写入词典
        }
        field_cache.push(term_offset_cache)
    }
    todo!()
}

pub struct DiskStoreReader {}

impl DiskStoreReader {
    fn open(name: PathBuf) -> GyResult<DiskStoreReader> {
        todo!()
    }
}

pub struct DiskStoreWriter {
    offset: usize,
    //filter_block: FilterBlock, //布隆过滤器s
    //index_block: IndexBlock, //fst
    data_block: DataBlock,
    file: File,
}

impl DiskStoreWriter {
    fn new(name: PathBuf) {}

    fn from(fname: PathBuf, offset: usize) -> GyResult<DiskStoreWriter> {
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(fname)?;
        //  file.seek(pos);
        todo!();
        //Self {}
    }

    fn append_key(&mut self, key: &[u8]) -> GyResult<()> {
        Ok(())
    }

    fn write(&mut self, b: &[u8]) -> GyResult<()> {
        self.data_block.write(b)?;
        Ok(())
    }

    fn flush_data_block(&mut self) -> GyResult<usize> {
        let (a, b) = self.data_block.get()?;
        self.file.write(a)?;
        self.file.write(b)?;
        self.file.flush()?;
        let offset = self.offset;
        self.offset += a.len() + b.len();
        Ok(offset)
    }

    fn add_posting() {}
}

struct FilterBlock {}

impl FilterBlock {}

struct IndexBlock {}

struct DataBlock {
    buf1: Vec<u8>,
    buf2: Vec<u8>,
}

impl DataBlock {
    fn new() -> DataBlock {
        Self {
            buf1: Vec::with_capacity(DEFAULT_BLOCK_SIZE),
            buf2: Vec::with_capacity(DEFAULT_BLOCK_SIZE),
        }
    }

    fn get(&mut self) -> GyResult<(&[u8], &[u8])> {
        self.buf1.write_vu64::<Binary>(self.buf2.len() as u64)?;
        Ok((self.buf1.as_slice(), self.buf2.as_slice()))
    }

    fn write(&mut self, key: &[u8]) -> GyResult<()> {
        self.buf2.write(key)?;
        Ok(())
    }

    fn append(&mut self, key: &[u8], value: &[u8]) {}
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
