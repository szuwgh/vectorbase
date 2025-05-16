use super::disk::GyWrite;
use super::util::error::{GyError, GyResult};
use super::util::fs::{FileIOSelector, IoSelector, MmapSelector};
use crate::disk::GyRead;
use crate::iocopy;
use crate::schema::{TensorEntry, VectorSerialize};
use crate::ValueSized;
use crate::Vector;
use core::arch::x86_64::*;
use core::cell::UnsafeCell;
use memmap2::{self, Mmap, MmapMut};
use rand::seq::IteratorRandom;
use serde::de::value;
use std::fs::{self, File};
use std::io::Read;
use std::io::Write;
use std::marker::PhantomData;
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock, Weak};

pub(crate) const DEFAULT_WAL_FILE_SIZE: usize = 2 * 1024 * 1024; //2MB

#[derive(Copy, Clone)]
pub enum IOType {
    FILEIO,
    MMAP,
}

const BLOCK_SIZE: usize = 1 << 15; //32KB

unsafe impl Send for Wal {}
unsafe impl Sync for Wal {}

pub struct WalIter<'a, V: VectorSerialize> {
    wal_reader: WalReader<'a>,
    tensor_entry: TensorEntry,
    _mark: PhantomData<V>,
}

impl<'a, V: VectorSerialize> WalIter<'a, V> {
    pub fn new(wal_reader: WalReader<'a>, tensor_entry: TensorEntry) -> WalIter<'a, V> {
        Self {
            wal_reader: wal_reader,
            tensor_entry: tensor_entry,
            _mark: PhantomData::default(),
        }
    }

    pub fn offset(&self) -> usize {
        self.wal_reader.offset()
    }

    fn fsize(&self) -> usize {
        self.wal_reader.wal.fsize
    }
}

impl<'a, V: VectorSerialize> Iterator for WalIter<'a, V> {
    type Item = (usize, V);
    fn next(&mut self) -> Option<Self::Item> {
        let offset = self.offset();
        // if (offset + 4 >= self.fsize()) {
        //     return None;
        // }
        match V::vector_nommap_deserialize(&mut self.wal_reader, &self.tensor_entry) {
            Ok(v) => Some((offset, v)),
            Err(_) => None,
        }
    }
}

pub struct WalReader<'a> {
    wal: &'a Wal,
    offset: usize,
    //end: usize,
}

impl<'a> Read for WalReader<'a> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let i = self
            .wal
            .io_selector
            .read(buf, self.offset)
            .map_err(|e| std::io::Error::from(std::io::ErrorKind::UnexpectedEof))?;
        self.offset += i;
        Ok(i)
    }
}

impl<'a> WalReader<'a> {
    pub(crate) fn new(wal: &'a Wal, offset: usize) -> WalReader<'a> {
        WalReader {
            wal: wal,
            offset: offset,
        }
    }
}

impl<'a> GyRead for WalReader<'a> {
    fn read_bytes(&mut self, n: usize) -> GyResult<&[u8]> {
        let v = self.wal.io_selector.read_bytes(self.offset, n)?;
        self.offset += n;
        Ok(v)
    }

    fn cursor(&self) -> &[u8] {
        todo!()
    }

    fn offset(&self) -> usize {
        self.offset
    }
}

pub struct ThreadWal {
    wal: UnsafeCell<Wal>,
    rw_lock: RwLock<()>,
}

impl ThreadWal {
    pub(crate) fn new(w: Wal) -> ThreadWal {
        Self {
            wal: UnsafeCell::new(w),
            rw_lock: RwLock::new(()),
        }
    }

    pub(crate) fn get_borrow(&self) -> &Wal {
        let _unused = self.rw_lock.read().unwrap();
        unsafe { &*self.wal.get() }
    }

    pub(crate) fn get_borrow_mut(&self) -> &mut Wal {
        let _unused = self.rw_lock.read().unwrap();
        unsafe { &mut *self.wal.get() }
    }

    pub(crate) fn reopen(&self, fsize: usize) -> GyResult<()> {
        let used = self.rw_lock.write()?;
        let w = unsafe { &mut *self.wal.get() };
        w.reopen(fsize)?;
        drop(used);
        Ok(())
    }
}

pub(crate) struct Wal {
    io_selector: Box<dyn IoSelector>,
    i: usize,
    j: usize,
    fsize: usize,
    buffer: [u8; BLOCK_SIZE],
    fname: PathBuf,
}

impl GyWrite for Wal {
    fn get_pos(&mut self) -> GyResult<usize> {
        Ok(self.i)
    }
}

impl Wal {
    pub(crate) fn new(fname: &Path, fsize: usize, io_type: &IOType) -> GyResult<Wal> {
        let io_selector: Box<dyn IoSelector> = match io_type {
            IOType::FILEIO => todo!(),
            IOType::MMAP => Box::new(MmapSelector::new(fname, fsize)?),
        };
        Ok(Self {
            io_selector: io_selector,
            i: 0,
            j: 0,
            fsize: fsize,
            buffer: [0u8; BLOCK_SIZE],
            fname: fname.to_path_buf(),
        })
    }

    pub(crate) fn reopen(&mut self, fsize: usize) -> GyResult<()> {
        self.io_selector.reopen(&self.fname, fsize)
    }

    pub(crate) fn rename(&mut self, new_fname: &Path) -> GyResult<()> {
        std::fs::rename(&self.fname, new_fname)?;
        self.fname = new_fname.to_path_buf();
        Ok(())
    }

    pub(crate) fn open(fname: &Path, fsize: usize, io_type: &IOType) -> GyResult<Wal> {
        let io_selector: Box<dyn IoSelector> = match io_type {
            IOType::FILEIO => todo!(),
            IOType::MMAP => Box::new(MmapSelector::open(fname, fsize)?),
        };
        Ok(Self {
            io_selector: io_selector,
            i: 0,
            j: 0,
            fsize: fsize,
            buffer: [0u8; BLOCK_SIZE],
            fname: fname.to_path_buf(),
        })
    }

    pub(crate) fn check_room(&self, t: usize) -> bool {
        if self.i + t > self.fsize {
            return false;
        }
        true
    }

    pub(crate) fn check_rotate<T: ValueSized>(&self, t: &T) -> bool {
        if self.i + t.bytes_size() > self.fsize {
            return false;
        }
        true
    }

    pub(crate) fn write_bytes(&mut self, content: &[u8]) -> GyResult<()> {
        self.write(content)?;
        Ok(())
    }

    pub(crate) fn offset(&self) -> usize {
        self.i
    }

    pub(crate) fn set_position(&mut self, pos: usize) {
        self.i = pos;
    }

    pub(crate) fn iter<'a, V: VectorSerialize>(&'a self, entry: TensorEntry) -> WalIter<'a, V> {
        WalIter::<V>::new(WalReader::new(self, 0), entry)
    }

    pub(crate) fn get_fname(&self) -> &Path {
        &self.fname
    }
}

impl Write for Wal {
    fn write(&mut self, mut buf: &[u8]) -> std::io::Result<usize> {
        let total = buf.len();
        while buf.len() > 0 {
            if self.j >= BLOCK_SIZE {
                self.flush()?;
            }
            let n = iocopy!(&mut self.buffer[self.j..], buf);
            self.j += n;
            buf = &buf[n..];
        }
        Ok(total)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.io_selector
            .write(&self.buffer[..self.j], self.i)
            .map_err(|e| std::io::Error::from(std::io::ErrorKind::InvalidData))?;
        self.io_selector
            .sync()
            .map_err(|e| std::io::Error::from(std::io::ErrorKind::InvalidData))?;
        self.i += self.j;
        self.j = 0;
        Ok(())
    }
}

#[cfg(test)]
mod tests {

    use std::fs::copy;

    use super::*;

    // fn test_tokenizer() {
    //     let jieba = Jieba::new().unwrap();
    //     //搜索引擎模式
    //     let words = jieba.cut_for_search("小明硕士，毕业于中国科学院计算所，后在日本京都大学深造");

    //     println!("【搜索引擎模式】:{}\n", words.join(" / "));
    // }
    #[test]
    fn test_copy() {
        let mut a = [0; 9];
        let b = [1, 2, 3, 4, 5];
        let i = iocopy!(&mut a, &b);
        println!("{:?},{}", a, i);
    }

    #[test]
    fn test_wal() {
        let mut wal = Wal::new(
            &PathBuf::from("/opt/rsproject/gptgrep/searchlite/00.wal"),
            1 * 1024 * 1024, //512MB
            &IOType::MMAP,
        )
        .unwrap();
        let buf = "abcdeee";
        wal.write(buf.as_bytes()).unwrap();
        wal.flush().unwrap();
    }

    use super::super::schema::{BinarySerialize, Document, FieldID, FieldValue, Value};
    use chrono::{TimeZone, Utc};
    #[test]
    fn test_document() {
        let mut wal = Wal::new(
            &PathBuf::from("/opt/rsproject/gptgrep/searchlite/00.wal"),
            1 * 1024 * 1024, //512MB
            &IOType::MMAP,
        )
        .unwrap();

        let field_1 = FieldValue::new(FieldID::from_field_id(1), Value::String("aa".to_string()));
        let field_2 = FieldValue::new(FieldID::from_field_id(2), Value::I64(123));
        let field_3 = FieldValue::new(FieldID::from_field_id(3), Value::U64(123456));
        let field_4 = FieldValue::new(FieldID::from_field_id(4), Value::I32(963));
        let field_5 = FieldValue::new(FieldID::from_field_id(5), Value::U32(123789));
        let field_6 = FieldValue::new(FieldID::from_field_id(6), Value::F64(123.456));
        let field_7 = FieldValue::new(FieldID::from_field_id(7), Value::F32(963.852));
        let field_8 = FieldValue::new(FieldID::from_field_id(8), Value::Date(Utc::now()));
        let field_9 = FieldValue::new(
            FieldID::from_field_id(9),
            Value::Bytes(vec![0u8, 1, 2, 3, 4, 5, 6, 7, 8, 9]),
        );
        let field_10 = FieldValue::new(FieldID::from_field_id(7), Value::F32(963.852));
        let field_values = vec![
            field_1, field_2, field_3, field_4, field_5, field_6, field_7, field_8, field_9,
            field_10,
        ];

        let offset = wal.offset();

        let doc1 = Document::from(field_values);
        doc1.binary_serialize(&mut wal).unwrap();
        wal.flush().unwrap();

        let mut wal_read = WalReader::new(&wal, offset);
        let doc2 = Document::binary_deserialize(&mut wal_read).unwrap();
        println!("doc2:{:?}", doc2);
        assert_eq!(doc1, doc2);
    }
}
