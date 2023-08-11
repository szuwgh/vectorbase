use super::util::error::{GyError, GyResult};
use fs2::FileExt;
use memmap2::{self, Mmap, MmapMut};
use std::fs::{self, File};
use std::io::Read;
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock, Weak};
use std::{
    fs::OpenOptions,
    io::{Seek, SeekFrom, Write},
};
const DEFAULT_WAL_FILE_SIZE: usize = 512 << 20; //

#[macro_export]
macro_rules! copy {
    ($des:expr, $src:expr) => {
        copy_slice($des, $src)
    };
}

fn copy_slice<T: Copy>(des: &mut [T], src: &[T]) -> usize {
    let l = if des.len() < src.len() {
        des.len()
    } else {
        src.len()
    };
    unsafe {
        std::ptr::copy_nonoverlapping(src.as_ptr(), des.as_mut_ptr(), l);
    }
    l
}

pub enum IOType {
    FILEIO,
    MMAP,
}

pub(crate) trait IoSelector {
    fn write(&mut self, data: &[u8], offset: usize) -> GyResult<usize>;

    fn read(&self, data: &mut [u8], offset: usize) -> GyResult<usize>;

    fn sync(&mut self) -> GyResult<()>;

    fn close(&mut self) -> GyResult<()>;

    fn delete(&self) -> GyResult<()>;
}

const BLOCK_SIZE: usize = 1 << 15;

unsafe impl Send for Wal {}
unsafe impl Sync for Wal {}

pub(crate) struct WalReader {
    wal: Arc<Wal>,
    offset: usize,
}

pub(crate) struct Wal {
    io_selector: Box<dyn IoSelector>,
    i: usize,
    j: usize,
    fsize: usize,
    buf: [u8; BLOCK_SIZE],
}

impl Wal {
    pub(crate) fn new(fname: &Path, fsize: usize, io_type: IOType) -> GyResult<Wal> {
        let io_selector: Box<dyn IoSelector> = match io_type {
            IOType::FILEIO => Box::new(FileIOSelector::new(fname, fsize)?),
            IOType::MMAP => Box::new(MmapSelector::new(fname, fsize)?),
        };
        Ok(Self {
            io_selector: io_selector,
            i: 0,
            j: 0,
            fsize: fsize,
            buf: [0u8; BLOCK_SIZE],
        })
    }

    pub(crate) fn check_rotate(&self, size: usize) -> GyResult<()> {
        if self.i + size > self.fsize {
            return Err(GyError::ErrWalOverflow);
        }
        Ok(())
    }

    pub(crate) fn write_bytes(&mut self, content: &[u8]) -> GyResult<usize> {
        let sz = self.write(content)?;
        self.i += sz;
        Ok(self.i)
    }
}

impl Read for WalReader {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let i = self
            .wal
            .io_selector
            .read(buf, self.offset)
            .map_err(|e| std::io::Error::from(std::io::ErrorKind::UnexpectedEof))?;
        Ok(i)
    }
}

impl Write for Wal {
    fn write(&mut self, mut buf: &[u8]) -> std::io::Result<usize> {
        while buf.len() > 0 {
            if self.j == BLOCK_SIZE {
                self.flush()?;
            }
            let n = copy!(&mut self.buf[self.j..], buf);
            self.j += n;
            buf = &buf[n..];
        }
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.io_selector
            .write(&self.buf[..self.j], self.i)
            .map_err(|e| std::io::Error::from(std::io::ErrorKind::InvalidData))?;
        self.io_selector
            .sync()
            .map_err(|e| std::io::Error::from(std::io::ErrorKind::InvalidData))?;
        self.i += self.buf[..self.j].len();
        //  if self.i >=
        self.j = 0;
        Ok(())
    }
}

pub(crate) struct MmapSelector {
    file: File,
    mmap: MmapMut,
}

impl MmapSelector {
    fn new(fname: &Path, fsize: usize) -> GyResult<MmapSelector> {
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(fname)?;
        file.allocate(fsize as u64)?;
        let nmmap = unsafe {
            memmap2::MmapOptions::new()
                .offset(0)
                .len(fsize)
                .map_mut(&file)
                .map_err(|e| format!("mmap failed: {}", e))?
        };
        Ok(Self {
            file: file,
            mmap: nmmap,
        })
    }
}

impl IoSelector for MmapSelector {
    fn write(&mut self, data: &[u8], offset: usize) -> GyResult<usize> {
        let i = copy!(&mut self.mmap[offset..], data);
        Ok(i)
    }

    fn read(&self, data: &mut [u8], offset: usize) -> GyResult<usize> {
        let i = copy!(data, &self.mmap[offset..]);
        Ok(i)
    }

    fn sync(&mut self) -> GyResult<()> {
        self.mmap.flush()?;
        // self.file.flush()?;
        Ok(())
    }

    fn close(&mut self) -> GyResult<()> {
        self.sync()?;
        Ok(())
    }

    fn delete(&self) -> GyResult<()> {
        todo!()
    }
}

pub(crate) struct FileIOSelector {
    file: File,
    mmap: MmapMut,
}

impl FileIOSelector {
    fn new(fname: &Path, fsize: usize) -> GyResult<FileIOSelector> {
        todo!()
    }
}

impl IoSelector for FileIOSelector {
    fn write(&mut self, data: &[u8], offset: usize) -> GyResult<usize> {
        todo!()
    }

    fn read(&self, data: &mut [u8], offset: usize) -> GyResult<usize> {
        todo!()
    }

    fn sync(&mut self) -> GyResult<()> {
        todo!()
    }

    fn close(&mut self) -> GyResult<()> {
        todo!()
    }

    fn delete(&self) -> GyResult<()> {
        todo!()
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
        let i = copy!(&mut a, &b);
        println!("{:?},{}", a, i);
    }

    #[test]
    fn test_wal() {
        let mut wal = Wal::new(
            &PathBuf::from("/opt/rsproject/gptgrep/searchlite/00.wal"),
            128 * 1024 * 1024, //512MB
            IOType::MMAP,
        )
        .unwrap();
        let buf = "abcdeee";
        wal.write(buf.as_bytes()).unwrap();
        wal.flush().unwrap();
    }
}
