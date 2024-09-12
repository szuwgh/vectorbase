use super::error::GyResult;

use crate::iocopy;
use crate::GyError;
use fs2::FileExt;
use memmap2::MmapAsRawDesc;
use memmap2::{self, MmapMut};
use serde::de::DeserializeOwned;
use serde::ser::Serialize;
use std::fs;
use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter};
#[cfg(target_os = "linux")]
use std::os::unix::fs::{FileExt, MetadataExt};
#[cfg(target_os = "windows")]
use std::os::windows::fs::{FileExt as windowsFileExt, MetadataExt};
use std::path::{Path, PathBuf};
pub struct GyFile(File);

impl GyFile {
    pub fn open<P: AsRef<Path>>(path: P) -> GyResult<GyFile> {
        let file = OpenOptions::new().read(true).open(path)?;
        Ok(GyFile(file))
    }

    pub fn file(&self) -> &File {
        &self.0
    }

    #[cfg(target_os = "linux")]
    pub(crate) fn fsize(&self) -> GyResult<usize> {
        self.0.metadata()?.size();
    }

    #[cfg(target_os = "windows")]
    pub fn fsize(&self) -> GyResult<usize> {
        let s = self.0.metadata()?.file_size();
        Ok(s as usize)
    }

    #[cfg(target_os = "windows")]
    pub(crate) fn read_at(&self, buf: &mut [u8], offset: u64) -> GyResult<usize> {
        let u = self.0.seek_read(buf, offset)?;
        Ok(u)
    }

    #[cfg(target_os = "linux")]
    pub(crate) fn read_at(&self, buf: &mut [u8], offset: u64) -> GyResult<usize> {
        let u = self.0.read_at(buf, offset)?;
        Ok(u)
    }
}

// 保存结构体到文件
pub fn to_json_file<T: Serialize, P: AsRef<Path>>(data: &T, filename: P) -> GyResult<()> {
    let file = File::create(filename)?;
    let writer = BufWriter::new(file);
    serde_json::to_writer(writer, data)?;
    Ok(())
}

// 从文件读取并反序列化结构体
pub fn from_json_file<T: DeserializeOwned, P: AsRef<Path>>(filename: P) -> GyResult<T> {
    let file = File::open(filename)?;
    let reader = BufReader::new(file);
    let data = serde_json::from_reader(reader)?;
    Ok(data)
}

pub(crate) fn open_file(fname: &Path, read: bool, write: bool) -> GyResult<File> {
    let file = OpenOptions::new()
        .create(true)
        .read(read)
        .write(write)
        .open(fname)?;
    Ok(file)
}

pub(crate) fn mkdir(path: &Path) -> GyResult<()> {
    fs::create_dir_all(path)?;
    Ok(())
}

pub(crate) trait IoSelector {
    fn write(&mut self, data: &[u8], offset: usize) -> GyResult<usize>;

    fn read(&self, data: &mut [u8], offset: usize) -> GyResult<usize>;

    fn read_bytes(&self, offset: usize, n: usize) -> GyResult<&[u8]>;

    fn sync(&mut self) -> GyResult<()>;

    fn close(&mut self) -> GyResult<()>;

    fn delete(&self) -> GyResult<()>;
}

pub(crate) struct MmapSelector {
    file: File,
    mmap: MmapMut,
    fsize: usize,
}

impl MmapSelector {
    pub(crate) fn new(fname: &Path, fsize: usize) -> GyResult<MmapSelector> {
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
            fsize: fsize,
        })
    }

    pub(crate) fn open(fname: &Path, fsize: usize) -> GyResult<MmapSelector> {
        let file = OpenOptions::new().read(true).write(true).open(fname)?;
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
            fsize: fsize,
        })
    }
}

impl IoSelector for MmapSelector {
    fn write(&mut self, data: &[u8], offset: usize) -> GyResult<usize> {
        let i = iocopy!(&mut self.mmap[offset..], data);
        Ok(i)
    }

    fn read(&self, data: &mut [u8], offset: usize) -> GyResult<usize> {
        let i = iocopy!(data, &self.mmap[offset..]);
        Ok(i)
    }

    fn read_bytes(&self, offset: usize, n: usize) -> GyResult<&[u8]> {
        if offset + n > self.fsize {
            return Err(GyError::EOF);
        }
        let v = &self.mmap[offset..offset + n];
        Ok(v)
    }

    fn sync(&mut self) -> GyResult<()> {
        self.mmap.flush()?;
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
    pub(crate) fn new(fname: &Path, fsize: usize) -> GyResult<FileIOSelector> {
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

    fn read_bytes(&self, offset: usize, n: usize) -> GyResult<&[u8]> {
        todo!()
    }
}

pub(crate) fn next_sequence_ext_file(path: &Path, ext: &str) -> GyResult<PathBuf> {
    let mut j = -1;
    for entry in read_dir_from_ext(path, ext)?.iter() {
        let new_name = entry.with_extension("");
        if let Some(Ok(i)) = new_name
            .to_str()
            .and_then(|name| Some(name.parse::<i32>().and_then(|i| Ok(i))))
        {
            j = i;
        }
    }

    Ok(PathBuf::from(format!("{:016}", j + 1)).with_extension(ext))
}

pub(crate) fn read_dir_from_ext(path: &Path, ext: &str) -> GyResult<Vec<PathBuf>> {
    let mut files: Vec<PathBuf> = Vec::new();
    for entry in fs::read_dir(path)? {
        let entry = entry?;
        let path = entry.path();
        if path.is_file() {
            if let Some(e) = path.extension() {
                let ext_str = e.to_string_lossy().to_string();
                if ext_str == ext {
                    if let Some(name) = path.file_name() {
                        files.push(PathBuf::from(name));
                    }
                }
            }
        }
    }
    files.sort();
    Ok(files)
}

#[cfg(test)]
mod tests {
    use super::*;

    // fmt::Debug：使用 {:?} 标记。格式化文本以供调试使用。
    // fmt::Display：使用 {} 标记。以更优雅和友好的风格来格式化文本。
    #[test]
    fn test_read_dir_from_ext() {
        let data_dir = PathBuf::from("/opt/rsproject/gptgrep/searchlite/data");
        let files = read_dir_from_ext(&data_dir, "wal");
        println!("file:{:?}", files);
    }

    #[test]
    fn test_next_sequence_ext_file() {
        let data_dir = PathBuf::from("/opt/rsproject/gptgrep/searchlite/data");
        let file = next_sequence_ext_file(&data_dir, "wal").unwrap();
        println!("file:{:?}", file);
    }

    #[test]
    fn test_to_json_file() {
        let data_dir = PathBuf::from("/opt/rsproject/gptgrep/searchlite/data");
        let file = next_sequence_ext_file(&data_dir, "wal").unwrap();
        println!("file:{:?}", file);
    }
}
