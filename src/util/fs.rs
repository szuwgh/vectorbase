use super::error::GyResult;

use crate::config::DATA_FILE;
use crate::config::WAL_FILE;
use crate::iocopy;
use crate::GyError;
use fs2::FileExt;
use memmap2::{self, MmapMut};
use regex::Regex;
use serde::de::DeserializeOwned;
use serde::ser::Serialize;
use std::fs;
use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter};
#[cfg(target_os = "linux")]
use std::os::unix::fs::{FileExt as linuxFileExt, MetadataExt};
#[cfg(target_os = "windows")]
use std::os::windows::fs::{FileExt as windowsFileExt, MetadataExt};
use std::path::{Path, PathBuf};
use ulid::Ulid;
pub struct GyFile {
    path: PathBuf,
    file: File,
}

impl GyFile {
    pub fn open<P: AsRef<Path>>(p: P) -> GyResult<GyFile> {
        let file = OpenOptions::new().read(true).open(p.as_ref())?;
        Ok(GyFile {
            path: p.as_ref().to_path_buf(),
            file: file,
        })
    }

    pub fn file(&self) -> &File {
        &self.file
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    #[cfg(target_os = "linux")]
    pub(crate) fn fsize(&self) -> GyResult<usize> {
        let s = self.file.metadata()?.size() as usize;
        Ok(s as usize)
    }

    #[cfg(target_os = "windows")]
    pub fn fsize(&self) -> GyResult<usize> {
        let s = self.file.metadata()?.file_size();
        Ok(s as usize)
    }

    #[cfg(target_os = "windows")]
    pub(crate) fn read_at(&self, buf: &mut [u8], offset: u64) -> GyResult<usize> {
        let u = self.file.seek_read(buf, offset)?;
        Ok(u)
    }

    #[cfg(target_os = "linux")]
    pub(crate) fn read_at(&self, buf: &mut [u8], offset: u64) -> GyResult<usize> {
        let u = self.file.read_at(buf, offset)?;
        Ok(u)
    }
}

struct BufferFileWriter {}

pub struct FileManager;

impl FileManager {
    pub(crate) fn get_mem_wal_fname<P: AsRef<Path>>(p: P) -> GyResult<PathBuf> {
        return Ok(PathBuf::new().join(p).join("mem.wal"));
    }

    pub(crate) fn get_imm_wal_fname<P: AsRef<Path>>(p: P) -> GyResult<PathBuf> {
        return Ok(PathBuf::new().join(p).join("imm.wal"));
    }

    pub(crate) fn get_next_table_dir<P: AsRef<Path>>(p: P) -> GyResult<PathBuf> {
        let uid = Ulid::new();
        let dir = PathBuf::new().join(p).join(uid.to_string());
        return Ok(dir);
    }

    pub(crate) fn get_rename_wal_path<P: AsRef<Path>>(p: P) -> GyResult<PathBuf> {
        let path = p.as_ref();
        let parent_dir = path.parent().unwrap();
        if let Some(name_str) = path.to_str() {
            // 去掉文件扩展名
            let base_name = name_str
                .strip_suffix(".wal")
                .expect("File name format is incorrect");
            return Ok(PathBuf::new()
                .join(parent_dir)
                .join(base_name)
                .join(DATA_FILE));
        }
        todo!()
    }

    pub(crate) fn get_next_wal_name<P: AsRef<Path>>(dir: P) -> GyResult<PathBuf> {
        let list = Self::get_files_with_extension(dir.as_ref(), "wal")?;
        if list.len() == 0 {
            return Ok(PathBuf::new()
                .join(dir.as_ref())
                .join(format!("{:0>20}{}", 0, WAL_FILE)));
        } else {
            let last_name = list.first().unwrap().file_name().unwrap();
            if let Some(name_str) = last_name.to_str() {
                // 去掉文件扩展名
                let base_name = name_str
                    .strip_suffix(".wal")
                    .expect("File name format is incorrect");
                // 将字符串转换为数字
                println!("base_name:{}", base_name);
                let number: u64 = base_name
                    .parse()
                    .expect("Unable to parse string into number");
                // 打印结果
                return Ok(PathBuf::new().join(dir.as_ref()).join(format!(
                    "{:0>20}{}",
                    number + 1,
                    WAL_FILE
                )));
            }
        }
        return Err(GyError::EOF);
    }

    pub(crate) fn get_2wal_file_name<P: AsRef<Path>>(
        dir: P,
        extension: &str,
    ) -> GyResult<(Option<PathBuf>, Option<PathBuf>)> {
        let list = Self::get_files_with_extension(dir, extension)?;
        match list.len() {
            0 => return Ok((None, None)),
            1 => return Ok((Some(list[0].clone()), None)),
            2 => return Ok((Some(list[0].clone()), Some(list[1].clone()))),
            _ => return Err(GyError::ErrCollectionWalInvalid),
        }
    }

    pub(crate) fn get_directories<P: AsRef<Path>>(dir: P) -> GyResult<Vec<PathBuf>> {
        let mut directories = Vec::new();
        for entry in fs::read_dir(dir)? {
            let entry = entry?;
            let path = entry.path();
            // 仅检查文件夹
            if path.is_dir() {
                directories.push(path);
            }
        }
        // 按文件名正序排序
        directories.sort_by(|a, b| b.file_name().cmp(&a.file_name()));
        Ok(directories)
    }

    pub(crate) fn get_table_directories<P: AsRef<Path>>(dir: P) -> GyResult<Vec<PathBuf>> {
        let mut directories = Vec::new();
        let ulid_regex = Regex::new(r"^[0-9A-HJ-NP-Z]{26}$").unwrap();
        for entry in fs::read_dir(dir)? {
            let entry = entry?;
            let path = entry.path();

            // 仅检查文件夹
            if path.is_dir() {
                if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
                    // 检查文件夹名是否匹配正则表达式
                    if ulid_regex.is_match(name) {
                        directories.push(path);
                    }
                }
            }
        }
        // 按文件名正序排序
        directories.sort_by(|a, b| b.file_name().cmp(&a.file_name()));
        Ok(directories)
    }

    // 查找某个目录下具有指定后缀的文件
    pub(crate) fn get_files_with_extension<P: AsRef<Path>>(
        dir: P,
        extension: &str,
    ) -> GyResult<Vec<PathBuf>> {
        let mut files_with_extension = Vec::new();

        // 读取目录条目
        for entry in fs::read_dir(dir)? {
            let entry = entry?;
            let path = entry.path();

            // 仅检查文件，不递归子目录
            if path.is_file() {
                if let Some(ext) = path.extension() {
                    // 检查文件的后缀是否匹配
                    if ext == extension {
                        files_with_extension.push(path);
                    }
                }
            }
        }
        // 按文件名倒序排序
        files_with_extension
            .sort_by(|a, b| b.file_name().cmp(&a.file_name()).then_with(|| a.cmp(b)));
        Ok(files_with_extension)
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
}

pub(crate) trait IoSelector {
    fn write(&mut self, data: &[u8], offset: usize) -> GyResult<usize>;

    fn read(&self, data: &mut [u8], offset: usize) -> GyResult<usize>;

    fn read_bytes(&self, offset: usize, n: usize) -> GyResult<&[u8]>;

    fn reopen(&mut self, fname: &Path, fsize: usize) -> GyResult<()>;

    fn sync(&mut self) -> GyResult<()>;

    fn close(&mut self) -> GyResult<()>;

    fn delete(&self) -> GyResult<()>;
}

pub(crate) struct MmapSelector {
    file: File,
    mmap: Option<MmapMut>,
    fsize: usize,
}

impl MmapSelector {
    pub(crate) fn new(fname: &Path, fsize: usize) -> GyResult<MmapSelector> {
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(fname)?;
        //  println!("fsize:{}", fsize);
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
            mmap: Some(nmmap),
            fsize: fsize,
        })
    }

    pub(crate) fn open(fname: &Path, fsize: usize) -> GyResult<MmapSelector> {
        let file = OpenOptions::new().read(true).write(true).open(fname)?;
        //  file.allocate(fsize as u64)?;
        let nmmap = unsafe {
            memmap2::MmapOptions::new()
                .offset(0)
                .len(fsize)
                .map_mut(&file)
                .map_err(|e| format!("mmap failed: {}", e))?
        };
        Ok(Self {
            file: file,
            mmap: Some(nmmap),
            fsize: fsize,
        })
    }

    fn get_mmap_mut(&mut self) -> &mut MmapMut {
        self.mmap.as_mut().unwrap()
    }

    fn get_mmap(&self) -> &MmapMut {
        self.mmap.as_ref().unwrap()
    }
}

impl IoSelector for MmapSelector {
    fn write(&mut self, data: &[u8], offset: usize) -> GyResult<usize> {
        let i = iocopy!(&mut self.get_mmap_mut()[offset..], data);
        Ok(i)
    }

    fn read(&self, data: &mut [u8], offset: usize) -> GyResult<usize> {
        let i = iocopy!(data, &self.get_mmap()[offset..]);
        Ok(i)
    }

    fn read_bytes(&self, offset: usize, n: usize) -> GyResult<&[u8]> {
        if offset + n > self.fsize {
            return Err(GyError::EOF);
        }
        let v = &self.get_mmap()[offset..offset + n];
        Ok(v)
    }

    fn reopen(&mut self, fname: &Path, fsize: usize) -> GyResult<()> {
        if let Some(mmap) = self.mmap.take() {
            drop(mmap);
        }
        drop(&self.file);
        let file = OpenOptions::new().read(true).write(true).open(fname)?;
        file.set_len(fsize as u64)?;
        let nmmap = unsafe {
            memmap2::MmapOptions::new()
                .offset(0)
                .len(fsize)
                .map_mut(&file)
                .map_err(|e| format!("mmap failed: {}", e))?
        };
        self.file = file;
        self.mmap = Some(nmmap);
        Ok(())
    }

    fn sync(&mut self) -> GyResult<()> {
        self.get_mmap_mut().flush()?;
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

    fn reopen(&mut self, fname: &Path, fsize: usize) -> GyResult<()> {
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

    #[test]
    fn test_get_2wal_file_name() {
        let (a, b) =
            FileManager::get_2wal_file_name("/opt/rsproject/chappie/searchlite/data", "wal")
                .unwrap();
        println!("{:?},{:?}", a, b);
    }

    #[test]
    fn test_get_table_directories() {
        let l = FileManager::get_table_directories(
            "/opt/rsproject/chappie/vectorbase/example/embed/data/vector1",
        )
        .unwrap();
        println!("{:?}", l);
    }

    #[test]
    fn test_get_next_wal_name() {
        let l = FileManager::get_next_wal_name("/opt/rsproject/chappie/searchlite/data1").unwrap();
        println!("{:?}", l);
    }
}
