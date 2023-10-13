use super::error::GyResult;
use std::fs;
use std::fs::{File, OpenOptions};
use std::path::{Path, PathBuf};
pub(crate) fn open_file(fname: PathBuf, read: bool, write: bool) -> GyResult<File> {
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
}
