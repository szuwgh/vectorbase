use memmap2::MmapMut;
use std::path::PathBuf;

//一个文件一页 32MB
struct Page {
    path: PathBuf,
    mmap: MmapMut,
}

impl Page {}
