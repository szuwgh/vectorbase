use memmap2::MmapMut;
use std::path::PathBuf;

struct Page {
    path: PathBuf,
    mmap: MmapMut,
}

impl Page {}
