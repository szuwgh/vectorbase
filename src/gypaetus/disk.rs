use std::path::PathBuf;

pub(crate) struct DiskStoreWriter {}

//合并索引
pub fn merge_index() {}

struct DiskIndexReader {}

impl DiskStoreWriter {
    fn from(name: PathBuf, offset: usize) -> DiskStoreWriter {
        Self {}
    }

    fn new(name: PathBuf) {}

    fn write() {}

    fn flush() {}
}
