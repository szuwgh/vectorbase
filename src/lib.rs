mod document;
mod embed;
mod field;
mod memory;
mod util;
mod tokenize;

use crate::document::Document;
use crate::memory::mem;

pub struct IndexWriter {
    mem_table: mem::MemTable,
}

impl IndexWriter {
    fn new() -> IndexWriter {
        Self {
            mem_table: mem::MemTable::default(),
        }
    }
}

pub struct IndexDiskWriter {}

struct IndexReader {}

impl IndexReader {
    fn search(doc: &Document) {}
}
