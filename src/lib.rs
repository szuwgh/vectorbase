mod memory;
mod query;
mod schema;
mod tokenize;
mod util;

use crate::memory::mem;
use crate::query::Query;
use crate::schema::document::Document;

pub struct Indexer {
    mem_table: mem::MemTable,
}

impl Indexer {
    fn new() -> Indexer {
        Self {
            mem_table: mem::MemTable::default(),
        }
    }

    fn add_document(doc: &Document) {}
}

pub struct IndexDiskWriter {}

struct IndexReader {}

impl IndexReader {
    fn search(query: &Query) {}
}
