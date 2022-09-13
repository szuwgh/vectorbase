pub mod knn;
mod memory;
mod query;
mod schema;
mod tokenize;
mod util;

use crate::memory::MemTable;
use crate::query::Query;
use crate::schema::Document;

pub struct IndexConfig {}

pub struct Indexer {
    mem_table: MemTable,
    doc_id: u64,
}

impl Indexer {
    fn new() -> Indexer {
        Self {
            mem_table: MemTable::default(),
            doc_id: 0,
        }
    }

    fn add_document(&mut self, doc: &Document) {
        self.mem_table.index_document(doc);
    }
}

pub struct IndexDiskWriter {}

struct IndexReader {}

impl IndexReader {
    fn search(query: &Query) {}
}
