pub mod ann;
mod memory;
mod query;
mod schema;
mod tokenize;
mod util;

use crate::memory::MemTable;
use crate::query::Query;
use crate::schema::Document;
use space::Metric;
pub struct IndexConfig {}

pub struct IndexWriter {
    mem_table: MemTable,
    doc_id: u64,
}

impl IndexWriter {
    fn new() -> IndexWriter {
        Self {
            mem_table: MemTable::default(),
            doc_id: 0,
        }
    }

    fn add_document(&mut self, doc: &Document) {
        self.mem_table.index_document(doc);
    }

    
}

struct IndexDiskWriter {}

struct IndexReader {}

impl IndexReader {
    fn search(query: &Query) {}
}
