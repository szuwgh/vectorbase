pub mod ann;
mod memory;
mod query;
mod schema;
mod store;
mod tokenize;
mod util;

use crate::memory::MemTable;
use crate::query::Query;
use crate::schema::Row;

pub struct IndexConfig {}

pub struct IndexWriter {
    mem_table: MemTable,
    doc_id: u64,
}

impl IndexWriter {
    fn new() -> IndexWriter {
        Self {
            mem_table: MemTable::new(),
            doc_id: 0,
        }
    }

    fn add_row(&mut self, doc: &Row) {
        self.mem_table.index_row(doc);
    }

    fn commit() {}
}

struct IndexDiskWriter {}

struct IndexReader {}

impl IndexReader {
    fn search(query: &Query) {}
}
