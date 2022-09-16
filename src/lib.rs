pub mod knn;
mod memory;
mod query;
mod schema;
mod tokenize;
mod util;

use crate::memory::MemTable;
use crate::query::Query;
use crate::schema::Document;
use knn::hnsw::Euclidean;
use knn::HNSW;
use space::Metric;
pub struct IndexConfig {}

pub struct IndexTextWriter {
    mem_table: MemTable,
    doc_id: u64,
}

impl IndexTextWriter {
    fn new() -> IndexTextWriter {
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

pub struct IndexVectorWriter<I> {
    vector: I,
}

impl<T> IndexVectorWriter<T> {
    fn new() {
        //let mut hnsw = HNSW::<Euclidean, Vec<f32>>::new(Euclidean);
    }
}
