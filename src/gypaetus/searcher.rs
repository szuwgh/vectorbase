use super::PostingReader;

use super::query::{Query, Term};
use super::schema::Document;
use super::IndexReader;

pub struct Searcher {
    readers: IndexReader,
}

impl Searcher {
    fn new(reader: IndexReader) -> Searcher {
        Searcher { readers: reader }
    }

    fn and(&mut self, reader: IndexReader) -> Searcher {
        Searcher { readers: reader }
    }

    fn doc() -> Document {
        todo!();
    }

    pub fn search(&self, query: &dyn Query) {
        query.query(&self);
    }
}
