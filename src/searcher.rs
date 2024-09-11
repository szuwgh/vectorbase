use super::PostingReader;

use super::query::{Query, Term};
use super::schema::{DocID, Document};
use super::GyResult;
use super::IndexReader;

pub struct Searcher {
    reader: IndexReader,
}

impl Searcher {
    fn new(reader: IndexReader) -> Searcher {
        Searcher { reader: reader }
    }

    fn and(&mut self, reader: IndexReader) -> Searcher {
        Searcher { reader: reader }
    }

    fn doc(&self, doc_id: DocID) -> GyResult<Document> {
        self.reader.doc(doc_id)
    }

    pub fn search(&self, query: &dyn Query) {
        query.query(&self.reader);
    }
}
