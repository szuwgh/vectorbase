use super::searcher::Searcher;
use super::PostingReader;

pub struct TermQuery {
    term: Term,
}

impl Query for TermQuery {
    fn query(&self, searcher: &Searcher) -> PostingReader {
        todo!();
    }
}

pub(crate) struct Term(pub(crate) Vec<u8>);

pub trait Query {
    fn query(&self, searcher: &Searcher) -> PostingReader;
}
