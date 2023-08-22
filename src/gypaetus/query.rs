use super::schema::FieldID;
use super::searcher::Searcher;
use super::util::common;
use super::{IndexReader, PostingReader};
use byteorder::{BigEndian, ByteOrder};
const INT_TERM_LEN: usize = 4 + 8;

pub struct TermQuery {
    term: Term,
}

impl Query for TermQuery {
    fn query(&self, reader: &IndexReader) -> PostingReader {
        //reader.search(term);
        todo!();
    }
}

pub(crate) struct Term(pub(crate) Vec<u8>);

impl Term {
    pub fn from_field_i32(field: FieldID, val: i32) -> Term {
        let mut term = Term(vec![0u8; 8]);
        term.set_field(field);
        term.set_i32(val);
        term
    }

    pub fn from_field_u64(field: FieldID, val: u64) -> Term {
        let mut term = Term(vec![0u8; INT_TERM_LEN]);
        term.set_field(field);
        term.set_u64(val);
        term
    }

    pub fn set_field(&mut self, field: FieldID) {
        BigEndian::write_u32(&mut self.0[0..4], field.0);
    }

    pub fn set_u64(&mut self, val: u64) {
        BigEndian::write_u64(&mut self.0[4..], val);
    }

    pub fn set_i32(&mut self, val: i32) {
        BigEndian::write_i32(&mut self.0[4..], val);
    }

    pub fn field_id(&self) -> FieldID {
        FieldID::from_field_id(BigEndian::read_u32(&self.0[..4]))
    }

    pub fn bytes_value(&self) -> &[u8] {
        &self.0[4..]
    }
}

pub trait Query {
    fn query(&self, searcher: &IndexReader) -> PostingReader;
}
