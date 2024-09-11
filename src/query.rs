use super::schema::FieldID;
use super::searcher::Searcher;
use super::util::common;
use super::{IndexReader, PostingReader};
use byteorder::{BigEndian, ByteOrder};
const INT_TERM_LEN: usize = 4 + 8;
use std::str;
pub struct TermQuery {
    term: Term,
}

impl Query for TermQuery {
    fn query(&self, reader: &IndexReader) -> PostingReader {
        //reader.search(term);
        todo!();
    }
}

pub struct Term(pub(crate) Vec<u8>);

impl Term {
    pub fn from_field_i32(field: FieldID, val: i32) -> Term {
        let mut term = Term(vec![0u8; 8]);
        term.set_field(field);
        term.set_i32(val);
        term
    }

    pub fn from_field_text(field: FieldID, val: &str) -> Term {
        let mut term = Term(Vec::with_capacity(4 + val.len()));
        term.set_field(field);
        term.set_bytes(val.as_bytes());
        term
    }

    pub fn from_field_u64(field: FieldID, val: u64) -> Term {
        let mut term = Term(vec![0u8; INT_TERM_LEN]);
        term.set_field(field);
        term.set_u64(val);
        term
    }

    pub fn set_field(&mut self, field: FieldID) {
        if self.0.len() < 4 {
            self.0.resize(4, 0u8);
        }
        BigEndian::write_u32(&mut self.0[0..4], field.id());
    }

    pub fn set_u64(&mut self, val: u64) {
        BigEndian::write_u64(&mut self.0[4..], val);
    }

    pub fn set_bytes(&mut self, bytes: &[u8]) {
        self.0.resize(4, 0u8);
        self.0.extend(bytes);
    }

    pub fn set_i32(&mut self, val: i32) {
        BigEndian::write_i32(&mut self.0[4..], val);
    }

    pub fn field_id(&self) -> FieldID {
        FieldID::from_field_id(BigEndian::read_u32(&self.0[..4]))
    }
}

impl Term {
    pub fn bytes_value(&self) -> &[u8] {
        &self.0[4..]
    }

    pub fn text(&self) -> &str {
        str::from_utf8(self.bytes_value()).expect("Term does not contain valid utf-8.")
    }
}

pub trait Query {
    fn query(&self, searcher: &IndexReader) -> PostingReader;
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_term() {}
}
