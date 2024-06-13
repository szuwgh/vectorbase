use super::error::GyResult;
use furze::{fst::Cow, Builder, FstIterator, FST};

pub(crate) fn new() {}

pub(crate) struct FstBuilder(Builder<Vec<u8>>);

impl FstBuilder {
    pub(crate) fn new() -> FstBuilder {
        FstBuilder(Builder::new(Vec::with_capacity(4 * 1024 * 1024)))
    }

    pub(crate) fn add(&mut self, key: &[u8], val: u64) -> GyResult<()> {
        self.0.add(key, val)?;
        Ok(())
    }

    pub(crate) fn finish(&mut self) -> GyResult<()> {
        self.0.finish()?;
        Ok(())
    }

    pub(crate) fn get_ref(&self) -> &[u8] {
        self.0.get()
    }

    pub(crate) fn reset(&mut self) -> GyResult<()> {
        self.0.reset()?;
        Ok(())
    }
}

pub(crate) struct FstReader<'a>(FST<&'a [u8]>);

impl<'a> FstReader<'a> {
    pub(crate) fn load(b: &'a [u8]) -> FstReader {
        Self(FST::load(b))
    }

    pub(crate) fn get(&self, key: &[u8]) -> GyResult<u64> {
        let u = self.0.get(key)?;
        Ok(u)
    }

    pub(crate) fn iter(&self) -> FstReaderIter {
        FstReaderIter {
            iter: self.0.iter(),
        }
    }
}

pub(crate) struct FstReaderIter<'a> {
    iter: FstIterator<'a, &'a [u8]>,
    item: Option<(Cow, u64)>,
}

impl<'a> FstReaderIter<'a> {
    fn next(&mut self) -> bool {
        let m = self.iter.next();
        match m {
            Some(v) => {
                self.item = m;
                return true;
            }
            None => return false,
        }
    }
    fn at(&self) -> (&'a [u8], u64) {
        self.item
    }
}

#[cfg(test)]
mod tests {

    use std::collections::HashMap;
    use std::vec;

    use super::*;
    #[test]
    fn test_fst() {
        let mut fst = FstBuilder::new();
        fst.add(b"aa", 1).unwrap();
        fst.add(b"bb", 2).unwrap();
        fst.finish();
        println!("{:?}", fst.get_ref());
        let fst_r = FstReader::load(fst.get_ref());
        let u = fst_r.get(b"aa").unwrap();
        println!("u:{}", u);

        let mut iter = fst_r.iter();
        while let Some(v) = iter.next() {
            println!("v:{:?}", v);
        }
    }
    use super::*;
    use std::collections::BTreeMap;
    #[test]
    fn test_merge() {
        let mut a_map: BTreeMap<i32, Vec<i32>> = BTreeMap::new();
        a_map.insert(1, vec![1, 2, 3]);
        a_map.insert(2, vec![1, 2, 3]);
        a_map.insert(3, vec![1, 2, 3]);

        let mut b_map: BTreeMap<i32, Vec<usize>> = BTreeMap::new();
        b_map.insert(2, vec![1, 2, 3]);
        b_map.insert(3, vec![1, 2, 3]);
        b_map.insert(4, vec![1, 2, 3]);
    }
}
