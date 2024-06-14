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
        FstReaderIter(self.0.iter())
    }
}

pub(crate) struct FstReaderIter<'a>(FstIterator<'a, &'a [u8]>);

impl<'a> Iterator for FstReaderIter<'a> {
    type Item = (Cow, u64);
    fn next(&mut self) -> Option<Self::Item> {
        self.0.next()
    }
}

use std::cmp::Ordering;
use std::iter::Peekable;

struct CompactionMerger<T: Iterator> {
    a: Peekable<T>,
    b: Peekable<T>,
}

impl<T: Iterator> CompactionMerger<T>
where
    T::Item: Ord,
{
    fn new(a: Peekable<T>, b: Peekable<T>) -> CompactionMerger<T> {
        Self { a, b }
    }

    fn merge(mut self) -> impl Iterator<Item = (Option<T::Item>, Option<T::Item>)>
    where
        Self: Sized,
    {
        std::iter::from_fn(move || match (self.a.peek(), self.b.peek()) {
            (Some(v1), Some(v2)) => match v1.cmp(v2) {
                Ordering::Less => Some((self.a.next(), None)),
                Ordering::Greater => Some((None, self.b.next())),
                Ordering::Equal => Some((self.a.next(), self.b.next())),
            },
            (Some(_), None) => Some((self.a.next(), None)),
            (None, Some(_)) => Some((None, self.b.next())),
            (None, None) => None,
        })
    }
}

#[cfg(test)]
mod tests {

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
            println!("v:{:?}", v.0.as_ref());
        }
    }

    #[test]
    fn test_merge() {
        let a = vec![0, 1, 2, 3];
        let b = vec![3, 4, 6, 7, 8, 9];
        CompactionMerger::new(a.iter().peekable(), b.iter().peekable())
            .merge()
            .for_each(|e| match (e.0, e.1) {
                (Some(a), Some(b)) => {
                    println!("a:{},b{}", a, b)
                }
                (None, Some(b)) => println!("a:{},b{}", "none", b),
                (Some(a), None) => {
                    println!("a:{},b{}", a, "none")
                }
                (None, None) => {
                    println!("none")
                }
            });
    }
}
