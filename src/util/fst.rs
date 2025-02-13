use super::error::GyResult;
use fst::raw::Output;
use fst::raw::Stream;
use fst::raw::{Builder, Fst};
use fst::Streamer;
pub(crate) struct FstBuilder<W: std::io::Write>(Builder<W>);

impl<W: std::io::Write> FstBuilder<W> {
    pub(crate) fn new(w: W) -> FstBuilder<W> {
        let b = Builder::new(w).unwrap();
        FstBuilder(b)
    }

    pub(crate) fn add(&mut self, key: &[u8], val: u64) -> GyResult<()> {
        self.0.insert(key, val).unwrap();
        Ok(())
    }

    pub(crate) fn finish(self) -> GyResult<W> {
        let w = self.0.into_inner()?;
        Ok(w)
    }

    // pub(crate) fn get_ref(&self) -> &[u8] {
    //     self.0.get_ref()
    // }

    pub(crate) fn reset(&mut self) -> GyResult<()> {
        Ok(())
    }
}

pub(crate) struct FstReader<'a>(Fst<&'a [u8]>);

impl<'a> FstReader<'a> {
    pub(crate) fn load(b: &'a [u8]) -> FstReader {
        Self(Fst::new(b).unwrap())
    }

    pub(crate) fn get(&self, key: &[u8]) -> GyResult<u64> {
        let u = self.0.get(key).unwrap();
        Ok(u.value())
    }

    pub(crate) fn iter(&self) -> FstReaderIter {
        FstReaderIter(self.0.stream())
    }
}

#[derive(Clone)]
pub struct FstItem(pub FstCow, pub u64);

impl PartialOrd for FstItem {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.0.as_ref().partial_cmp(other.0.as_ref())
    }
}

impl PartialEq for FstItem {
    fn eq(&self, other: &Self) -> bool {
        self.0.as_ref() == other.0.as_ref()
    }
}

impl Eq for FstItem {}

impl Ord for FstItem {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0.as_ref().cmp(other.0.as_ref())
    }
}

use core::ptr::NonNull;
#[derive(Clone)]
pub struct FstCow {
    ptr: NonNull<u8>,
    len: usize,
}

impl FstCow {
    fn from_raw_parts(ptr: *mut u8, length: usize) -> Self {
        unsafe {
            Self {
                ptr: NonNull::new_unchecked(ptr),
                len: length,
            }
        }
    }
}

impl AsRef<[u8]> for FstCow {
    fn as_ref(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.ptr.as_ptr() as *const u8, self.len) }
    }
}

pub(crate) struct FstReaderIter<'a>(Stream<'a>);

impl<'a> Iterator for FstReaderIter<'a> {
    type Item = FstItem;
    fn next(&mut self) -> Option<Self::Item> {
        let (k, v) = self.0.next()?;
        return Some(FstItem(
            FstCow::from_raw_parts(k.as_ptr() as *mut u8, k.len()),
            v.value(),
        ));
    }
}

#[cfg(test)]
mod tests {

    use std::vec;

    use super::*;
    #[test]
    fn test_fst() {
        // let mut fst = FstBuilder::new();
        // fst.add(b"aa", 1).unwrap();
        // fst.add(b"bb", 2).unwrap();
        // fst.finish();
        // println!("{:?}", fst.get_ref());
        // let fst_r = FstReader::load(fst.get_ref());
        // let u = fst_r.get(b"aa").unwrap();
        // println!("u:{}", u);

        // let mut iter = fst_r.iter();
        // while let Some(v) = iter.next() {
        //     println!("v:{:?}", v.0.as_ref());
        // }
    }

    // #[test]
    // fn test_merge() {
    //     let a = vec![0, 1, 2, 3];
    //     let b = vec![3, 4, 6, 7, 8, 9];
    //     CompactionMerger::new(a.iter().peekable(), b.iter().peekable())
    //         .merge()
    //         .for_each(|e| match (e.0, e.1) {
    //             (Some(a), Some(b)) => {
    //                 println!("a:{},b{}", a, b)
    //             }
    //             (None, Some(b)) => println!("a:{},b{}", "none", b),
    //             (Some(a), None) => {
    //                 println!("a:{},b{}", a, "none")
    //             }
    //             (None, None) => {
    //                 println!("none")
    //             }
    //         });
    // }
}
