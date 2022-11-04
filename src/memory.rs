

pub(crate) trait TextIndex {
    fn insert(&mut self, k: Vec<u8>, v: u64);
}

trait Block {}
pub(crate) struct BytesBlock {}
