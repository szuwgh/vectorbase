pub(super) trait TextIndex {
    fn insert(&mut self, k: Vec<u8>, v: u64);
}

pub(super) trait Block {}
pub(super) struct BytesBlock {}

impl BytesBlock {}
