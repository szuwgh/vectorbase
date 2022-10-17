pub mod annoy;
pub mod hnsw;
pub use self::hnsw::HNSW;

pub trait AnnIndex<T, B> {
    fn insert(&mut self, k: T, v: u64);
    fn near(&mut self, k: &T, dest: &mut [B]);
}