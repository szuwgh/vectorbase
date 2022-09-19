pub mod hnsw;
pub use self::hnsw::HNSW;
use space::{Metric, Neighbor};

pub trait KnnIndex<T, B> {
    fn insert(&mut self, k: T, v: u64);
    fn near(&mut self, k: &T, dest: &mut [B]);
}
