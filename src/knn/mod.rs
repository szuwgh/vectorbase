pub mod hnsw;
pub use self::hnsw::HNSW;
use space::{Metric, Neighbor};

pub trait KnnIndex<T> {
    fn insert(&mut self, k: Vec<T>, v: u64);
    fn near(&mut self, k: &Vec<T>) -> Vec<Neighbor<u64>>;
}
