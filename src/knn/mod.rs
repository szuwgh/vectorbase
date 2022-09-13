pub mod hnsw;
pub use self::hnsw::HNSW;
use space::{Metric, Neighbor};

pub trait KnnIndex {
    fn insert(&mut self, k: Vec<f64>, v: u64);
    fn near(&mut self, k: &Vec<f64>) -> Vec<Neighbor<u64>>;
}
