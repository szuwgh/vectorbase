use super::KnnIndex;
use hnsw::{Hnsw, Searcher};
use rand_pcg::Pcg64;
use space::{Metric, Neighbor};
use std::iter::Sum;
use std::marker::PhantomData;
use std::ops::Sub;
struct Euclidean;

impl Metric<Vec<f64>> for Euclidean {
    type Unit = u64;
    fn distance(&self, a: &Vec<f64>, b: &Vec<f64>) -> u64 {
        a.iter()
            .zip(b.iter())
            .map(|(&a1, &b1)| (a1 - b1).powi(2))
            .sum::<f64>()
            .sqrt()
            .to_bits() as u64
    }
}

pub struct HNSW<T> {
    hnsw: Hnsw<Euclidean, Vec<T>, Pcg64, 12, 24>,
    searcher: Searcher<u64>,
}

impl<T> HNSW<T> {
    pub fn new() -> HNSW<T> {
        Self {
            hnsw: Hnsw::new(Euclidean),
            searcher: Searcher::default(),
        }
    }
}

impl<T> KnnIndex<T> for HNSW<T>
where
    T: Sub + Sum,
{
    fn insert(&mut self, k: Vec<T>, v: u64) {
        self.hnsw.insert(k, &mut self.searcher);
    }

    fn near(&mut self, k: &Vec<T>) -> Vec<Neighbor<u64>> {
        let mut neighbors = [Neighbor {
            index: !0,
            distance: !0,
        }; 8];
        self.hnsw.nearest(k, 24, &mut self.searcher, &mut neighbors);
        neighbors.to_vec()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use hnsw::{Hnsw, Searcher};
    use rand_pcg::Pcg64;

    fn test_hnsw() -> (Hnsw<Euclidean, Vec<f32>, Pcg64, 12, 24>, Searcher<u64>) {
        let mut searcher = Searcher::default();
        let mut hnsw = Hnsw::new(Euclidean);

        let features = [
            &[0.0, 0.0, 0.0, 1.0],
            &[0.0, 0.0, 1.0, 0.0],
            &[0.0, 1.0, 0.0, 0.0],
            &[1.0, 0.0, 0.0, 0.0],
            &[0.0, 0.0, 1.0, 1.0],
            &[0.0, 1.0, 1.0, 0.0],
            &[1.0, 1.0, 0.0, 0.0],
            &[1.0, 0.0, 0.0, 1.0],
        ];

        for &feature in &features {
            hnsw.insert(feature.to_vec(), &mut searcher);
        }

        (hnsw, searcher)
    }

    #[test]
    fn test_hnsw_near() {
        let (hnsw, mut searcher) = test_hnsw();
        let searcher = &mut searcher;
        let mut neighbors = [Neighbor {
            index: !0,
            distance: !0,
        }; 8];
        let n = hnsw.nearest(
            &[0.0f32, 1.0, 0.0, 0.0][..].to_vec(),
            24,
            searcher,
            &mut neighbors,
        );
        println!("{:?}", n);
    }
}
