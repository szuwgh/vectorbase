use hnsw::{Hnsw, Searcher};
use num_traits::float::Float;
use rand_pcg::Pcg64;
use space::{Metric, Neighbor};
use std::iter::Sum;
pub struct Euclidean;

impl<T> Metric<Vec<T>> for Euclidean
where
    T: Float + Sum,
{
    type Unit = u64;
    fn distance(&self, a: &Vec<T>, b: &Vec<T>) -> u64 {
        a.iter()
            .zip(b.iter())
            .map(|(&a1, &b1)| (a1 - b1).powi(2))
            .sum::<T>()
            .sqrt()
            .to_u64()
            .unwrap()
    }
}

pub struct HNSW<M, T>
where
    M: Metric<T>,
{
    hnsw: Hnsw<M, T, Pcg64, 12, 24>,
    searcher: Searcher<M::Unit>,
}

impl<M, T> HNSW<M, T>
where
    M: Metric<T>,
{
    pub fn new(m: M) -> HNSW<M, T> {
        Self {
            hnsw: Hnsw::new(m),
            searcher: Searcher::default(),
        }
    }
}

impl<M, T> HNSW<M, T>
where
    M: Metric<T>,
{
    fn insert(&mut self, k: T, v: u64) {
        self.hnsw.insert(k, &mut self.searcher);
    }

    fn near(&mut self, k: &T, dest: &mut [Neighbor<M::Unit>]) {
        self.hnsw.nearest(k, 24, &mut self.searcher, dest);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use hnsw::{Hnsw, Searcher};
    use rand_pcg::Pcg64;

    #[test]
    fn test_my_hnsw() {
        let mut hnsw = HNSW::<Euclidean, Vec<f32>>::new(Euclidean);

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
            hnsw.insert(feature.to_vec(), 0);
        }
        let mut neighbors = [Neighbor {
            index: !0,
            distance: 0u64,
        }; 8];
        hnsw.near(&[0.0f32, 1.0, 0.0, 0.0][..].to_vec(), &mut neighbors);
        println!("{:?}", neighbors);
    }
}
