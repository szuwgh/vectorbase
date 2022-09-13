use crate::schema::field::Value;
use crate::schema::Document;
use std::collections::HashMap;
use std::default;
pub(crate) trait TextIndex {
    fn insert(&mut self, k: Vec<u8>, v: u64);
}

pub(crate) trait VectorIndex {
    fn insert(&mut self, k: &[f32], v: u64);
    fn near(&self, k: &[f32]) {}
}

#[derive(Default)]
pub(crate) struct MemTable {
    indexs: HashMap<String, Box<dyn TextIndex + 'static>>,
    vectors: HashMap<String, Box<dyn VectorIndex + 'static>>,
    doc_id: u64,
}

impl MemTable {
    pub(crate) fn index_document(&mut self, doc: &Document) {
        for field in doc.fields.iter() {
            match &field.value {
                Value::Vector(v) => {
                    //  let index = self.vectors.get(&field.name).get_or_insert(value);
                }
                _ => {}
            };
        }
    }
}

struct HNSW {}

#[cfg(test)]
mod tests {
    use super::*;
    use hnsw::{Hnsw, Searcher};
    use rand_pcg::Pcg64;
    use space::{Metric, Neighbor};
    use std::fs::File;
    use std::io::BufReader;

    struct Euclidean;

    impl Metric<&[f64]> for Euclidean {
        type Unit = u64;
        fn distance(&self, a: &&[f64], b: &&[f64]) -> u64 {
            a.iter()
                .zip(b.iter())
                .map(|(&a, &b)| (a - b).powi(2))
                .sum::<f64>()
                .sqrt()
                .to_bits()
        }
    }

    fn test_hnsw() -> (
        Hnsw<Euclidean, &'static [f64], Pcg64, 12, 24>,
        Searcher<u64>,
    ) {
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
            hnsw.insert(feature, &mut searcher);
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

        let n = hnsw.nearest(&&[0.0, 1.0, 0.0, 0.0][..], 24, searcher, &mut neighbors);
        println!("{:?}", n);
    }
}
