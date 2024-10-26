fn main() {
    println!("Hello, world!");
}
use galois::similarity::Similarity;
use galois::Tensor;
use hnsw::Hnsw;
use hnsw::Searcher;
use rand::{thread_rng, Rng};
use space::{Metric as Metric2, Neighbor};
struct Euclidean;

impl Metric2<Tensor> for Euclidean {
    type Unit = u64;
    fn distance(&self, a: &Tensor, b: &Tensor) -> u64 {
        a.euclidean(b) as u64
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    const dims: usize = 100;
    use rand_pcg::Pcg64;
    #[test]
    fn test_hnsw2_rng() {
        let size = 1000;

        // 创建随机数生成器
        let mut rng = rand::thread_rng();
        let mut array_list = Vec::with_capacity(size);
        for i in 0..size {
            let random_array2: [f32; dims] = std::array::from_fn(|_| rng.gen());
            array_list.push(random_array2);
        }

        let mut searcher = Searcher::default();
        let mut hnsw: Hnsw<Euclidean, Tensor, Pcg64, 32, 64> = Hnsw::new(Euclidean);

        for i in 0..size {
            hnsw.insert(Tensor::arr_array(array_list[i]), &mut searcher);
        }

        for i in 0..size {
            let mut neighbors = [Neighbor {
                index: !0,
                distance: !0,
            }; 5];
            let v = hnsw.nearest(
                &Tensor::arr_array(array_list[i]),
                5,
                &mut searcher,
                &mut neighbors,
            );
            if neighbors[0].distance != 0 {
                println!("neig :{}", neighbors[0].distance)
            }
        }
    }
}
