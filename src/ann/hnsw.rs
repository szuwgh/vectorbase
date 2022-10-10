use num_traits::Float;
use rand::prelude::ThreadRng;
use rand::Rng;
use std::f64::consts::{self, *};

struct Node {
    level: usize,
    friend: Vec<usize>,
    p: Vec<f32>,
}

pub struct HNSW {
    enter_point: usize,
    rng: ThreadRng,
    level_mut: f64,
    nodes: Vec<Node>,
    features: Vec<Vec<f32>>,
}

struct Neighbor {
    id: usize,
    d: f32, //distance
}

impl HNSW {
    fn new(m: u64) -> HNSW {
        Self {
            enter_point: 0,
            rng: rand::thread_rng(),
            level_mut: 1f64 / ((m as f64).ln()),
            nodes: Vec::new(),
            features: Vec::new(),
        }
    }

    fn get_random_level(&mut self) -> usize {
        let x: f64 = self.rng.gen();
        ((-(x * self.level_mut).ln()).floor()) as usize
    }

    fn insert(&mut self, q: Vec<f32>, id: u32) {
        let cur_level = self.get_random_level();
        let ep_id = self.enter_point;
        let current_max_layer = self.nodes.get(ep_id).unwrap().level;
        let new_id = id;
        let ep = Neighbor {
            id: self.enter_point,
            d: 0,
        };
        for level in (cur_level..current_max_layer).rev() {}
    }

    fn search_at_layer(q: Vec<f32>, level: u32) {
        let candidates: Vec<Neighbor> = Vec::new();
    }

    fn search() {}
}

fn distance() {}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::{thread_rng, Rng};

    #[test]
    fn test_hnsw() {
        let mut rng = rand::thread_rng();
        let x: f64 = rng.gen();
        println!("x = {x}");
        let x: f64 = rng.gen();
        println!("x = {x}");
        let x: f64 = rng.gen();
        println!("x = {x}");
    }

    #[test]
    fn test_hnsw_rand_level() {
        let mut hnsw = HNSW::new(32);
        println!("{}", hnsw.level_mut);
        for _ in 0..100000 {
            let x1 = hnsw.get_random_level();
        }
    }

    // #[test]
    // fn test_hnsw_rand_level2() {
    //     let mut hnsw = HNSW::new(32);
    //     println!("{}", hnsw.level_mut);
    //     for _ in 0..10000000 {
    //         let x1 = hnsw.get_random_level2();
    //     }
    // }
}
