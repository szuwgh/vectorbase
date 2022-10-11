use num_traits::Float;
use rand::prelude::ThreadRng;
use rand::Rng;
use std::collections::HashMap;
use std::{
    borrow::Borrow,
    f64::consts::{self, *},
};

#[derive(Default)]
struct Node {
    level: usize,
    neighbors: Vec<Vec<usize>>,
    p: Vec<f32>,
}

impl Node {
    fn get_neighbors(&self, level: usize) -> impl Iterator<Item = usize> + '_ {
        self.neighbors.get(level).unwrap().iter().cloned()
    }
}

pub struct HNSW {
    enter_point: usize,
    ef_construction: usize,
    rng: ThreadRng,
    level_mut: f64,
    nodes: Vec<Node>,
    features: Vec<Vec<f32>>,
}

#[derive(Default, Clone, Copy)]
struct Neighbor {
    id: usize,
    d: f32, //distance
}

impl HNSW {
    fn new(m: u64) -> HNSW {
        Self {
            enter_point: 0,
            ef_construction: 400,
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

        //起始点
        let mut ep = Neighbor {
            id: self.enter_point,
            d: distance(self.nodes.get(ep_id).unwrap().p.borrow(), &q),
        };
        let mut changed = true;

        //那么从当前图的从最高层逐层往下寻找直至节点的层数+1停止，寻找到离data_point最近的节点，作为下面一层寻找的起始点
        for level in (cur_level..current_max_layer).rev() {
            changed = true;
            while changed {
                changed = false;
                for i in self.get_neighbors_nodes(ep.id, level) {
                    let d = distance(self.nodes.get(ep_id).unwrap().p.borrow(), &q);
                    if d < ep.d {
                        ep.id = i;
                        ep.d = d;
                        changed = true;
                    }
                }
            }
        }

        //从curlevel依次开始往下，每一层寻找离data_point最接近的ef_construction_（构建HNSW是可指定）个节点构成候选集
        for level in (0..core::cmp::min(cur_level, current_max_layer)).rev() {}
    }

    fn get_neighbors_nodes(&self, n: usize, level: usize) -> impl Iterator<Item = usize> + '_ {
        let node = self.nodes.get(n).unwrap();
        node.get_neighbors(level)
    }

    fn search_at_layer(&mut self, q: Vec<f32>, ef_construction: usize, ep: Neighbor, level: usize) {
        let mut candidates: Vec<Neighbor> = Vec::new();
        let mut visited_set: HashMap<usize, usize> = HashMap::new();
        let mut results: Vec<Neighbor> = Vec::new();

        candidates.push(ep.clone());
        visited_set.insert(ep.id, ep.id);
        results.push(ep.clone());

        while candidates.len() > 0 {
            let c = candidates.pop().unwrap();
            let d = results.pop().unwrap();
            //从candidates中选择距离查询点最近的点c，和results中距离查询点最远的点d进行比较，如果c和查询点q的距离大于d和查询点q的距离，则结束查询
            if c.d > d.d {
                break;
            }
            // 查询c的所有邻居e，如果e已经在visitedset中存在则跳过，不存在则加入visitedset
            // 把比d和q距离更近的e加入candidates、results中，如果results未满，
            // 则把所有的e都加入candidates、results
            // 如果results已满，则弹出和q距离最远的点
            for n in self.get_neighbors_nodes(c.id, level) {
                if !visited_set.contains_key(&n) {
                    visited_set.insert(n, n);
                    let dist = distance(&q, self.nodes.get(n).unwrap().p.borrow());
                }
            }
        }
    }

    fn search(&mut self) {}
}

fn distance(a: &Vec<f32>, b: &Vec<f32>) -> f32 {
    a.iter()
        .zip(b.iter())
        .map(|(&a1, &b1)| (a1 - b1).powi(2))
        .sum::<f32>()
        .sqrt()
}

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
