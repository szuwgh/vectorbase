use core::cmp::Ordering;
use num_traits::Float;
use rand::prelude::ThreadRng;
use rand::Rng;
use std::collections::BinaryHeap;
use std::collections::HashSet;
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

#[derive(Default, Clone, Copy, PartialEq, Debug)]
struct Neighbor {
    id: usize,
    d: f32, //distance
}

impl Ord for Neighbor {
    fn cmp(&self, other: &Neighbor) -> Ordering {
        other.d.partial_cmp(&self.d).unwrap()
    }
}

impl PartialOrd for Neighbor {
    fn partial_cmp(&self, other: &Neighbor) -> Option<Ordering> {
        Some(other.cmp(self))
    }
}

impl Eq for Neighbor {}

pub struct HNSW {
    enter_point: usize,
    ef_construction: usize,
    rng: ThreadRng,
    level_mut: f64,
    nodes: Vec<Node>,
    features: Vec<Vec<f32>>,
    M: usize,
}

impl HNSW {
    fn new(M: usize) -> HNSW {
        Self {
            enter_point: 0,
            ef_construction: 400,
            rng: rand::thread_rng(),
            level_mut: 1f64 / ((M as f64).ln()),
            nodes: Vec::new(),
            features: Vec::new(),
            M: M,
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

    // 检查每个neighbors的连接数，如果大于Mmax，则需要缩减连接到最近邻的Mmax个
    fn connect_neighbor(&self, cur_id: usize, mut candidates: BinaryHeap<Neighbor>, level: usize) {
        let maxl = self.M;
        let mut selected_neighbors: Vec<usize> = vec![0usize; candidates.len()];
        while let Some(n) = candidates.pop() {
            selected_neighbors.push(n.id);
        }
        selected_neighbors.reverse();
        //检查cur_id 个邻居的 邻居 是否超标
        for n in selected_neighbors.iter() {}
    }

    fn get_neighbors_nodes(&self, n: usize, level: usize) -> impl Iterator<Item = usize> + '_ {
        let node = self.nodes.get(n).unwrap();
        node.get_neighbors(level)
    }

    fn search_at_layer(
        &mut self,
        q: Vec<f32>,
        ef_construction: usize,
        ep: Neighbor,
        level: usize,
    ) -> BinaryHeap<Neighbor> {
        let mut visited_set: HashSet<usize> = HashSet::new();
        let mut candidates: BinaryHeap<Neighbor> = BinaryHeap::new();
        let mut results: BinaryHeap<Neighbor> = BinaryHeap::new();

        candidates.push(Neighbor {
            id: ep.id,
            d: -ep.d,
        });
        visited_set.insert(ep.id);
        results.push(ep);

        // 从candidates中选择距离查询点最近的点c
        while let Some(c) = candidates.pop() {
            let d = results.peek().unwrap();
            // 从candidates中选择距离查询点最近的点c，和results中距离查询点最远的点d进行比较，如果c和查询点q的距离大于d和查询点q的距离，则结束查询
            if -c.d > d.d {
                break;
            }
            // 查询c的所有邻居e，如果e已经在visitedset中存在则跳过，不存在则加入visitedset
            // 把比d和q距离更近的e加入candidates、results中，如果results未满，
            // 则把所有的e都加入candidates、results
            // 如果results已满，则弹出和q距离最远的点
            self.get_neighbors_nodes(c.id, level).for_each(|n| {
                //如果e已经在visitedset中存在则跳过，
                if visited_set.contains(&n) {
                    return;
                }
                //不存在则加入visitedset
                visited_set.insert(n);
                let dist = distance(&q, self.nodes.get(n).unwrap().p.borrow());
                let top_d = results.peek().unwrap();
                //如果results未满，则把所有的e都加入candidates、results

                if results.len() < self.ef_construction {
                    results.push(Neighbor { id: n, d: dist });
                    candidates.push(Neighbor { id: n, d: -dist });
                } else if dist < top_d.d {
                    // 如果results已满，则弹出和q距离最远的点
                    results.pop();
                    results.push(Neighbor { id: n, d: dist });
                    candidates.push(Neighbor { id: n, d: -dist });
                }
            });
        }
        results
    }

    // 探索式寻找最近邻
    // 在W中选择q最近邻的M个点作为neighbors双向连接起来 启发式算法
    // https://www.ryanligod.com/2019/07/23/2019-07-23%20%E5%85%B3%E4%BA%8E%20HNSW%20%E5%90%AF%E5%8F%91%E5%BC%8F%E7%AE%97%E6%B3%95%E7%9A%84%E4%B8%80%E4%BA%9B%E7%9C%8B%E6%B3%95/
    // 启发式选择的目的不是为了解决图的全局连通性，而是为了有一条“高速公路”可以到另一个区域
    // 候选元素队列不为空且结果数量少于M时，在W中选择q最近邻e
    // 如果e和q的距离比e和R中的其中一个元素的距离更小，就把e加入到R中，否则就把e加入Wd（丢弃）
    fn get_neighbors_by_heuristic(&mut self, candidates: &mut BinaryHeap<Neighbor>, M: usize) {
        if candidates.len() <= M {
            return;
        }
        let mut temp_list: BinaryHeap<Neighbor> = BinaryHeap::with_capacity(candidates.len());
        let mut result: BinaryHeap<Neighbor> = BinaryHeap::new();
        let mut w: BinaryHeap<Neighbor> = BinaryHeap::with_capacity(candidates.len());
        while let Some(e) = candidates.pop() {
            w.push(Neighbor { id: e.id, d: -e.d });
        }

        while w.len() > 0 {
            if result.len() >= M {
                break;
            }
            //从w中提取q得最近邻 e
            let e = w.pop().unwrap();
            let dist = -e.d;
            //如果e和q的距离比e和R中的其中一个元素的距离更小，就把e加入到result中
            if result
                .iter()
                .map(|r| distance(self.nodes[r.id].p.borrow(), &self.nodes[e.id].p))
                .any(|x| dist < x)
            {
                result.push(e);
            } else {
                temp_list.push(e);
            }
        }
        while result.len() < M {
            if let Some(e) = temp_list.pop() {
                result.push(e);
            } else {
                break;
            }
        }
        result.iter().for_each(|item| {
            candidates.push(Neighbor {
                id: item.id,
                d: -item.d,
            })
        });
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

    #[test]
    fn test_hnsw_binary_heap() {
        let mut heap: BinaryHeap<Neighbor> = BinaryHeap::new();

        heap.push(Neighbor { id: 0, d: 10.0 });
        heap.push(Neighbor { id: 2, d: 9.0 });
        heap.push(Neighbor { id: 1, d: 15.0 });
        println!("{:?}", heap.peek()); //
        println!("{:?}", heap);
    }
}