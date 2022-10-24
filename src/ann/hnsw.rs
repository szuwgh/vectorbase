use core::cmp::Ordering;
use num_traits::Float;
use rand::prelude::ThreadRng;
use rand::Rng;
use std::borrow::BorrowMut;
use std::cmp::max;
use std::collections::BinaryHeap;
use std::collections::HashSet;
use std::{
    borrow::Borrow,
    f64::consts::{self, *},
};
#[derive(Default)]
struct Node {
    //id: usize,
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
    max_layer: usize,
    ef_construction: usize,
    M: usize,
    M0: usize,
    n_items: usize,
    rng: ThreadRng,
    level_mut: f64,
    nodes: Vec<Node>,
    // features: Vec<Vec<f32>>,
}

impl HNSW {
    fn new(M: usize) -> HNSW {
        Self {
            enter_point: 0,
            max_layer: 0,
            ef_construction: 400,
            rng: rand::thread_rng(),
            level_mut: 1f64 / ((M as f64).ln()),
            nodes: vec![Node {
                level: 0,
                neighbors: Vec::new(),
                p: Vec::new(),
            }],
            //features: Vec::new(),
            M: M,
            M0: M * 2,
            n_items: 0,
        }
    }

    fn print(&self) {}

    fn get_random_level(&mut self) -> usize {
        let x: f64 = self.rng.gen();
        ((-(x * self.level_mut).ln()).floor()) as usize
    }

    fn search(&self, q: &[f32], K: usize) -> Vec<Neighbor> {
        let current_max_layer = self.max_layer;
        let mut ep = Neighbor {
            id: self.enter_point,
            d: distance(self.get_node(self.enter_point).p.borrow(), &q),
        };
        let mut changed = true;
        for level in (0..current_max_layer).rev() {
            changed = true;
            while changed {
                changed = false;
                for i in self.get_neighbors_nodes(ep.id, level) {
                    let d = distance(self.get_node(self.enter_point).p.borrow(), &q);
                    if d < ep.d {
                        ep.id = i;
                        ep.d = d;
                        changed = true;
                    }
                }
            }
        }
        let mut x = self.search_at_layer(&q, ep, 0);
        while x.len() > K {
            x.pop();
        }
        x.into_sorted_vec()
    }

    //插入
    fn insert(&mut self, q: Vec<f32>) {
        let cur_level = self.get_random_level();
        let ep_id = self.enter_point;
        let current_max_layer = self.get_node(ep_id).level;
        let new_id = self.n_items;

        //起始点
        let mut ep = Neighbor {
            id: self.enter_point,
            d: distance(self.get_node(ep_id).p.borrow(), &q),
        };
        let mut changed = true;
        //那么从当前图的从最高层逐层往下寻找直至节点的层数+1停止，寻找到离data_point最近的节点，作为下面一层寻找的起始点
        for level in (cur_level..current_max_layer).rev() {
            changed = true;
            while changed {
                changed = false;
                for i in self.get_neighbors_nodes(ep.id, level) {
                    let d = distance(self.get_node(ep_id).p.borrow(), &q);
                    if d < ep.d {
                        ep.id = i;
                        ep.d = d;
                        changed = true;
                    }
                }
            }
        }

        let mut new_node = Node {
            //id: new_id,
            level: cur_level,
            neighbors: vec![Vec::new(); cur_level],
            p: q,
        };
        self.nodes.push(new_node);
        //从curlevel依次开始往下，每一层寻找离data_point最接近的ef_construction_（构建HNSW是可指定）个节点构成候选集
        for level in (0..core::cmp::min(cur_level, current_max_layer)).rev() {
            //在每层选择data_point最接近的ef_construction_（构建HNSW是可指定）个节点构成候选集
            let candidates = self.search_at_layer(&self.nodes[new_id].p, ep, level);
            //连接邻居
            self.connect_neighbor(new_id, candidates, level);
        }

        self.n_items += 1;

        if current_max_layer > self.max_layer {
            self.max_layer = current_max_layer;
            self.enter_point = new_id;
        }
    }

    fn get_node(&self, x: usize) -> &Node {
        self.nodes.get(x).expect("get node fail")
    }

    fn get_node_mut(&mut self, x: usize) -> &mut Node {
        self.nodes.get_mut(x).expect("get mut node fail")
    }

    //连接邻居
    fn connect_neighbor(&mut self, cur_id: usize, candidates: BinaryHeap<Neighbor>, level: usize) {
        let maxl = if level == 0 { self.M0 } else { self.M };
        let selected_neighbors = &mut self.get_node_mut(cur_id).neighbors[level]; //vec![0usize; candidates.len()]; // self.get_node_mut(cur_id); //vec![0usize; candidates.len()];
        let sort_neighbors = candidates.into_sorted_vec();
        for x in sort_neighbors.iter() {
            selected_neighbors.push(x.id);
        }
        selected_neighbors.reverse();
        //检查cur_id 的邻居的 邻居 是否超标
        for n in sort_neighbors.iter() {
            let l = {
                let node = self.get_node_mut(n.id);
                if node.neighbors.len() < level + 1 {
                    for _ in node.neighbors.len()..=level {
                        node.neighbors.push(Vec::with_capacity(maxl));
                    }
                }
                let x = node.neighbors.get_mut(level).unwrap();
                x.push(cur_id);
                x.len()
            };
            //检查每个neighbors的连接数，如果大于Mmax，则需要缩减连接到最近邻的Mmax个
            if l > maxl {
                let mut result_set: BinaryHeap<Neighbor> = BinaryHeap::with_capacity(maxl);
                {
                    let p = self.get_node(n.id).p.borrow();
                    for x in self.get_neighbors_nodes(n.id, level) {
                        result_set.push(Neighbor {
                            id: x,
                            d: -distance(p, self.get_node(x).p.borrow()),
                        });
                    }
                }
                self.get_neighbors_by_heuristic_closest_frist(&mut result_set, self.M);
                let neighbors = self.get_node_mut(n.id).neighbors.get_mut(level).unwrap();
                neighbors.clear();
                for x in result_set.iter() {
                    neighbors.push(x.id);
                }
                neighbors.reverse();
            }
        }
    }

    fn get_neighbors_nodes(&self, n: usize, level: usize) -> impl Iterator<Item = usize> + '_ {
        self.get_node(n).get_neighbors(level)
    }

    // 返回 result 从远到近
    fn search_at_layer(&self, q: &[f32], ep: Neighbor, level: usize) -> BinaryHeap<Neighbor> {
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
            // 从candidates中选择距离查询点最近的点c，和results中距离查询点最远的点d进行比较，
            // 如果c和查询点q的距离大于d和查询点q的距离，则结束查询
            if -c.d > d.d {
                break;
            }
            // 查询c的所有邻居e，如果e已经在visitedset中存在则跳过，不存在则加入visitedset
            // 把比d和q距离更近的e加入candidates、results中，如果results未满，
            // 则把所有的e都加入candidates、results
            // 如果results已满，则弹出和q距离最远的点
            println!("id={} , level={}", c.id, level);
            if self.get_node(c.id).neighbors.len() < level + 1 {
                continue;
            }
            self.get_neighbors_nodes(c.id, level).for_each(|n| {
                //如果e已经在visitedset中存在则跳过，
                if visited_set.contains(&n) {
                    return;
                }
                //不存在则加入visitedset
                visited_set.insert(n);
                let dist = distance(q, self.nodes.get(n).unwrap().p.borrow());
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

    fn get_neighbors_by_heuristic_closest_frist(&mut self, w: &mut BinaryHeap<Neighbor>, M: usize) {
        if w.len() <= M {
            return;
        }
        let mut temp_list: BinaryHeap<Neighbor> = BinaryHeap::with_capacity(w.len());
        let mut result: BinaryHeap<Neighbor> = BinaryHeap::new();
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
            w.push(Neighbor {
                id: item.id,
                d: -item.d,
            })
        });
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
}

fn distance(a: &[f32], b: &[f32]) -> f32 {
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
    fn test_rng() {
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
        for x in 0..100 {
            let x1 = hnsw.get_random_level();
            println!("x1 = {x1}");
        }
    }

    #[test]
    fn test_hnsw_binary_heap() {
        let mut heap: BinaryHeap<Neighbor> = BinaryHeap::new();

        heap.push(Neighbor { id: 0, d: 10.0 });
        heap.push(Neighbor { id: 2, d: 9.0 });
        heap.push(Neighbor { id: 1, d: 15.0 });
        println!("{:?}", heap.into_sorted_vec()); //
                                                  //  println!("{:?}", heap.peek()); //
                                                  //   println!("{:?}", heap);
    }

    #[test]
    fn test_hnsw_search() {
        let mut hnsw = HNSW::new(32);

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
            hnsw.insert(feature.to_vec());
        }

        let neighbors = hnsw.search(&[0.0f32, 1.0, 0.0, 0.0][..].to_vec(), 8);
        println!("{:?}", neighbors);
    }
}
