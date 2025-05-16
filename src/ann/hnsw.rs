use super::super::util::error::GyResult;
use super::{AnnFilter, AnnIndex, AnnPrioritizer, Emptyer, Metric, Neighbor};
use crate::disk::{GyRead, GyWrite};
use crate::schema::BinarySerialize;
use crate::TensorEntry;
use crate::VectorSerialize;
use rand::prelude::ThreadRng;
use rand::Rng;
use std::collections::BinaryHeap;
use std::collections::HashSet;
use wwml::similarity::TensorSimilar;

// impl Metric<Vec<f32>> for Vec<f32> {
//     fn distance(&self, b: &Vec<f32>) -> f32 {
//         self.iter()
//             .zip(b.iter())
//             .map(|(&a1, &b1)| (a1 - b1).powi(2))
//             .sum::<f32>()
//             .sqrt()
//     }
// }

const GGUF_DEFAULT_ALIGNMENT: usize = 32;

#[derive(Default, Debug, Clone)]
struct Node {
    level: usize,
    neighbors: Vec<Vec<usize>>, //layer --> vec
}

impl Node {
    fn get_neighbors(&self, level: usize) -> Option<impl Iterator<Item = usize> + '_> {
        let x = self.neighbors.get(level)?;
        Some(x.iter().cloned())
    }
}

#[derive(Debug)]
#[warn(non_snake_case)]
pub struct HNSW<V: VectorSerialize + TensorSimilar + Clone> {
    enter_point: usize,
    max_layer: usize,
    ef_construction: usize,
    M: usize,
    M0: usize,
    n_items: usize,
    rng: ThreadRng,
    level_mut: f64,
    nodes: Vec<Node>,
    vectors: Vec<V>,
    current_id: usize,
}

impl<V: VectorSerialize + TensorSimilar + Clone> VectorSerialize for HNSW<V> {
    fn vector_nommap_deserialize<R: std::io::Read + GyRead>(
        reader: &mut R,
        entry: &TensorEntry,
    ) -> GyResult<Self> {
        todo!()
    }

    fn vector_deserialize<R: std::io::Read + GyRead>(
        reader: &mut R,
        entry: &TensorEntry,
    ) -> GyResult<Self> {
        let M = usize::binary_deserialize(reader)?;
        let M0 = usize::binary_deserialize(reader)?;
        let ef_construction = usize::binary_deserialize(reader)?;
        let level_mut = f64::binary_deserialize(reader)?;
        let max_layer = usize::binary_deserialize(reader)?;
        let enter_point = usize::binary_deserialize(reader)?;
        let node_len = usize::binary_deserialize(reader)?;
        let mut nodes: Vec<Node> = Vec::with_capacity(node_len);
        for _ in 0..node_len {
            let level = usize::binary_deserialize(reader)?;
            let neighbors: Vec<Vec<usize>> = Vec::<Vec<usize>>::binary_deserialize(reader)?;
            nodes.push(Node {
                level: level,
                neighbors: neighbors,
            });
        }
        let position = reader.offset();
        let next_position = position - (position % GGUF_DEFAULT_ALIGNMENT) + GGUF_DEFAULT_ALIGNMENT;
        reader.read_bytes(next_position - position)?;
        let mut vectors: Vec<V> = Vec::with_capacity(node_len);
        for _ in 0..node_len {
            let p = V::vector_deserialize(reader, entry)?;
            vectors.push(p)
        }
        Ok(HNSW {
            enter_point: enter_point,
            max_layer: max_layer,
            ef_construction: ef_construction,
            M: M,
            M0: M0,
            n_items: 0,
            rng: rand::thread_rng(),
            level_mut: level_mut,
            nodes: nodes,
            vectors: vectors,
            current_id: 0,
        })
    }

    fn vector_serialize<W: std::io::Write + GyWrite>(&self, writer: &mut W) -> GyResult<()> {
        self.M.binary_serialize(writer)?;
        self.M0.binary_serialize(writer)?;
        self.ef_construction.binary_serialize(writer)?;
        self.level_mut.binary_serialize(writer)?;
        self.max_layer.binary_serialize(writer)?;
        self.enter_point.binary_serialize(writer)?;
        self.nodes.len().binary_serialize(writer)?;
        for n in self.nodes.iter() {
            n.level.binary_serialize(writer)?;
            n.neighbors.binary_serialize(writer)?;
        }
        let position = writer.get_pos()?;
        let next_position = position - (position % GGUF_DEFAULT_ALIGNMENT) + GGUF_DEFAULT_ALIGNMENT;
        writer.write(&vec![0u8; next_position - position])?;
        for v in self.vectors.iter() {
            v.vector_serialize(writer)?;
        }
        Ok(())
    }
}

impl<V: VectorSerialize + TensorSimilar + Clone> AnnIndex<V> for HNSW<V> {
    //插入
    fn insert(&mut self, q: V) -> GyResult<usize>
    where
        V: Metric<V>,
    {
        let cur_level: usize = self.get_random_level();
        if self.current_id == 0 {
            let new_node = Node {
                level: cur_level,
                neighbors: vec![Vec::new(); cur_level],
            };
            self.nodes.push(new_node);
            self.vectors.push(q);
            self.enter_point = self.current_id;
            self.current_id += 1;
            return Ok(self.enter_point);
        } else {
            let ep_id = self.enter_point;
            let current_max_layer = self.get_node(ep_id).level;
            let new_id = self.current_id;
            self.current_id += 1;

            //起始点
            let mut ep = Neighbor {
                id: self.enter_point,
                d: self.get_vector(ep_id).distance(&q),
            };
            let mut changed = true;
            //那么从当前图的从最高层逐层往下寻找直至节点的层数+1停止，寻找到离data_point最近的节点，作为下面一层寻找的起始点
            for level in (cur_level..current_max_layer).rev() {
                changed = true;
                while changed {
                    changed = false;
                    for i in self.get_neighbors_nodes(ep.id, level).unwrap() {
                        let d = self.get_vector(i).distance(&q); //distance(self.get_node(ep_id).p.borrow(), &q);
                        if d < ep.d {
                            ep.id = i;
                            ep.d = d;
                            changed = true;
                        }
                    }
                }
            }

            let new_node = Node {
                level: cur_level,
                neighbors: vec![Vec::new(); cur_level],
                //p: q,
            };
            self.nodes.push(new_node);
            self.vectors.push(q);
            //从curlevel依次开始往下，每一层寻找离data_point最接近的ef_construction_（构建HNSW是可指定）个节点构成候选集
            for level in (0..core::cmp::min(cur_level, current_max_layer)).rev() {
                //在每层选择data_point最接近的ef_construction_（构建HNSW是可指定）个节点构成候选集
                let candidates = self.search_at_layer::<V, Emptyer, Emptyer>(
                    self.get_vector(new_id),
                    ep,
                    level,
                    &None,
                    &None,
                );
                //连接邻居?
                self.connect_neighbor(new_id, candidates, level);
            }
            self.n_items += 1;

            if cur_level > self.max_layer {
                self.max_layer = cur_level;
                self.enter_point = new_id;
            }
            return Ok(new_id);
        }

        //  new_id
    }

    fn query<T: TensorSimilar, P: AnnPrioritizer, F: AnnFilter>(
        &self,
        q: &T,
        K: usize,
        prioritizer: Option<P>,
        filter: Option<F>,
    ) -> GyResult<Vec<Neighbor>>
    where
        V: Metric<T>,
    {
        if self.enter_point >= self.vectors.len() {
            return Ok(Vec::new());
        }
        let current_max_layer = self.max_layer;
        let mut ep = Neighbor {
            id: self.enter_point,
            d: self.get_vector(self.enter_point).distance(q), //distance(self.get_node(self.enter_point).p.borrow(), &q),
        };
        let mut changed = true;
        for level in (0..current_max_layer).rev() {
            changed = true;
            while changed {
                changed = false;
                if let Some(x) = self.get_neighbors_nodes(ep.id, level) {
                    for i in x {
                        let d = self.get_vector(i).distance(&q); // distance(self.get_node(self.enter_point).p.borrow(), &q);
                        if d < ep.d {
                            ep.id = i;
                            ep.d = d;
                            changed = true;
                        }
                    }
                }
            }
        }
        let mut x = self.search_at_layer(&q, ep, 0, &prioritizer, &filter);
        while x.len() > K {
            x.pop();
        }
        Ok(x.into_sorted_vec())
    }
}

impl<V: VectorSerialize + TensorSimilar + Clone> HNSW<V> {
    pub fn merge(&self, other: &Self) -> GyResult<HNSW<V>>
    where
        V: Metric<V>,
    {
        let mut new_hnsw = HNSW::<V>::new(self.M);
        for n in self.vectors.iter() {
            new_hnsw.insert(n.clone())?;
        }
        for n in other.vectors.iter() {
            new_hnsw.insert(n.clone())?;
        }
        Ok(new_hnsw)
    }

    pub fn new(M: usize) -> HNSW<V> {
        Self {
            enter_point: 0,
            max_layer: 0,
            ef_construction: 400,
            rng: rand::thread_rng(),
            level_mut: 1f64 / ((M as f64).ln()),
            nodes: Vec::with_capacity(10000),
            vectors: Vec::with_capacity(10000),
            M: M,
            M0: M * 2,
            current_id: 0,
            n_items: 1,
        }
    }

    fn print(&self) {
        for (i, x) in self.nodes.iter().enumerate() {
            println!("level:{:?},{:?}", x.level, x.neighbors);
        }
    }

    fn get_random_level(&mut self) -> usize {
        let x: f64 = self.rng.gen();
        ((-(x * self.level_mut).ln()).floor()) as usize
    }

    fn get_vector(&self, x: usize) -> &V {
        self.vectors.get(x).expect("get vector fail")
    }

    pub fn get_vectors(&self) -> &Vec<V> {
        &self.vectors
    }

    fn get_node(&self, x: usize) -> &Node {
        self.nodes.get(x).expect("get node fail")
    }

    fn get_node_mut(&mut self, x: usize) -> &mut Node {
        self.nodes.get_mut(x).expect("get mut node fail")
    }

    //连接邻居
    fn connect_neighbor(&mut self, cur_id: usize, candidates: BinaryHeap<Neighbor>, level: usize)
    where
        V: Metric<V>,
    {
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
                //将cur_id插入到 邻居的 neighbors中
                x.push(cur_id);
                x.len()
            };
            //检查每个neighbors的连接数，如果大于maxl，则需要缩减连接到最近邻的maxl个
            if l > maxl {
                let mut result_set: BinaryHeap<Neighbor> = BinaryHeap::with_capacity(maxl);

                let p = self.get_vector(n.id);
                self.get_neighbors_nodes(n.id, level)
                    .unwrap()
                    .for_each(|x| {
                        result_set.push(Neighbor {
                            id: x,
                            d: -self.get_vector(x).distance(p), //distance(p, self.get_node(x).p.borrow()),
                        });
                    });

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

    fn get_neighbors_nodes(
        &self,
        n: usize,
        level: usize,
    ) -> Option<impl Iterator<Item = usize> + '_> {
        self.get_node(n).get_neighbors(level)
    }

    // 定义一个辅助函数，根据是否在 prioritized 集合中调整距离
    fn effective_distance<T: TensorSimilar>(
        &self,
        id: usize,
        q: &T,
        prioritized: &HashSet<usize>,
    ) -> f32
    where
        V: Metric<T>,
    {
        let mut d = self.get_vector(id).distance(q);
        // 如果该 id 在优先匹配集合中，降低其距离（比如乘以 0.8）
        if prioritized.contains(&id) {
            d *= 0.8;
        }
        d
    }

    // 返回 result 从远到近
    fn search_at_layer<T: TensorSimilar, P: AnnPrioritizer, F: AnnFilter>(
        &self,
        q: &T,
        ep: Neighbor,
        level: usize,
        prioritizer: &Option<P>,
        filter: &Option<F>,
    ) -> BinaryHeap<Neighbor>
    where
        V: Metric<T>,
    {
        let mut visited_set: HashSet<usize> = HashSet::new();
        let mut candidates: BinaryHeap<Neighbor> =
            BinaryHeap::with_capacity(self.ef_construction * 3);
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
            if self.get_node(c.id).neighbors.len() < level + 1 {
                continue;
            }
            // 查询c的所有邻居e，如果e已经在visitedset中存在则跳过，不存在则加入visitedset
            // 把比d和q距离更近的e加入candidates、results中，如果results未满，
            // 则把所有的e都加入candidates、results
            // 如果results已满，则弹出和q距离最远的点
            self.get_neighbors_nodes(c.id, level)
                .unwrap()
                .for_each(|n| {
                    //如果e已经在visitedset中存在则跳过，
                    if visited_set.contains(&n) {
                        return;
                    }
                    if let Some(fil) = filter {
                        if fil.filter(n) {
                            return;
                        }
                    }

                    //不存在则加入visitedset
                    visited_set.insert(n);
                    let mut dist = self.get_vector(n).distance(q); //   distance(q, self.nodes.get(n).unwrap().p.borrow());

                    // **混合匹配逻辑：如果在 preferred_ids 中，降低它的距离**
                    if let Some(pri) = prioritizer {
                        if pri.contains(n) {
                            dist *= 0.7;
                        } // 可以调整这个权重，使其更容易进入候选队列
                    }

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

    fn get_neighbors_by_heuristic_closest_frist(&mut self, w: &mut BinaryHeap<Neighbor>, M: usize)
    where
        V: Metric<V>,
    {
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
                .map(|r| self.get_vector(r.id).distance(self.get_vector(e.id))) //distance(self.nodes[r.id].p.borrow(), &self.nodes[e.id].p)
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
    fn get_neighbors_by_heuristic(&mut self, candidates: &mut BinaryHeap<Neighbor>, M: usize)
    where
        V: Metric<V>,
    {
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
                .map(|r| self.get_vector(r.id).distance(self.get_vector(e.id))) //distance(self.nodes[r.id].p.borrow(), &self.nodes[e.id].p)
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{VectorFrom, VectorType};
    use crate::util::fs::GyFile;
    use rand::{thread_rng, Rng};
    use std::fs::File;
    use std::{collections::HashMap, io::Write};
    use wwml::Shape;
    use wwml::Tensor;
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
        // let mut hnsw = HNSW::new(32);
        // println!("{}", hnsw.level_mut);
        // for x in 0..100 {
        //     let x1 = hnsw.get_random_level();
        //     println!("x1 = {x1}");
        // }
    }

    // #[test]
    // fn test_hnsw_binary_heap() {
    //     let mut heap: BinaryHeap<Neighbor> = BinaryHeap::new();

    //     heap.push(Neighbor { id: 0, d: 10.0 });
    //     heap.push(Neighbor { id: 2, d: 9.0 });
    //     heap.push(Neighbor { id: 1, d: 15.0 });
    //     println!("{:?}", heap.into_sorted_vec()); //
    //                                               //  println!("{:?}", heap.peek()); //
    //                                               //   println!("{:?}", heap);
    //}

    // #[test]
    // fn test_hnsw_search() {
    //     let mut hnsw1 = HNSW::<Vec<f32>>::new(32);

    //     let features = [
    //         &[0.0, 0.0, 0.0, 1.0],
    //         &[0.0, 0.0, 1.0, 0.0],
    //         &[0.0, 1.0, 0.0, 0.0],
    //         &[1.0, 0.0, 0.0, 0.0],
    //     ];

    //     for &feature in &features {
    //         hnsw1.insert(feature.to_vec()).unwrap();
    //         // i += 1;
    //     }

    //     let mut hnsw2 = HNSW::<Vec<f32>>::new(32);
    //     let features = [
    //         &[0.0, 0.0, 1.0, 1.0],
    //         &[0.0, 1.0, 1.0, 0.0],
    //         &[1.0, 1.0, 0.0, 0.0],
    //         &[1.0, 0.0, 0.0, 1.0],
    //     ];

    //     for &feature in &features {
    //         hnsw2.insert(feature.to_vec()).unwrap();
    //         // i += 1;
    //     }

    //     let hnsw = hnsw1.merge(&hnsw2).unwrap();

    //     hnsw.print();

    //     let neighbors = hnsw.query(&[0.0f32, 0.0, 1.0, 0.0][..].to_vec(), 4);
    //     println!("{:?}", neighbors);
    //     let mut file = File::create("./data.hnsw").unwrap();
    //     hnsw.vector_serialize(&mut file).unwrap();
    //     file.flush();
    // }
    use wwml::Device;
    #[test]
    fn test_hnsw_tensor_search() {
        let cpu = Device::Cpu;
        let mut hnsw = HNSW::<Tensor>::new(32);

        let features = [
            Tensor::from_vec(vec![0.0f32, 0.0, 0.0, 1.0], 1, Shape::from_array([4]), &cpu).unwrap(),
            Tensor::from_vec(vec![0.0f32, 0.0, 1.0, 0.0], 1, Shape::from_array([4]), &cpu).unwrap(),
            Tensor::from_vec(vec![0.0f32, 1.0, 0.0, 0.0], 1, Shape::from_array([4]), &cpu).unwrap(),
            Tensor::from_vec(vec![1.0f32, 0.0, 0.0, 0.0], 1, Shape::from_array([4]), &cpu).unwrap(),
            Tensor::from_vec(vec![0.0f32, 0.0, 1.0, 1.0], 1, Shape::from_array([4]), &cpu).unwrap(),
            Tensor::from_vec(vec![0.0f32, 1.0, 1.0, 0.0], 1, Shape::from_array([4]), &cpu).unwrap(),
            Tensor::from_vec(vec![1.0f32, 0.0, 0.0, 1.0], 1, Shape::from_array([4]), &cpu).unwrap(),
            Tensor::from_vec(vec![1.0f32, 1.0, 0.0, 0.0], 1, Shape::from_array([4]), &cpu).unwrap(),
        ];
        for feature in features.into_iter() {
            hnsw.insert(feature);
        }
        hnsw.print();

        let mut prioritizer = HashSet::<usize>::new();
        prioritizer.insert(5);

        let mut filter = HashSet::<usize>::new();
        filter.insert(4);
        filter.insert(0);

        let neighbors = hnsw.query::<Tensor, HashSet<usize>, HashSet<usize>>(
            &Tensor::from_vec(vec![0.0f32, 0.0, 1.0, 0.0], 1, Shape::from_array([4]), &cpu)
                .unwrap(),
            4,
            Some(prioritizer),
            None,
        );
        println!("{:?}", neighbors);
        // let mut file = File::create("./data.hnsw").unwrap();
        // hnsw.vector_serialize(&mut file).unwrap();
        // file.flush().unwrap();
    }

    // const dims: usize = 100;

    // #[test]
    // fn test_hnsw_rng() {
    //     let size = 1000;

    //     // 创建随机数生成器
    //     let mut rng = rand::thread_rng();
    //     let mut array_list = Vec::with_capacity(size);
    //     for i in 0..size {
    //         let random_array2: [f32; dims] = std::array::from_fn(|_| rng.gen());
    //         array_list.push(random_array2);
    //     }

    //     let mut hnsw = HNSW::<Tensor>::new(32);

    //     for i in 0..size {
    //         hnsw.insert(Tensor::arr_array(array_list[i])).unwrap();
    //     }

    //     for i in 0..size {
    //         let neighbors = hnsw.query(&Tensor::arr_array(array_list[i]), 5).unwrap();
    //         assert!(neighbors.len() == 5);

    //         if (neighbors[0].d != 0.0) {
    //             println!("fail");
    //         }
    //     }
    // }

    // use crate::disk::MmapReader;
    // use memmap2::Mmap;
    // use std::fs::OpenOptions;

    // #[test]
    // fn test_tensor_reader() {
    //     let mut hnsw = HNSW::<Tensor>::new(32);
    //     let array_count = 10000;
    //     let array_length = 133;

    //     // 创建随机数生成器
    //     let mut rng = rand::thread_rng();
    //     let mut arrays: Vec<[f32; 133]> = Vec::with_capacity(array_count);
    //     // 填充数组
    //     for _ in 0..array_count {
    //         let mut arr = [0.0_f32; 133]; // 初始化长度为 100 的 f32 数组
    //         for i in 0..array_length {
    //             arr[i] = rng.gen::<f32>(); // 填充随机数
    //         }
    //         arrays.push(arr);
    //         hnsw.insert(Tensor::arr_array(arr)).unwrap();
    //     }

    //     let mut arr = [0.0_f32; 133]; // 初始化长度为 100 的 f32 数组
    //     for i in 0..array_length {
    //         arr[i] = rng.gen::<f32>(); // 填充随机数
    //     }

    //     // let neighbors = hnsw.query(&Tensor::arr_array(arr), 4);

    //     let mut file = File::create("./data.hnsw").unwrap();
    //     hnsw.vector_serialize(&mut file).unwrap();
    //     file.flush().unwrap();

    //     let mut file = GyFile::open("./data.hnsw").unwrap();
    //     let file_size = file.fsize().unwrap();
    //     let mmap: Mmap = unsafe { memmap2::MmapOptions::new().map(file.file()).unwrap() };
    //     let mut mmap_reader = MmapReader::new(&mmap, 0, file_size);
    //     let entry = TensorEntry::new(1, [133], VectorType::F32);
    //     let hnsw = HNSW::<Tensor>::vector_deserialize(&mut mmap_reader, &entry).unwrap();
    //     hnsw.print();
    //     for (i, v) in hnsw.vectors.iter().enumerate() {
    //         let s = unsafe { v.as_slice::<f32>() };
    //         assert!(arrays[i] == s)
    //     }
    //     println!("{:?}", "finish");

    //     // let neighbors = hnsw.query(
    //     //     &Tensor::from_vec(vec![0.0f32, 0.0, 1.0, 0.0], 1, Shape::from_array([4])),
    //     //     4,
    //     // );
    //     // println!("xxx{:?}", neighbors);
    // }
    // #[test]
    // fn test_slice() {
    //     let v = vec![0u8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 128, 63];
    //     let vv = &v[..];
    //     let len = v.len() / std::mem::size_of::<f32>();
    //     let s = unsafe { std::slice::from_raw_parts(v.as_ptr() as *const f32, len) };
    //     println!("{:?}", s);
    // }
}
