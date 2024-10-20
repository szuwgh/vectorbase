use galois::Tensor;

use super::query::{Query, Term};
use super::schema::{DocID, Document};
use super::GyResult;
use crate::disk::DiskStoreReader;
use crate::Vector;
use crate::{EngineReader, Neighbor};
use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::sync::Arc;

// 1. 定义 BlockReader trait
pub trait BlockReader {
    fn query(&self, tensor: &Tensor, k: usize) -> GyResult<Vec<Neighbor>>;
    fn vector(&self, doc_id: DocID) -> GyResult<Vector>;
}

// enum BlockQuerySet {
//     Memory(EngineReader),
//     Disk(Arc<DiskStoreReader>),
// }

struct NeighborSet {
    neighbor: Neighbor,
    i: usize,
}

pub struct VectorSet {
    v: Vector,
    d: f32,
}

impl Eq for NeighborSet {}

impl PartialEq for NeighborSet {
    fn eq(&self, other: &Self) -> bool {
        self.neighbor.distance() == other.neighbor.distance()
    }
}

impl PartialOrd for NeighborSet {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.neighbor.partial_cmp(&other.neighbor)
    }
}

impl Ord for NeighborSet {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.neighbor.cmp(&other.neighbor)
    }
}

pub struct Searcher {
    blocks: Vec<Box<dyn BlockReader>>,
}

impl Searcher {
    pub fn new(blocks: Vec<Box<dyn BlockReader>>) -> Searcher {
        Searcher { blocks: blocks }
    }

    pub(crate) fn done(self) {
        for v in self.blocks.into_iter() {
            drop(v);
        }
    }

    pub fn query(&self, tensor: &Tensor, k: usize) -> GyResult<Vec<VectorSet>> {
        let mut heap = BinaryHeap::new(); // 最大堆存储最小的K个元素
        for (i, block) in self.blocks.iter().enumerate() {
            let neighbor = block.query(tensor, k)?;

            for n in neighbor {
                heap.push(Reverse(NeighborSet { neighbor: n, i: i }));
            }
            // 如果堆中元素多于 K 个，弹出堆顶最大元素
            if heap.len() > k {
                heap.pop();
            }
        }
        // 从堆中提取元素，并转换为 Vec
        let ne_set: Vec<_> = heap
            .into_sorted_vec()
            .into_iter()
            .map(|Reverse(x)| x)
            .collect();
        let mut vec_set = Vec::with_capacity(ne_set.len());
        for n in ne_set {
            let v = self.blocks[n.i].vector(n.neighbor.doc_id())?;
            vec_set.push(VectorSet {
                v: v,
                d: n.neighbor.distance(),
            });
        }

        Ok(vec_set)
    }
}
