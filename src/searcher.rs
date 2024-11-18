use galois::Tensor;

use super::query::{Query, Term};
use super::schema::{DocID, Document};
use super::GyResult;
use crate::disk::{DiskPostingReader, DiskStoreReader};
use crate::schema::DocFreq;
use crate::MemPostingReader;
use crate::Vector;
use crate::{EngineReader, Neighbor};
use std::cmp::Reverse;

use std::collections::BinaryHeap;
use std::sync::Arc;

// 1. 定义 BlockReader trait
pub trait BlockReader {
    fn query(&self, tensor: &Tensor, k: usize, term: &Option<Term>) -> GyResult<Vec<Neighbor>>;
    fn search(&self, term: Term) -> GyResult<PostingReader>;
    fn vector(&self, doc_id: DocID) -> GyResult<Vector>;
}

pub enum PostingReader {
    Mem(MemPostingReader),
    Disk(DiskPostingReader),
}

pub struct NeighborSet {
    neighbor: Neighbor,
    i: usize,
}

pub struct VectorSet {
    v: Vector,
    d: f32,
}

impl VectorSet {
    pub fn vector(&self) -> &Vector {
        &self.v
    }

    pub fn d(&self) -> f32 {
        self.d
    }
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

    pub fn done(self) {
        for v in self.blocks.into_iter() {
            drop(v);
        }
    }

    pub fn search(&self, term: Term) -> GyResult<PostingReader> {
        todo!()
    }

    pub fn query(
        &self,
        tensor: &Tensor,
        k: usize,
        term: Option<Term>,
    ) -> GyResult<Vec<NeighborSet>> {
        let mut heap = BinaryHeap::new(); // 最大堆存储最小的K个元素
        for (i, block) in self.blocks.iter().enumerate() {
            let neighbor = block.query(tensor, k, &term)?;
            // println!("distance:{}", neighbor[0].distance());
            for n in neighbor {
                heap.push(NeighborSet { neighbor: n, i: i });
            }
            // 如果堆中元素多于 K 个，弹出堆顶最大元素
            while heap.len() > k {
                heap.pop();
            }
        }
        // 从堆中提取元素，并转换为 Vec
        let ne_set = heap.into_sorted_vec().into_iter().map(|x| x).collect();
        // let mut vec_set = Vec::with_capacity(ne_set.len());
        // for n in ne_set {
        //     let v = self.blocks[n.i].vector(n.neighbor.doc_id())?;
        //     vec_set.push(VectorSet {
        //         v: v,
        //         d: n.neighbor.distance(),
        //     });
        // }

        Ok(ne_set)
    }
}
