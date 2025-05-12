use wwml::Tensor;

use super::query::{Query, Term};
use super::schema::{DocID, Document};
use super::GyResult;
use crate::disk::{DiskPostingReader, DiskStoreReader};
use crate::schema::DocFreq;
use crate::MemPostingReader;
use crate::Neighbor;
use crate::Vector;

use std::collections::BinaryHeap;

// 1. 定义 BlockReader trait
pub trait BlockReader: Send {
    fn query(&self, tensor: &Tensor, k: usize, term: &Option<Term>) -> GyResult<Vec<Neighbor>>;
    fn search(&self, term: &Term) -> GyResult<PostingReader>;
    fn vector(&self, doc_id: DocID) -> GyResult<Vector>;
}

pub enum PostingReader {
    Mem(MemPostingReader),
    Disk(DiskPostingReader),
}

pub trait GetDocId {
    fn doc_id(&self) -> DocID;

    fn block_id(&self) -> usize;
}

#[derive(Debug)]
pub struct NeighborSet {
    neighbor: Neighbor,
    i: usize,
}

impl GetDocId for NeighborSet {
    fn doc_id(&self) -> DocID {
        self.neighbor.doc_id()
    }

    fn block_id(&self) -> usize {
        self.i
    }
}

pub struct DocumentSet {
    doc_freq: DocFreq,
    i: usize,
}

impl GetDocId for DocumentSet {
    fn doc_id(&self) -> DocID {
        self.doc_freq.doc_id()
    }
    fn block_id(&self) -> usize {
        self.i
    }
}

// impl VectorSet {
//     pub fn vector(&self) -> &Vector {
//         &self.v
//     }

//     pub fn d(&self) -> f32 {
//         self.d
//     }
// }

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

    pub fn vector<T: GetDocId>(&self, t: &T) -> GyResult<Vector> {
        Ok(self.blocks[t.block_id()].vector(t.doc_id())?)
    }

    pub fn search(&self, term: Term) -> GyResult<impl Iterator<Item = DocumentSet> + '_> {
        Ok(self.blocks.iter().enumerate().flat_map(move |(i, block)| {
            block
                .search(&term)
                .ok()
                .into_iter()
                .flat_map(move |posting| match posting {
                    PostingReader::Mem(p) => p
                        .iter()
                        .map(move |doc_freq| DocumentSet { doc_freq, i })
                        .collect::<Vec<_>>(),
                    PostingReader::Disk(p) => p
                        .iter()
                        .map(move |doc_freq| DocumentSet { doc_freq, i })
                        .collect::<Vec<_>>(),
                })
        }))
    }

    pub fn query(
        &self,
        tensor: &Tensor,
        k: usize,
        term: Option<Term>,
    ) -> GyResult<impl Iterator<Item = NeighborSet>> {
        let mut heap = BinaryHeap::new(); // 最大堆存储最小的K个元素
        for (i, block) in self.blocks.iter().enumerate() {
            let neighbor = block.query(tensor, k, &term)?;
            for n in neighbor {
                heap.push(NeighborSet { neighbor: n, i: i });
            }
            // 如果堆中元素多于 K 个，弹出堆顶最大元素
            while heap.len() > k {
                heap.pop();
            }
        }
        // 从堆中提取元素，并转换为 Vec
        Ok(heap.into_sorted_vec().into_iter().map(|x| x))
    }
}
