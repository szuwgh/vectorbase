pub mod annoy;
pub mod hnsw;
pub use self::hnsw::HNSW;
use super::schema::BinarySerialize;
use super::schema::DocID;
use super::util::error::GyResult;
use crate::disk::GyRead;
use crate::disk::GyWrite;
use crate::TensorEntry;
use crate::VectorSerialize;
use byteorder::LittleEndian;
use core::cmp::Ordering;
use wwml::similarity::TensorSimilar;
use roaring::RoaringBitmap;
use serde::Deserialize;
use serde::Serialize;
use std::collections::HashSet;
use std::io::{Read, Write};

type Endian = LittleEndian;

pub(crate) struct Emptyer;

impl AnnPrioritizer for Emptyer {
    fn contains(&self, _id: usize) -> bool {
        true
    }
}

impl AnnFilter for Emptyer {
    fn filter(&self, _id: usize) -> bool {
        true
    }
}

pub trait AnnPrioritizer {
    fn contains(&self, id: usize) -> bool;
}

pub trait AnnFilter {
    fn filter(&self, id: usize) -> bool;
}

impl AnnPrioritizer for HashSet<usize> {
    fn contains(&self, id: usize) -> bool {
        self.contains(&id)
    }
}

impl AnnFilter for HashSet<usize> {
    fn filter(&self, id: usize) -> bool {
        self.contains(&id)
    }
}

impl AnnPrioritizer for RoaringBitmap {
    fn contains(&self, id: usize) -> bool {
        self.contains(id as u32)
    }
}

impl AnnFilter for RoaringBitmap {
    fn filter(&self, id: usize) -> bool {
        self.contains(id as u32)
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, Default)]
#[repr(usize)]
pub enum AnnType {
    #[default]
    HNSW = 1,
}

impl AnnType {
    fn to_usize(&self) -> usize {
        *self as usize
    }

    fn from_usize(u: usize) -> Self {
        match u {
            1 => AnnType::HNSW,
            _ => todo!(),
        }
    }
}

#[derive(Debug)]
pub enum Ann<V: VectorSerialize + TensorSimilar + Clone> {
    HNSW(HNSW<V>),
}

impl<V: VectorSerialize + TensorSimilar + Clone> VectorSerialize for Ann<V> {
    fn vector_deserialize<R: Read + GyRead>(reader: &mut R, entry: &TensorEntry) -> GyResult<Self> {
        let n = usize::binary_deserialize(reader)?;
        let ann_type = AnnType::from_usize(n);
        match ann_type {
            AnnType::HNSW => Ok(Ann::HNSW(HNSW::<V>::vector_deserialize(reader, entry)?)),
            _ => todo!(),
        }
    }
    fn vector_serialize<W: Write + GyWrite>(&self, writer: &mut W) -> GyResult<()> {
        match self {
            Ann::HNSW(v) => {
                AnnType::HNSW.to_usize().binary_serialize(writer)?;
                v.vector_serialize(writer)
            }
            _ => todo!(),
        }
    }

    fn vector_nommap_deserialize<R: Read + GyRead>(
        reader: &mut R,
        entry: &TensorEntry,
    ) -> GyResult<Self> {
        todo!()
    }
}

impl<V: VectorSerialize + TensorSimilar + Clone> Ann<V> {
    pub fn insert(&mut self, q: V) -> GyResult<usize>
    where
        V: Metric<V>,
    {
        match self {
            Ann::HNSW(v) => v.insert(q),
            _ => todo!(),
        }
    }

    pub fn query<T: TensorSimilar, P: AnnPrioritizer, F: AnnFilter>(
        &self,
        q: &T,
        k: usize,
        prioritizer: Option<P>,
        filter: Option<F>,
    ) -> GyResult<Vec<Neighbor>>
    where
        V: Metric<T>,
    {
        match self {
            Ann::HNSW(v) => v.query(q, k, prioritizer, filter),
            _ => todo!(),
        }
    }

    pub fn merge(&self, other: &Self) -> GyResult<Ann<V>>
    where
        V: Metric<V>,
    {
        match (self, other) {
            (Ann::HNSW(a), Ann::HNSW(b)) => Ok(Ann::HNSW(a.merge(b)?)),
            _ => todo!(),
        }
    }
}

pub trait Metric<P: TensorSimilar> {
    fn distance(&self, b: &P) -> f32;
}

pub trait VectorCreate {
    fn create() -> Self;
}

#[derive(Default, Clone, Copy, PartialEq, Debug)]
pub struct Neighbor {
    id: usize,
    d: f32, //distance
}

impl Neighbor {
    pub fn doc_id(&self) -> DocID {
        self.id as DocID
    }

    pub fn distance(&self) -> f32 {
        self.d
    }
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

pub trait AnnIndex<V: VectorSerialize + TensorSimilar> {
    fn insert(&mut self, q: V) -> GyResult<usize>
    where
        V: Metric<V>;
    fn query<T: TensorSimilar, P: AnnPrioritizer, F: AnnFilter>(
        &self,
        q: &T,
        k: usize,
        prioritizer: Option<P>,
        filter: Option<F>,
    ) -> GyResult<Vec<Neighbor>>
    where
        V: Metric<T>;
}
