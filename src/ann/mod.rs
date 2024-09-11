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
use serde::Deserialize;
use serde::Serialize;
use std::io::{Read, Write};
type Endian = LittleEndian;

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

pub enum Ann<V: VectorSerialize + Clone> {
    HNSW(HNSW<V>),
}

impl<V: VectorSerialize + Clone> VectorSerialize for Ann<V> {
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
}

impl<V: VectorSerialize + Clone> Ann<V>
where
    V: Metric<V>,
{
    pub fn insert(&mut self, q: V) -> GyResult<usize> {
        match self {
            Ann::HNSW(v) => v.insert(q),
            _ => todo!(),
        }
    }

    pub fn query(&self, q: &V, k: usize) -> GyResult<Vec<Neighbor>> {
        match self {
            Ann::HNSW(v) => v.query(q, k),
            _ => todo!(),
        }
    }

    pub fn merge(&self, other: &Self) -> GyResult<Self> {
        match (self, other) {
            (Ann::HNSW(a), Ann::HNSW(b)) => Ok(Ann::HNSW(a.merge(b)?)),
            _ => todo!(),
        }
    }
}

pub trait Metric<P = Self> {
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

// pub struct BoxedAnnIndex<V: BinarySerialize>(pub Box<dyn AnnIndex<V>>);

// impl<V: BinarySerialize> BoxedAnnIndex<V>
// where
//     V: Metric<V>,
// {
//     pub fn insert(&mut self, q: V) -> GyResult<usize> {
//         self.0.insert(q)
//     }
//     pub fn query(&self, q: &V, k: usize) -> GyResult<Vec<Neighbor>> {
//         self.0.query(q, k)
//     }
// }

pub trait AnnIndex<V: VectorSerialize>
where
    V: Metric<V>,
{
    fn insert(&mut self, q: V) -> GyResult<usize>;
    fn query(&self, q: &V, k: usize) -> GyResult<Vec<Neighbor>>;
}
