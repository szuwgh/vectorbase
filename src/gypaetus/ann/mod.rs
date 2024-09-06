pub mod annoy;
pub mod hnsw;
pub use self::hnsw::HNSW;
use super::schema::BinarySerialize;
use super::util::error::GyResult;
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use core::cmp::Ordering;
use std::io::{Read, Write};
use std::sync::Arc;
type Endian = LittleEndian;

pub enum AnnType {
    HNSW,
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

pub struct BoxedAnnIndex<V: BinarySerialize>(pub Box<dyn AnnIndex<V>>);

impl<V: BinarySerialize> BoxedAnnIndex<V>
where
    V: Metric<V>,
{
    pub fn insert(&mut self, q: V) -> GyResult<usize> {
        self.0.insert(q)
    }
    pub fn query(&self, q: &V, k: usize) -> GyResult<Vec<Neighbor>> {
        self.0.query(q, k)
    }
}

pub trait AnnIndex<V: BinarySerialize>
where
    V: Metric<V>,
{
    const AnnType: t;
    fn insert(&mut self, q: V) -> GyResult<usize>;
    fn query(&self, q: &V, k: usize) -> GyResult<Vec<Neighbor>>;
}

// struct ReadDisk<R: Read> {
//     r: R,
// }

// impl<R: Read> ReadDisk<R> {
//     fn new(r: R) -> ReadDisk<R> {
//         Self { r: r }
//     }

//     fn read_usize(&mut self) -> GyResult<usize> {
//         let x = self.r.read_u32::<Endian>()?;
//         Ok(x as usize)
//     }

//     fn read_f32(&mut self) -> GyResult<f32> {
//         let x = self.r.read_f32::<Endian>()?;
//         Ok(x)
//     }

//     fn read_f64(&mut self) -> GyResult<f64> {
//         let x = self.r.read_f64::<Endian>()?;
//         Ok(x)
//     }

//     fn read_vec_f32(&mut self) -> GyResult<Vec<f32>> {
//         let l = self.read_usize()?;
//         let mut x: Vec<f32> = Vec::with_capacity(l);
//         for _ in 0..l {
//             x.push(self.read_f32()?);
//         }
//         Ok(x)
//     }

//     fn read_vec_usize(&mut self) -> GyResult<Vec<usize>> {
//         let l = self.read_usize()?;
//         let mut x: Vec<usize> = Vec::with_capacity(l);
//         for _ in 0..l {
//             x.push(self.read_usize()?);
//         }
//         Ok(x)
//     }
// }

// struct WriteDisk<V: Write> {
//     w: V,
// }

// impl<V: Write> WriteDisk<V> {
//     fn new(w: V) -> WriteDisk<V> {
//         Self { w: w }
//     }

//     fn write_usize(&mut self, x: usize) -> GyResult<()> {
//         self.w.write_u32::<Endian>(x as u32)?;
//         Ok(())
//     }

//     fn write_f32(&mut self, x: f32) -> GyResult<()> {
//         self.w.write_f32::<Endian>(x)?;
//         Ok(())
//     }

//     fn write_f64(&mut self, x: f64) -> GyResult<()> {
//         self.w.write_f64::<Endian>(x)?;
//         Ok(())
//     }

//     fn write_vec_f32(&mut self, x: &[f32]) -> GyResult<()> {
//         let l = x.len();
//         self.write_usize(l)?;
//         for f in x {
//             self.write_f32(*f)?;
//         }
//         Ok(())
//     }

//     fn write_vec_usize(&mut self, x: &[usize]) -> GyResult<()> {
//         let l = x.len();
//         self.write_usize(l)?;
//         for u in x {
//             self.write_usize(*u)?;
//         }
//         Ok(())
//     }

//     fn flush(&mut self) -> GyResult<()> {
//         self.w.flush()?;
//         Ok(())
//     }
// }
