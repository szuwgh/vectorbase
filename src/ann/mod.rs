pub mod annoy;
pub mod hnsw;
mod hnsw2;
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};

pub use self::hnsw::HNSW;
use crate::util::error::GyResult;
use std::io::{Read, Write};

type Endian = LittleEndian;

pub trait AnnIndex<T, B> {
    fn insert(&mut self, k: T, v: u64);
    fn near(&mut self, k: &T, dest: &mut [B]);
}

struct ReadDisk<R: Read> {
    r: R,
}

impl<R: Read> ReadDisk<R> {
    fn new(r: R) -> ReadDisk<R> {
        Self { r: r }
    }

    fn read_usize(&mut self) -> GyResult<usize> {
        let x = self.r.read_u32::<Endian>()?;
        Ok(x as usize)
    }

    fn read_f32(&mut self) -> GyResult<f32> {
        let x = self.r.read_f32::<Endian>()?;
        Ok(x)
    }

    fn read_f64(&mut self) -> GyResult<f64> {
        let x = self.r.read_f64::<Endian>()?;
        Ok(x)
    }

    fn read_vec_f32(&mut self) -> GyResult<Vec<f32>> {
        let l = self.read_usize()?;
        let mut x: Vec<f32> = Vec::with_capacity(l);
        for _ in 0..l {
            x.push(self.read_f32()?);
        }
        Ok(x)
    }

    fn read_vec_usize(&mut self) -> GyResult<Vec<usize>> {
        let l = self.read_usize()?;
        let mut x: Vec<usize> = Vec::with_capacity(l);
        for _ in 0..l {
            x.push(self.read_usize()?);
        }
        Ok(x)
    }
}

struct WriteDisk<T: Write> {
    w: T,
}

impl<T: Write> WriteDisk<T> {
    fn new(w: T) -> WriteDisk<T> {
        Self { w: w }
    }

    fn write_usize(&mut self, x: usize) -> GyResult<()> {
        self.w.write_u32::<Endian>(x as u32)?;
        Ok(())
    }

    fn write_f32(&mut self, x: f32) -> GyResult<()> {
        self.w.write_f32::<Endian>(x)?;
        Ok(())
    }

    fn write_f64(&mut self, x: f64) -> GyResult<()> {
        self.w.write_f64::<Endian>(x)?;
        Ok(())
    }

    fn write_vec_f32(&mut self, x: &[f32]) -> GyResult<()> {
        let l = x.len();
        self.write_usize(l)?;
        for f in x {
            self.write_f32(*f)?;
        }
        Ok(())
    }

    fn write_vec_usize(&mut self, x: &[usize]) -> GyResult<()> {
        let l = x.len();
        self.write_usize(l)?;
        for u in x {
            self.write_usize(*u)?;
        }
        Ok(())
    }

    fn flush(&mut self) -> GyResult<()> {
        self.w.flush()?;
        Ok(())
    }
}
