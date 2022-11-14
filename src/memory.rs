use std::io::Write;

use varintrs::{Binary, WriteBytesVarExt};

//参考 lucene 设计 缓存管理
//https://www.cnblogs.com/forfuture1978/archive/2010/02/02/1661441.html

//const SIZE_CLASS: [usize; 10] = [9, 18, 24, 34, 44, 64, 84, 104, 148, 204];
const SIZE_CLASS: [usize; 10] = [9, 9, 9, 9, 9, 9, 9, 9, 9, 9];
const LEVEL_CLASS: [usize; 10] = [1, 2, 3, 4, 5, 6, 7, 8, 9, 9];
const BYTE_BLOCK_SIZE: usize = 32; //1 << 15; //64 KB
const POINTER_LEN: usize = 4;

pub(super) trait TextIndex {
    fn insert(&mut self, k: Vec<u8>, v: u64);
}

pub(super) trait Block {}

pub(super) struct ByteBlockPool {
    buffers: Vec<Vec<u8>>,
    pos: usize,
    used_pos: usize,
    buffer_pos: usize,
}

impl ByteBlockPool {
    pub(super) fn new() -> ByteBlockPool {
        Self {
            buffers: Vec::new(),
            pos: 0,
            used_pos: 0,
            buffer_pos: 0,
        }
    }

    fn write_u8(&mut self, pos: usize, v: u8) -> Result<usize, std::io::Error> {
        self.pos = pos;
        self.write(&[v])
    }

    fn write_vusize(&mut self, pos: usize, v: usize) -> Result<usize, std::io::Error> {
        self.pos = pos;
        self.write_vu64::<Binary>(v as u64)?;
        Ok(self.pos)
    }

    fn write_u64(&mut self, pos: usize, v: u64) -> Result<usize, std::io::Error> {
        self.pos = pos;
        self.write_vu64::<Binary>(v)?;
        Ok(self.pos)
        // self.write_v
        // self.write_vu64(pos, v)
    }

    fn new_bytes(&mut self, size: usize) -> usize {
        if self.pos >= BYTE_BLOCK_SIZE - size {
            self.expand_buffer();
        }
        let buffer_pos = self.buffer_pos;
        self.buffer_pos += size;
        buffer_pos + self.used_pos
    }

    fn alloc_bytes(&mut self, slice: &mut [u8], size: u8) -> usize {
        let level = slice[size as usize] & 15u8;
        let new_level = LEVEL_CLASS[level as usize];
        let new_size = SIZE_CLASS[new_level];
        let pos = self.new_bytes(new_size);
        let buf = self.buffers.last_mut().unwrap();
        buf[self.buffer_pos - POINTER_LEN] = (16 | new_level) as u8;
        0
    }

    fn expand_buffer(&mut self) {
        let v = vec![0u8; BYTE_BLOCK_SIZE];
        self.buffers.push(v);
        self.buffer_pos = 0;
    }

    fn get_pos(pos: usize) -> PosTuple {
        let m = pos / BYTE_BLOCK_SIZE;
        let n = pos & (BYTE_BLOCK_SIZE - 1);
        return PosTuple(m, n);
    }

    //  fn write_u8(&mut self, x: u8) {}
}

struct PosTuple(usize, usize);

impl Write for ByteBlockPool {
    fn write(&mut self, mut x: &[u8]) -> Result<usize, std::io::Error> {
        let total = x.len();
        while x.len() > 0 {
            let pos_tuple = Self::get_pos(self.pos);
            let (i, level) = {
                let b = self.buffers.get_mut(pos_tuple.0).unwrap();

                let i = x
                    .iter()
                    .enumerate()
                    .filter(|(i, v)| {
                        if b[pos_tuple.1 + *i] == 0 {
                            b[pos_tuple.1 + *i] = **v;
                            return false;
                        }
                        true
                    })
                    .last();
                if i.is_none() {
                    self.pos += total;
                    return Ok(total);
                }
                let size = b[pos_tuple.1];
                let slice = &mut b[pos_tuple.1..pos_tuple.1 + 4];
                let level = slice[size as usize] & 15u8;
                (i.unwrap().0, level)
            };
            //申请新的内存块
            let pos = {
                let new_level = LEVEL_CLASS[level as usize];
                let new_size = SIZE_CLASS[new_level];
                let pos = self.new_bytes(new_size);
                let buf = self.buffers.last_mut().unwrap();
                buf[self.buffer_pos - POINTER_LEN] = (16 | new_level) as u8; //写入新的内存块边界
                pos
            };

            //在上一个内存块中最后四个byte 写入下一个内存块的地址
            {
                let b = self.buffers.get_mut(pos_tuple.0).unwrap();
                let slice = &mut b[pos_tuple.1 + i..pos_tuple.1 + i + 4];
                for i in 0..slice.len() {
                    slice[i] = (pos >> (8 * (3 - i)) as usize) as u8;
                }
            }
            self.pos = pos;
            x = &x[i..];
        }
        Ok(total)
    }

    fn flush(&mut self) -> Result<(), std::io::Error> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_level() {
        let mut up: u8 = 16 | 0;
        for x in 1..20 {
            let level = up & 15;
            println!("level:{}", level);
            let newLevel = LEVEL_CLASS[level as usize];
            println!("newLevel:{}", newLevel);
            up = (16 | newLevel) as u8;
            println!("up:{}", up);
        }
    }

    #[test]
    fn test_iter() {
        let x = [1, 0, 0, 0, 0, 0];
        let i = x
            .iter()
            .enumerate()
            .filter(|(i, v)| {
                if **v == 0 {
                    return false;
                }
                true
            })
            .last();
        println!("{:?}", i);
    }

    #[test]
    fn test_xxx() {
        let x = [1, 0, 0, 0, 0, 0];
        let i = x
            .iter()
            .enumerate()
            .filter(|(i, v)| {
                if **v == 0 {
                    return false;
                }
                true
            })
            .last();
        println!("{:?}", i);
    }
}
