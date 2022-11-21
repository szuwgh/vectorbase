use std::io::{Read, Write};

use std::cell::RefCell;
use std::sync::Weak;
use varintrs::{Binary, WriteBytesVarExt};

//参考 lucene 设计 缓存管理
//https://www.cnblogs.com/forfuture1978/archive/2010/02/02/1661441.html

//const SIZE_CLASS: [usize; 10] = [9, 18, 24, 34, 44, 64, 84, 104, 148, 204];
const SIZE_CLASS: [usize; 10] = [9, 10, 11, 12, 13, 14, 15, 16, 17, 18];
const LEVEL_CLASS: [usize; 10] = [1, 2, 3, 4, 5, 6, 7, 8, 9, 9];
const BYTE_BLOCK_SIZE: usize = 32; //1 << 15; //64 KB
const POINTER_LEN: usize = 4;

pub(super) trait TextIndex {
    fn insert(&mut self, k: Vec<u8>, v: u64);
}

pub(super) trait Block {}

pub(super) struct ByteBlockPool {
    pub(super) buffers: Vec<Vec<u8>>,
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

    fn write_array(&mut self, pos: usize, v: &[u8]) -> Result<usize, std::io::Error> {
        self.pos = pos;
        self.write(v)
    }

    fn write_vusize(&mut self, pos: usize, v: usize) -> Result<usize, std::io::Error> {
        self.pos = pos;
        self.write_vu64::<Binary>(v as u64)?;
        Ok(self.pos)
    }

    pub(super) fn write_u64(&mut self, pos: usize, v: u64) -> Result<usize, std::io::Error> {
        self.pos = pos;
        self.write_vu64::<Binary>(v)?;
        Ok(self.pos)
    }

    fn next_bytes(&mut self, cur_level: usize, last: Option<PosTuple>) -> usize {
        let next_level = LEVEL_CLASS[cur_level as usize];
        self.alloc_bytes(next_level, last)
    }

    pub(super) fn new_bytes(&mut self, size: usize) -> usize {
        if self.buffers.is_empty() {
            self.expand_buffer();
        }
        if self.pos >= BYTE_BLOCK_SIZE - size {
            self.expand_buffer();
        }
        let buffer_pos = self.buffer_pos;
        self.buffer_pos += size;
        buffer_pos + self.used_pos
    }

    fn alloc_bytes(&mut self, next_level: usize, last: Option<PosTuple>) -> usize {
        //申请新的内存块
        let new_size = SIZE_CLASS[next_level];
        let pos = self.new_bytes(new_size);
        let buf = self.buffers.last_mut().unwrap();
        buf[self.buffer_pos - POINTER_LEN] = (16 | next_level) as u8; //写入新的内存块边界

        if let Some(last_pos) = last {
            //在上一个内存块中最后四个byte 写入下一个内存块的地址
            let b = self.buffers.get_mut(last_pos.0).unwrap();
            let slice = &mut b[last_pos.1..last_pos.1 + 4];
            for i in 0..slice.len() {
                slice[i] = (pos >> (8 * (3 - i)) as usize) as u8;
            }
        }
        // 返回申请的内存块首地址
        pos
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
}

struct PosTuple(usize, usize);

impl Write for ByteBlockPool {
    // 在 byteblockpool 写入 [u8]
    // 当内存块不足时将申请新得内存块
    fn write(&mut self, mut x: &[u8]) -> Result<usize, std::io::Error> {
        let total = x.len();
        while x.len() > 0 {
            let mut pos_tuple = Self::get_pos(self.pos);
            let (i, cur_level) = {
                let b = self.buffers.get_mut(pos_tuple.0).unwrap();
                let i = x.iter().enumerate().find(|(i, v)| {
                    if b[pos_tuple.1 + *i] == 0 {
                        // 在buffer数组中写入数据
                        b[pos_tuple.1 + *i] = **v;
                        return false;
                    }
                    true
                });
                if i.is_none() {
                    self.pos += total;
                    return Ok(total);
                }
                let i = i.unwrap().0;
                pos_tuple.1 += i;
                let level = b[pos_tuple.1] & 15u8;
                (i, level)
            };
            //申请新的内存块
            self.pos = self.next_bytes(cur_level as usize, Some(pos_tuple));
            x = &x[i..];
        }
        Ok(total)
    }

    fn flush(&mut self) -> Result<(), std::io::Error> {
        Ok(())
    }
}

pub(super) struct ByteBlockReader {
    pool: Weak<ByteBlockPool>,
    start_pos: usize,
    end_pos: usize,
    limit: usize,
}

impl ByteBlockReader {
    fn new(pool: Weak<ByteBlockPool>, start_pos: usize, end_pos: usize) -> ByteBlockReader {
        let mut reader = Self {
            pool: pool,
            start_pos: start_pos,
            end_pos: end_pos,
            limit: 0,
        };
        reader.limit = SIZE_CLASS[0] - POINTER_LEN;
        reader
    }
}

impl Read for ByteBlockReader {
    fn read(&mut self, x: &mut [u8]) -> Result<usize, std::io::Error> {
        // self.read_exact(buf)
        todo!()
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
        let x = [0, 0, 0, 1, 0, 0];
        let i = x.iter().enumerate().find(|(i, v)| {
            println!("{}", **v);
            if **v != 0 {
                return true;
            }
            false
        });
        println!("{:?}", i);
    }

    #[test]
    fn test_write() {
        let mut b = ByteBlockPool::new();
        let x: [u8; 12] = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12];
        let pos = b.alloc_bytes(0, None);
        b.write_array(pos, &x).unwrap();
        println!("{:?}", b.buffers);
    }
}
