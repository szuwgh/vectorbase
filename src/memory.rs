use std::io::Write;

use byteorder::WriteBytesExt;

//参考 lucene 设计 缓存管理
//https://www.cnblogs.com/forfuture1978/archive/2010/02/02/1661441.html

const SIZE_CLASS: [usize; 10] = [9, 18, 24, 34, 44, 64, 84, 104, 148, 204];
const LEVEL_CLASS: [usize; 10] = [1, 2, 3, 4, 5, 6, 7, 8, 9, 9];
const BYTE_BLOCK_SIZE: usize = 1 << 15; //64 KB
const POINTER_LEN: usize = 4;

pub(super) trait TextIndex {
    fn insert(&mut self, k: Vec<u8>, v: u64);
}

pub(super) trait Block {}

pub(super) struct ByteBlockPool {
    cur_vec: usize,
    buffers: Vec<Vec<u8>>,
    pos: usize,
    used_pos: usize,
    buffer_pos: usize,
}

impl ByteBlockPool {
    pub(super) fn new() -> ByteBlockPool {
        Self {
            cur_vec: 0,
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

    fn write_vu64(&mut self, pos: usize, v: u64) -> Result<usize, std::io::Error> {
        self.pos = pos;
        self.write_vu64(pos, v)
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
}

impl Write for ByteBlockPool {
    fn write(&mut self, x: &[u8]) -> Result<usize, std::io::Error> {
        let b = self.buffers.get_mut(self.cur_vec).unwrap();
        let mut pos = self.pos;
        x.iter().for_each(|v| {
            if b[pos] == 0 {
                b[pos] = *v;
                pos += 1;
            } else {
            }
        });
        Ok(0)
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
}
