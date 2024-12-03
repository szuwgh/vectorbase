use std::borrow::BorrowMut;
use std::io::{Read, Write};
use std::sync::atomic::AtomicUsize;
use std::sync::RwLock;

use super::util::error::GyResult;
use std::cell::RefCell;
use std::cell::UnsafeCell;
use std::sync::Arc;
use std::sync::Weak;
use varintrs::{Binary, ReadBytesVarExt, WriteBytesVarExt};
//参考 lucene 设计 缓存管理
//https://www.cnblogs.com/forfuture1978/archive/2010/02/02/1661441.html

pub(crate) const BLOCK_SIZE_CLASS: [usize; 10] = [9, 18, 24, 34, 44, 64, 84, 104, 148, 204];
const LEVEL_CLASS: [usize; 10] = [1, 2, 3, 4, 5, 6, 7, 8, 9, 9];
const BYTE_BLOCK_SIZE: usize = 32 * 1024; //32 KB
const POINTER_LEN: usize = 4;
pub(super) type Addr = usize;

pub(crate) type SafeAddr = AtomicUsize;

pub(super) trait TextIndex {
    fn insert(&mut self, k: Vec<u8>, v: u64);
}

unsafe impl Send for RingBuffer {}
unsafe impl Sync for RingBuffer {}

// 一写多读
pub(crate) struct RingBuffer(UnsafeCell<ByteBlockPool>);

impl RingBuffer {
    pub(crate) fn new() -> RingBuffer {
        RingBuffer(UnsafeCell::from(ByteBlockPool::new()))
    }

    pub(crate) fn get_borrow(&self) -> &ByteBlockPool {
        unsafe { &*self.0.get() }
    }
    pub(crate) fn get_borrow_mut(&self) -> &mut ByteBlockPool {
        unsafe { &mut *self.0.get() }
    }
    pub(crate) fn iter() {}
}

pub(crate) struct ByteBlockPool {
    pub(super) buffers: Vec<Box<[u8]>>,
    pos: Addr,
    used_pos: Addr,
    buffer_pos: Addr,
}

impl ByteBlockPool {
    pub(crate) fn new() -> ByteBlockPool {
        Self {
            buffers: Vec::with_capacity(128),
            pos: 0,
            used_pos: 0,
            buffer_pos: 0,
        }
    }

    fn write_u8(&mut self, pos: Addr, v: u8) -> GyResult<Addr> {
        self.pos = pos;
        let x = self.write(&[v])?;
        Ok(x)
    }

    pub(crate) fn set_pos(&mut self, pos: Addr) {
        self.pos = pos;
    }

    pub(crate) fn get_pos(&self) -> Addr {
        self.pos
    }

    pub(super) fn write_array(&mut self, pos: Addr, v: &[u8]) -> Result<Addr, std::io::Error> {
        self.pos = pos;
        self.write(v)?;
        Ok(self.pos)
    }

    pub(super) fn write_vusize(&mut self, pos: Addr, v: usize) -> Result<Addr, std::io::Error> {
        self.pos = pos;
        self.write_vu64::<Binary>(v as u64)?;
        Ok(self.pos)
    }

    pub(super) fn write_vu32(&mut self, pos: Addr, v: u32) -> Result<Addr, std::io::Error> {
        self.pos = pos;
        self.write_vu64::<Binary>(v as u64)?;
        Ok(self.pos)
    }

    pub(super) fn write_var_u64(&mut self, pos: Addr, v: u64) -> Result<Addr, std::io::Error> {
        self.pos = pos;
        self.write_vu64::<Binary>(v)?;
        Ok(self.pos)
    }

    fn next_bytes(&mut self, cur_level: Addr, last: Option<PosTuple>) -> Addr {
        let next_level = LEVEL_CLASS[cur_level];
        self.alloc_bytes(next_level, last)
    }

    fn get_bytes(&self, start_addr: Addr, limit: Addr) -> &[u8] {
        let pos_tuple = Self::get_pos_tuple(start_addr);
        &self
            .buffers
            .get(pos_tuple.0)
            .expect("buffer block out of bounds")[pos_tuple.1..pos_tuple.1 + limit]
    }

    fn get_bytes_mut(&mut self, start_addr: Addr, limit: usize) -> &mut [u8] {
        let pos_tuple = Self::get_pos_tuple(start_addr);
        &mut self
            .buffers
            .get_mut(pos_tuple.0)
            .expect("buffer block out of bounds")[pos_tuple.1..pos_tuple.1 + limit]
    }

    fn get_next_addr(&self, limit: Addr) -> Addr {
        let pos_tuple = Self::get_pos_tuple(limit);
        let b = self.buffers.get(pos_tuple.0).unwrap();
        let next_addr = (((b[pos_tuple.1]) as Addr & 0xff) << 24)
            + (((b[pos_tuple.1 + 1]) as Addr & 0xff) << 16)
            + (((b[pos_tuple.1 + 2]) as Addr & 0xff) << 8)
            + ((b[pos_tuple.1 + 3]) as Addr & 0xff);
        next_addr
    }

    pub(super) fn new_bytes(&mut self, size: usize) -> Addr {
        if self.buffers.is_empty() {
            self.expand_buffer();
        }
        if self.buffer_pos + size > BYTE_BLOCK_SIZE {
            self.expand_buffer();
        }
        let buffer_pos = self.buffer_pos;
        self.buffer_pos += size;
        buffer_pos + self.used_pos
    }

    pub(super) fn alloc_bytes(&mut self, next_level: usize, last: Option<PosTuple>) -> Addr {
        //申请新的内存块
        let new_size = BLOCK_SIZE_CLASS[next_level];
        let pos = self.new_bytes(new_size);
        let buf = self.buffers.last_mut().unwrap();

        buf[self.buffer_pos - POINTER_LEN] = (16 | next_level) as u8; //写入新的内存块边界

        if let Some(last_pos) = last {
            //在上一个内存块中最后四个byte 写入下一个内存块的地址
            let b = self.buffers.get_mut(last_pos.0).unwrap();
            let slice = &mut b[last_pos.1..last_pos.1 + POINTER_LEN];
            for i in 0..slice.len() {
                slice[i] = (pos >> (8 * (3 - i)) as usize) as u8;
            }
        }
        // 返回申请的内存块首地址
        pos
    }

    fn expand_buffer(&mut self) {
        let v = vec![0u8; BYTE_BLOCK_SIZE];
        self.buffers.push(v.into_boxed_slice());
        self.buffer_pos = 0;
        if self.buffers.len() > 1 {
            self.used_pos += BYTE_BLOCK_SIZE;
        }
    }

    fn get_pos_tuple(pos: Addr) -> PosTuple {
        let m = pos / BYTE_BLOCK_SIZE;
        let n = pos & (BYTE_BLOCK_SIZE - 1);
        return PosTuple(m, n);
    }
}

pub(super) struct PosTuple(usize, usize);

impl Write for ByteBlockPool {
    // 在 byteblockpool 写入 [u8]
    // 当内存块不足时将申请新得内存块
    fn write(&mut self, mut x: &[u8]) -> Result<usize, std::io::Error> {
        let total = x.len();
        while x.len() > 0 {
            let mut pos_tuple = Self::get_pos_tuple(self.pos);
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
                    self.pos += x.len();
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

pub(crate) struct RingBufferReader {
    pool: Arc<RingBuffer>,
    start_addr: Addr,
    end_addr: Addr,
}

impl RingBufferReader {
    pub(crate) fn new(pool: Arc<RingBuffer>, start_addr: Addr, end_addr: Addr) -> RingBufferReader {
        let reader = Self {
            pool: pool,
            start_addr: start_addr,
            end_addr: end_addr,
        };
        reader
    }

    pub(crate) fn iter<'a>(&'a self) -> RingBufferReaderIter<'a> {
        RingBufferReaderIter::new(&self.pool, self.start_addr, self.end_addr)
    }
}

pub(crate) struct RingBufferReaderIter<'a> {
    pool: &'a RingBuffer,
    start_addr: Addr,
    end_addr: Addr,
    limit: usize, //获取每一个块能读取的长度限制
    level: usize,
    eof: bool,
    first: bool,
}

impl<'a> Iterator for RingBufferReaderIter<'a> {
    type Item = &'a [u8];
    fn next(&mut self) -> Option<Self::Item> {
        if self.eof {
            return None;
        }
        match self.next_block() {
            Ok(m) => Some(m),
            Err(_) => None,
        }
    }
}

impl<'a> RingBufferReaderIter<'a> {
    pub(crate) fn new(
        pool: &'a RingBuffer,
        start_addr: Addr,
        end_addr: Addr,
    ) -> RingBufferReaderIter<'a> {
        let reader = Self {
            pool: pool,
            start_addr: start_addr,
            end_addr: end_addr,
            limit: 0,
            level: 0,
            eof: false,
            first: true,
        };
        reader
    }

    pub(crate) fn next_block(&mut self) -> Result<&'a [u8], std::io::Error> {
        if self.eof {
            return Err(std::io::Error::from(std::io::ErrorKind::UnexpectedEof));
        }
        if self.first {
            self.limit =
                if self.start_addr + BLOCK_SIZE_CLASS[self.level] - POINTER_LEN >= self.end_addr {
                    self.end_addr - self.start_addr
                } else {
                    BLOCK_SIZE_CLASS[self.level] - POINTER_LEN
                };
            self.first = false;
        } else {
            self.level = LEVEL_CLASS[((16 | self.level) as u8 & 15u8) as usize];
            let next_addr = self
                .pool
                .get_borrow()
                .get_next_addr(self.start_addr + self.limit);
            self.start_addr = next_addr;
            self.limit =
                if self.start_addr + BLOCK_SIZE_CLASS[self.level] - POINTER_LEN >= self.end_addr {
                    self.end_addr - self.start_addr
                } else {
                    BLOCK_SIZE_CLASS[self.level] - POINTER_LEN
                };
        }
        let b: &[u8] = self
            .pool
            .get_borrow()
            .get_bytes(self.start_addr, self.limit);
        if self.start_addr + self.limit >= self.end_addr {
            self.eof = true;
        }
        Ok(b)
    }
}

pub(crate) struct SnapshotReader {
    pub(crate) reader: RingBufferReader,
}

impl SnapshotReader {
    pub fn new(reader: RingBufferReader) -> SnapshotReader {
        Self { reader: reader }
    }

    pub fn iter<'a>(&'a self) -> SnapshotReaderIter<'a> {
        SnapshotReaderIter::new(self.reader.iter()).unwrap()
    }
}

// 快照读写
pub(crate) struct SnapshotReaderIter<'a> {
    offset: Addr,
    reader_iter: RingBufferReaderIter<'a>,
    cur_block: &'a [u8],
}

impl<'a> SnapshotReaderIter<'a> {
    pub(crate) fn new(mut iter: RingBufferReaderIter<'a>) -> GyResult<SnapshotReaderIter<'a>> {
        let block = iter
            .next()
            .ok_or(std::io::Error::from(std::io::ErrorKind::UnexpectedEof))?;
        Ok(Self {
            offset: 0,
            reader_iter: iter,
            cur_block: block,
        })
    }
}

impl<'a> Iterator for SnapshotReaderIter<'a> {
    type Item = u64;
    fn next(&mut self) -> Option<Self::Item> {
        let (i, x) = self.read_vu64::<Binary>();
        if x == 0 {
            return None;
        }
        Some(i)
    }
}

impl<'a> Read for SnapshotReaderIter<'a> {
    fn read(&mut self, x: &mut [u8]) -> Result<usize, std::io::Error> {
        for i in 0..x.len() {
            if self.offset == self.cur_block.len() {
                self.cur_block = self
                    .reader_iter
                    .next()
                    .ok_or(std::io::Error::from(std::io::ErrorKind::UnexpectedEof))?;
                self.offset = 0;
            }
            x[i] = self.cur_block[self.offset];
            self.offset += 1;
        }
        Ok(x.len())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const uvar_test: [u32; 19] = [
        0,
        1,
        2,
        10,
        20,
        63,
        64,
        65,
        127,
        128,
        129,
        255,
        256,
        257,
        517,
        768,
        5976,
        59767464,
        1 << 32 - 1,
    ];

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
    fn test_slice() {
        let mut slice = [0, 0, 0, 0];
        for i in 0..slice.len() {
            slice[i] = (256 >> (8 * (3 - i)) as usize) as u8;
        }
        println!("slice{:?}", slice);

        let next_addr = (((slice[0]) as Addr & 0xff) << 24)
            + (((slice[1]) as Addr & 0xff) << 16)
            + (((slice[2]) as Addr & 0xff) << 8)
            + ((slice[3]) as Addr & 0xff);
        println!("next_addr{:?}", next_addr);
    }

    #[test]
    fn test_write2() {
        let mut b = RingBuffer::new();
        // let x: [u8; 5] = [1, 2, 3, 4, 5];
        let start = b.get_borrow_mut().alloc_bytes(0, None);
        let mut end = b.get_borrow_mut().write_var_u64(start, 1).unwrap();
        end = b.get_borrow_mut().write_var_u64(end, 3).unwrap();
        end = b.get_borrow_mut().write_var_u64(end, 3).unwrap();
        end = b.get_borrow_mut().write_var_u64(end, 3).unwrap();
        end = b.get_borrow_mut().write_var_u64(end, 3).unwrap();
        end = b.get_borrow_mut().write_var_u64(end, 3).unwrap();

        println!("b:{:?}", b.get_borrow().buffers);
        println!("start:{},end:{}", start, end);
        let pool = Arc::new(b);
        let reader = RingBufferReader::new(pool, start, end);
        for v in reader.iter() {
            println!("b:{:?},len:{:?}", v, v.len());
        }
    }

    #[test]
    fn test_write() {
        let mut b = RingBuffer::new();
        let x: [u8; 5] = [1, 2, 3, 4, 5];
        let start = b.get_borrow_mut().alloc_bytes(0, None);
        let mut end = b.get_borrow_mut().write_array(start, &x).unwrap();
        // end = b.borrow_mut().write_array(end, &x).unwrap();
        // end = b.borrow_mut().write_array(end, &x).unwrap();
        // end = b.borrow_mut().write_array(end, &x).unwrap();
        // end = b.borrow_mut().write_array(end, &x).unwrap();
        // end = b.borrow_mut().write_array(end, &x).unwrap();
        // end = b.borrow_mut().write_array(end, &x).unwrap();
        // end = b.borrow_mut().write_array(end, &x).unwrap();
        // end = b.borrow_mut().write_array(end, &x).unwrap();
        // end = b.borrow_mut().write_array(end, &x).unwrap();
        // end = b.borrow_mut().write_array(end, &x).unwrap();

        println!("b:{:?}", b.get_borrow().buffers);
        println!("start:{},end:{}", start, end);
        let pool = Arc::new(b);
        let reader = RingBufferReader::new(pool, start, end);
        for v in reader.iter() {
            println!("b:{:?},len:{:?}", v, v.len());
        }
    }

    const u64var_test: [u64; 23] = [
        1,
        2,
        10,
        20,
        63,
        64,
        65,
        127,
        128,
        129,
        255,
        256,
        257,
        517,
        768,
        5976746468,
        88748464645454,
        5789627789625558,
        18446744073709551,
        184467440737095516,
        1844674407370955161,
        18446744073709551615,
        1 << 64 - 1,
    ];

    #[test]
    fn test_read_Int() {
        let mut b = RingBuffer::new();
        let start = b.get_borrow_mut().alloc_bytes(0, None);
        let mut end = b.get_borrow_mut().write_var_u64(start, 0).unwrap();
        for i in u64var_test {
            end = b.get_borrow_mut().write_var_u64(end, i).unwrap();
        }
        let a = Arc::new(b);
        let reader = RingBufferReader::new(a, start, end);

        for v in reader.iter() {
            println!("b:{:?},len:{:?}", v, v.len());
        }

        let mut r = SnapshotReader::new(reader);
        let mut t = r.iter();

        for i in t {
            // let c = t.next().unwrap();
            println!("c:{}", i);
        }

        // for _ in 0..u64var_test.len() + 1 {
        //     let (i, _) = r.read_vu64::<Binary>();
        //     println!("i:{}", i);
        // }
    }

    use std::thread;
    #[test]

    fn test_read() {
        let mut b = RingBuffer::new();
        // let x: [u8; 12] = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12];
        // let mut pos = b.alloc_bytes(0, None);
        // for v in uvar_test {
        //     pos = b.write_vu32(pos, v).unwrap();
        // }

        let pool = Arc::new(b);
        let a = Arc::clone(&pool);
        let t1 = thread::spawn(move || loop {
            let mut reader = RingBufferReader::new(a.clone(), 0, 0);
            for v in uvar_test {
                // let x = reader.read_vu32();
                //println!("x{:?}", x);
            }
            break;
        });
        let b = Arc::clone(&pool);
        let t2 = thread::spawn(move || loop {
            let mut reader = RingBufferReader::new(b.clone(), 0, 0);
            for v in uvar_test {
                // let x = reader.read_vu32();
                //println!("x{:?}", x);
            }
            break;
        });
        t1.join();
        t2.join();
    }
}
