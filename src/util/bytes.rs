use std::io;
use std::io::Result;
const MAX_VARINT_LEN64: usize = 10;

pub trait WriteBinary {
    fn put_uvarint64(buf: &mut [u8], x: u64) -> usize;
    fn put_varint64(buf: &mut [u8], x: i64) -> usize;
}

pub trait ReadBinary {
    fn uvarint64(buf: &[u8]) -> (u64, i32);
    fn varint64(buf: &[u8]) -> (i64, i32);
    fn read_uvarint64<T: ReadU8 + ?Sized>(t: &mut T) -> (u64, i32);
}

pub enum binary {}

impl WriteBinary for binary {
    #[inline]
    fn put_uvarint64(buf: &mut [u8], mut x: u64) -> usize {
        let mut i: usize = 0;
        while x >= 0x80 {
            buf[i] = x as u8 | 0x80;
            x >>= 7;
            i += 1;
        }
        buf[i] = x as u8;
        i + 1
    }

    // PutVarint encodes an int64 into buf and returns the number of bytes written.
    // If the buffer is too small, PutVarint will panic.
    #[inline]
    fn put_varint64(buf: &mut [u8], x: i64) -> usize {
        let mut ux = (x as u64) << 1;
        if x < 0 {
            ux = !ux;
        }
        Self::put_uvarint64(buf, ux)
    }
}

impl ReadBinary for binary {
    #[inline]
    fn uvarint64(buf: &[u8]) -> (u64, i32) {
        let mut x: u64 = 0;
        let mut s: u32 = 0;
        for (i, b) in buf.iter().enumerate() {
            if i == MAX_VARINT_LEN64 {
                // Catch byte reads past MaxVarintLen64.
                return (0, -(i as i32 + 1));
            }
            if *b < 0x80 {
                if i == MAX_VARINT_LEN64 - 1 && *b > 1 {
                    return (0, -(i as i32 + 1)); // overflow
                }
                return (x | (*b as u64) << s, i as i32 + 1);
            }
            x |= ((b & 0x7f) as u64) << s;
            s += 7
        }
        (0, 0)
    }

    #[inline]
    fn varint64(buf: &[u8]) -> (i64, i32) {
        let (ux, n) = Self::uvarint64(buf);
        let mut x = (ux >> 1) as i64;
        if ux & 1 != 0 {
            x = !x;
        }
        (x, n)
    }

    #[inline]
    fn read_uvarint64<T: ReadU8 + ?Sized>(t: &mut T) -> (u64, i32) {
        let mut x: u64 = 0;
        let mut s: u32 = 0;
        let mut i: usize = 0;
        while let Ok(b) = t.read_u8() {
            if i == MAX_VARINT_LEN64 {
                // Catch byte reads past MaxVarintLen64.
                return (0, -(i as i32 + 1));
            }
            if b < 0x80 {
                if i == MAX_VARINT_LEN64 - 1 && b > 1 {
                    return (0, -(i as i32 + 1)); // overflow
                }
                return (x | (b as u64) << s, i as i32 + 1);
            }
            x |= ((b & 0x7f) as u64) << s;
            s += 7;
            i += 1;
        }
        (0, 0)
    }
}

pub trait WriteBytesVarExt: io::Write {
    #[inline]
    fn write_uvarint64<T: WriteBinary>(&mut self, x: u64) -> Result<usize> {
        let mut buf = [0u8; MAX_VARINT_LEN64];
        let i = T::put_uvarint64(&mut buf, x);
        self.write_all(&buf[..i])?;
        Ok(i)
    }
}

pub trait ReadU8: io::Read {
    #[inline]
    fn read_u8(&mut self) -> Result<u8> {
        let mut buf = [0; 1];
        self.read_exact(&mut buf)?;
        Ok(buf[0])
    }
}

impl<R: io::Read + ?Sized> ReadU8 for R {}

impl<R: ReadU8 + ?Sized> ReadBytesVarExt for R {}

pub trait ReadBytesVarExt: ReadU8 {
    fn read_uvarint64<T: ReadBinary>(&mut self) -> (u64, i32) {
        T::read_uvarint64(self)
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    const ivar_test: [i64; 40] = [
        -1,
        -2,
        -10,
        -20,
        -63,
        -64,
        -65,
        -127,
        -128,
        -129,
        -255,
        -256,
        -257,
        -5976746468,
        -88748464645454,
        -5789627789625558,
        -18446744073709551,
        -184467440737095516,
        -1844674407370955161,
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
        5976746468,
        88748464645454,
        5789627789625558,
        18446744073709551,
        184467440737095516,
        1844674407370955161,
        1 << 63 - 1,
    ];

    const uvar_test: [u64; 22] = [
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
    fn test_uvarint64() {
        let mut buf = [0u8; MAX_VARINT_LEN64];
        for x in uvar_test {
            binary::put_uvarint64(&mut buf, x);
            let (v, _) = binary::uvarint64(&buf);
            assert!(x == v);
        }
    }

    #[test]
    fn test_varint64() {
        let mut buf = [0u8; MAX_VARINT_LEN64];
        for x in ivar_test {
            binary::put_varint64(&mut buf, x);
            let (v, _) = binary::varint64(&buf);
            assert!(x == v);
        }
    }
    use super::ReadU8;

    //use std::io::Read;
    // use byteorder::ReadBytesExt;
    use std::io::{Cursor, Read};
    #[test]
    fn test_filevarint64() {
        let mut rdr = Cursor::new(vec![2, 5]);
        rdr.read_u8();
    }
}
