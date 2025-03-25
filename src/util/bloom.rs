use crate::schema::BinarySerialize;
use crate::GyResult;
use bloomfilter::Bloom;

impl BinarySerialize for [(u64, u64); 2] {
    fn binary_deserialize<R: std::io::Read>(reader: &mut R) -> GyResult<Self> {
        let a = <(u64, u64)>::binary_deserialize(reader)?;
        let b = <(u64, u64)>::binary_deserialize(reader)?;
        Ok([a, b])
    }

    fn binary_serialize<W: std::io::Write>(&self, writer: &mut W) -> GyResult<()> {
        self[0].binary_serialize(writer)?;
        self[1].binary_serialize(writer)
    }
}

impl BinarySerialize for (u64, u64) {
    fn binary_deserialize<R: std::io::Read>(reader: &mut R) -> GyResult<Self> {
        let a = u64::binary_deserialize(reader)?;
        let b = u64::binary_deserialize(reader)?;
        Ok((a, b))
    }
    fn binary_serialize<W: std::io::Write>(&self, writer: &mut W) -> GyResult<()> {
        self.0.binary_serialize(writer)?;
        self.1.binary_serialize(writer)
    }
}

const fp_rate: f64 = 0.001;
// 布隆过滤器实现
pub struct GyBloom {
    bloom: Bloom<[u8]>,
    items_count: usize,
}

impl BinarySerialize for GyBloom {
    fn binary_deserialize<R: std::io::Read>(reader: &mut R) -> GyResult<Self> {
        let items_count = usize::binary_deserialize(reader)?;
        let number_of_bits = u64::binary_deserialize(reader)?;
        let number_of_hash_functions = u32::binary_deserialize(reader)?;
        let sip_keys: [(u64, u64); 2] = <[(u64, u64); 2]>::binary_deserialize(reader)?;
        let v = Vec::<u8>::binary_deserialize(reader)?;
        let bloom = Bloom::from_existing(&v, number_of_bits, number_of_hash_functions, sip_keys);

        Ok(GyBloom {
            bloom: bloom,
            items_count: items_count,
        })
    }
    fn binary_serialize<W: std::io::Write>(&self, writer: &mut W) -> super::error::GyResult<()> {
        self.items_count.binary_serialize(writer)?;
        self.bloom.number_of_bits().binary_serialize(writer)?;
        self.bloom
            .number_of_hash_functions()
            .binary_serialize(writer)?;
        self.bloom.sip_keys().binary_serialize(writer)?;
        self.bloom.bitmap().binary_serialize(writer)
    }
}

impl GyBloom {
    pub fn new(items_count: usize) -> GyBloom {
        Self {
            bloom: Bloom::new_for_fp_rate(items_count, fp_rate),
            items_count: items_count,
        }
    }

    pub fn check(&self, key: &[u8]) -> bool {
        self.bloom.check(key)
    }

    pub fn set(&mut self, key: &[u8]) {
        self.bloom.set(key)
    }

    pub fn clear(&mut self) {
        self.bloom.clear()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;
    #[test]
    fn TestBloom() {
        let mut bloom: GyBloom = GyBloom::new(2);
        bloom.set("a".as_bytes());
        bloom.set("b".as_bytes());
        bloom.set("uu".as_bytes());
        bloom.set("pp".as_bytes());

        println!("a:{}", bloom.check("a".as_bytes()));
        println!("b:{}", bloom.check("b".as_bytes()));
        println!("uu:{}", bloom.check("uu".as_bytes()));
        println!("pp:{}", bloom.check("pp".as_bytes()));

        println!("xx:{}", bloom.check("xx".as_bytes()));
        println!("y:{}", bloom.check("y".as_bytes()));
        println!("u:{}", bloom.check("u".as_bytes()));
        println!("poo:{}", bloom.check("poo".as_bytes()));

        let mut bytes: Vec<u8> = Vec::with_capacity(1024);

        let mut cursor = Cursor::new(&mut bytes);

        bloom.binary_serialize(&mut cursor).unwrap();

        let mut cursor1 = Cursor::new(&bytes);

        let new_bloom = GyBloom::binary_deserialize(&mut cursor1).unwrap();

        println!("=====================");
        println!("a:{}", new_bloom.check("a".as_bytes()));
        println!("b:{}", new_bloom.check("b".as_bytes()));
        println!("uu:{}", new_bloom.check("uu".as_bytes()));
        println!("pp:{}", new_bloom.check("pp".as_bytes()));

        println!("xx:{}", new_bloom.check("xx".as_bytes()));
        println!("y:{}", new_bloom.check("y".as_bytes()));
        println!("u:{}", new_bloom.check("u".as_bytes()));
        println!("poo:{}", new_bloom.check("poo".as_bytes()));
    }
}
