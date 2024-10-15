use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
use rand::thread_rng;
use rand::{rngs::ThreadRng, RngCore};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};
/// 原子计数器，防止并发时重复
static COUNTER: AtomicU64 = AtomicU64::new(0);

const HIGHEST_BIT: u64 = 1 << 63;

#[inline(always)]
pub fn i64_to_u64(val: i64) -> u64 {
    (val as u64) ^ HIGHEST_BIT
}

/// Reverse the mapping given by [`i64_to_u64`](./fn.i64_to_u64.html).
#[inline(always)]
pub fn u64_to_i64(val: u64) -> i64 {
    (val ^ HIGHEST_BIT) as i64
}

pub(crate) struct IdGenerator {
    rng: ThreadRng,
    counter: u16, // 普通计数器，无需原子操作
}

impl IdGenerator {
    /// 创建一个新的 ID 生成器
    pub(crate) fn new() -> Self {
        Self {
            rng: thread_rng(),
            counter: 0,
        }
    }

    /// 生成 URL 安全的、高性能的 ID（长度为 20 字符）
    pub(crate) fn generate_id(&mut self) -> String {
        // 1. 获取当前 Unix 时间戳（毫秒）
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards")
            .as_secs();

        // 2. 更新计数器，限制为 16 位
        self.counter = self.counter.wrapping_add(1) & 0xFFFF;

        // 3. 使用随机数生成器
        let mut random = [0u8; 6]; // 6 字节随机数
        self.rng.fill_bytes(&mut random);

        // 4. 组合时间戳、计数器和随机数组为 16 字节数组
        let mut bytes = [0u8; 16];
        bytes[..8].copy_from_slice(&timestamp.to_be_bytes());
        bytes[8..10].copy_from_slice(&self.counter.to_be_bytes());
        bytes[10..].copy_from_slice(&random);

        // 5. Base64 URL 安全编码，并截取前 20 个字符
        let mut encoded = URL_SAFE_NO_PAD.encode(&bytes);
        encoded.truncate(20);

        encoded
    }
}

pub(crate) fn merge_sorted_vecs_unique<T: std::cmp::PartialOrd + Clone>(
    vec1: &Vec<T>,
    vec2: &Vec<T>,
) -> Vec<T> {
    let mut merged: Vec<T> = Vec::with_capacity(vec1.len() + vec2.len());
    let mut iter1 = vec1.into_iter();
    let mut iter2 = vec2.into_iter();
    let mut v1 = iter1.next();
    let mut v2 = iter2.next();
    while v1.is_some() || v2.is_some() {
        match (v1, v2) {
            (Some(a), Some(b)) => {
                if a < b {
                    merged.push(a.clone());
                    v1 = iter1.next();
                } else if a > b {
                    merged.push(b.clone());
                    v2 = iter2.next();
                } else {
                    merged.push(a.clone());
                    v1 = iter1.next();
                    v2 = iter2.next();
                }
            }
            (Some(a), None) => {
                merged.push(a.clone());
                v1 = iter1.next();
            }
            (None, Some(b)) => {
                merged.push(b.clone());
                v2 = iter2.next();
            }
            (None, None) => break,
        }
    }
    merged
}

#[cfg(test)]
mod tests {
    use super::IdGenerator;
    use super::*;
    #[test]
    fn test_add_doc() {
        let mut id_gen = IdGenerator::new();
        println!("{}", id_gen.generate_id());
        println!("{}", id_gen.generate_id());
        println!("{}", id_gen.generate_id());
        println!("{}", id_gen.generate_id());
    }

    #[test]
    fn test_merge_sorted_vecs_unique() {
        let vec1 = vec![1, 3, 5, 7, 9];
        let vec2 = vec![2, 3, 4, 5, 6, 8, 10];
        let merged_vec = merge_sorted_vecs_unique(&vec1, &vec2);
        println!("{:?}", merged_vec); // 输出：[1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
    }
}
