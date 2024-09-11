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
