#include "hash.h"
// https://nullprogram.com/blog/2018/07/31/
inline static u64 murmurhash32(u32 x)
{
    x ^= x >> 16;
    x *= UINT32_C(0x85ebca6b);
    x ^= x >> 13;
    x *= UINT32_C(0xc2b2ae35);
    x ^= x >> 16;
    return (u64)x;
}

inline static u64 murmurhash64(u64 x)
{
    x ^= x >> 30;
    x *= UINT64_C(0xbf58476d1ce4e5b9);
    x ^= x >> 27;
    x *= UINT64_C(0x94d049bb133111eb);
    x ^= x >> 31;
    return x;
}

static u64 Hash(u64 val)
{
    return murmurhash32((u32)val);
}

static u64 Hash1(const char *val, usize size)
{
    u64 hash = 5381;

    for (usize i = 0; i < size; i++)
    {
        hash = ((hash << 5) + hash) + val[i];
    }
    return hash;
}

static u64 Hash_char(char *val, usize size)
{
    return Hash1((const char *)val, size);
}

static u64 Hash_u8(u8 *val, usize size)
{
    return Hash1((const char *)val, size);
}

u64 checksum(u8 *buffer, usize size)
{
    u64 result = 5381;
    u64 *ptr = (u64 *)buffer;
    usize i;
        // for efficiency, we first hash uint64_t values
    for (i = 0; i < size / 8; i++)
    {
        result ^= Hash(ptr[i]);
    }
    if (size - i * 8 > 0)
    {
                // the remaining 0-7 bytes we hash using a string hash
        result ^= Hash_u8(buffer + i * 8, size - i * 8);
    }
    return result;
}