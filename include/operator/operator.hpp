/*
 * @Author: SunZewen
 * @Date: 2021-02-24 09:43:39
 * @LastEditors: SunZewen
 * @LastEditTime: 2022-01-14 00:02:13
 * @Description:   
 * @FilePath: /sunzw/workspace/ecnu/ecnu/hef/include/operator/operator.hpp
 */

#ifndef OPERATOR
#define OPERATOR

#include <iostream>
#include <unistd.h>
#include <cstdint>
#include <immintrin.h>


void filter11_uint(uint64_t *index, uint64_t *src, uint64_t size, uint64_t threshold, uint64_t *dst, uint64_t &out_size)
{
    out_size = 0;
    for (uint64_t i = 0; i < size; i++)
    {
        if (src[index[i]] == threshold)
        {
            dst[out_size] = index[i];
            out_size = out_size + 1;
        }
    }
}

void filter11_str(uint64_t *index, std::string *src, uint64_t size, std::string threshold, uint64_t *dst, uint64_t &out_size)
{
    out_size = 0;
    for (uint64_t i = 0; i < size; i++)
    {
        if (threshold.compare(src[index[i]]) == 0)
        {
            dst[out_size] = index[i];
            out_size = out_size + 1;
        }
    }
}

void filter12_str(uint64_t *index, std::string *src, uint64_t size, std::string threshold1, std::string threshold2, uint64_t *dst, uint64_t &out_size)
{
    out_size = 0;
    for (uint64_t i = 0; i < size; i++)
    {
        if (threshold1.compare(src[index[i]]) == 0 || threshold2.compare(src[index[i]]) == 0)
        {
            dst[out_size] = index[i];
            out_size = out_size + 1;
        }
    }
}

void filter12_uint(uint64_t *index, uint64_t *src, uint64_t size, int threshold1, int threshold2, uint64_t *dst, uint64_t &out_size)
{
    out_size = 0;
    for (uint64_t i = 0; i < size; i++)
    {
        if (src[index[i]] >= threshold1 && src[index[i]] <= threshold2)
        {
            dst[out_size] = index[i];
            out_size++;
        }
    }
}

inline uint64_t hash(uint64_t k, uint64_t seed)
{
    // MurmurHash64A
    const uint64_t m = 0xc6a4a7935bd1e995;
    const int r = 47;
    uint64_t h = seed ^ 0x8445d61a4e774912 ^ (8 * m);

    k *= m;
    k ^= k >> r;
    k *= m;
    h ^= k;
    h *= m;
    h ^= h >> r;
    h *= m;
    h ^= h >> r;

    return h;
}

inline void hash_build(uint64_t *src, uint64_t src_size, uint64_t *hash_table, uint64_t ht_size)
{
    uint64_t mask = ht_size - 1;
    for (uint64_t i = 0; i < src_size; i++)
    {
        // compute hash value
        uint64_t hash_value = hash(src[i], 0);

        // put the data into hash table
        hash_table[hash_value & mask] = src[i];
    }
}


inline void hash_build_rand(uint64_t *index, uint64_t *src, uint64_t size, uint64_t *hash_table, uint64_t ht_size)
{
    uint64_t mask = ht_size - 1;
    for (uint64_t i = 0; i < size; i++)
    {
        // compute hash value
        uint64_t hash_value = hash(src[index[i]], 0);

        // put the data into hash table
        hash_table[hash_value & mask] = src[index[i]];
    }
}


inline bool hash_probe(uint64_t *hash_table, uint64_t ht_size, uint64_t val)
{
    uint64_t mask = ht_size - 1;

    // compute hash value
    uint64_t hash_value = hash(val, 0);

    // std::cout << (hash_value & mask) << std::endl;

    // put the data into hash table
    if (hash_table[hash_value & mask] == val)
    {
        return true;
    }

    return false;
}

void project_mul(uint64_t *index, uint64_t *in1, uint64_t *in2, uint64_t *dst, uint64_t size)
{
    for (uint64_t i = 0; i < size; i++)
    {
        dst[i] = in1[index[i]] * in2[index[i]];
    }
}

void reduce_sum(uint64_t *src, uint64_t size, unsigned long &dst)
{
    for (uint64_t i = 0; i < size; i++)
    {
        dst += src[i];
    }
}




inline void hash_avx512(__m512i key, __m512i &out, unsigned int seed)
{
    // MurmurHash64A
    __m512i m = _mm512_set1_epi64(0xc6a4a7935bd1e995);
    // __m512i r = _mm512_set1_epi64(47);

    __m512i h = _mm512_set1_epi64(seed ^ 0x8445d61a4e774912 ^ (8 * 0xc6a4a7935bd1e995));

    __m512i k = _mm512_mullo_epi64(key, m);
    __m512i kr = _mm512_srli_epi64(k, 47);
    k = _mm512_xor_epi64(k, kr);
    k = _mm512_mullo_epi64(k, m);

    h = _mm512_xor_epi64(k, h);
    h = _mm512_mullo_epi64(h, m);

    __m512i hr = _mm512_srli_epi64(h, 47);
    h = _mm512_xor_epi64(h, hr);
    h = _mm512_mullo_epi64(h, m);

    hr = _mm512_srli_epi64(h, 47);
    h = _mm512_xor_epi64(h, hr);

    out = h;
}


inline __mmask8 probe_avx512(uint64_t *hash_table,
                             uint64_t ht_size,
                             __m512i val)
{
    __m512i addr_mask = _mm512_set1_epi64(ht_size - 1);

    __m512i hash_val;
    hash_avx512(val, hash_val, 0);

    __m512i vindex = _mm512_and_epi64(hash_val, addr_mask); // trans the hash value to address, use the low 16 bit

    //load hash table data
    __m512i hash_data = _mm512_i64gather_epi64(vindex, hash_table, 8);

    //cmp
    __mmask8 res = _mm512_cmpeq_epu64_mask(hash_data, val); //

    return res;
}

#endif