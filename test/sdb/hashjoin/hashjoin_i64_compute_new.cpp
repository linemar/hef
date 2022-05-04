/*
 * @Author: SunZewen
 * @Date: 2022-01-01 16:30:34
 * @LastEditors: SunZewen
 * @LastEditTime: 2022-01-11 19:29:26
 * @Description:   
 * @FilePath: /sunzw/workspace/ecnu/ecnu/hef/test/sdb/hashjoin/hashjoin_i64_compute_new.cpp
 */

#include <iostream>
#include <vector>
#include <chrono>
#include <stdint.h>
#include <immintrin.h>
#include <cmath>
#include <stdlib.h>
#include <iomanip>

#include "../../../include/operator/operator.hpp"

double hashjoin_comp_test()
{
    uint64_t size = 512 * 1024 * 1024;
    //build data hash table
    uint64_t *src = new uint64_t[size];
    uint64_t *dst = new uint64_t[size];

    for (uint64_t i = 0; i < size; i++)
    {
        src[i] = i;
    }

    auto t1 = std::chrono::steady_clock::now();

    uint64_t rest = size % 8;
    __m512i m = _mm512_set1_epi64(0xc6a4a7935bd1e995);
    __m512i h = _mm512_set1_epi64(0 ^ 0x8445d61a4e774912 ^ (8 * 0xc6a4a7935bd1e995));

    for (uint64_t i = 0; i < size - rest; i = i + 8)
    {
        // MurmurHash64A
        __m512i key = _mm512_loadu_epi64(src + i);
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
        _mm512_storeu_epi64(dst + i, h);
    }

    for (uint64_t i = size - rest; i < size; i++)
    {
        dst[i] = hash(src[i], 0);
    }

    auto t2 = std::chrono::steady_clock::now();
    double dr_ms = std::chrono::duration<double, std::milli>(t2 - t1).count();
    std::cout << "simd hash compute finished, use time " << std::setprecision(3) << dr_ms << " ms" << std::endl;

    return dr_ms;
}

int main(int argc, char *argv[])
{
    double runtime = (hashjoin_comp_test());
    std::cout << std::setiosflags(std::ios::fixed) << std::setprecision(3) << runtime;

    return 0;
}