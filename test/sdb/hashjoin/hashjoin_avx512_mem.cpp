/*
 * @Author: SunZewen
 * @Date: 2022-01-01 16:30:34
 * @LastEditors: SunZewen
 * @LastEditTime: 2022-01-17 09:40:23
 * @Description:   
 * @FilePath: /sunzw/workspace/ecnu/ecnu/hef/test/sdb/hashjoin/hashjoin_avx512_mem.cpp
 */

#include <iostream>
#include <vector>
#include <chrono>
#include <stdint.h>
#include <immintrin.h>
#include <cmath>
#include <stdlib.h>
#include <iomanip>
#include <string>

#include "../../../include/common.hpp"
#include "../../../include/operator/operator.hpp"

double hashjoin_l3_test()
{
    const uint64_t hash_tabel_size = 128 * 1024 * 1024;

    std::vector<uint64_t> lo_vector;
    std::vector<uint64_t> o_vector;

    //get table data
    load_data(lo_vector, std::string("/tmp/ram/tpch/scale_100/lineitem_line0.txt"));
    load_data(o_vector, std::string("/tmp/ram/tpch/scale_100/orders_line0.txt"));

    uint64_t *l_orderkey = lo_vector.data();
    uint64_t *o_orderkey = o_vector.data();


    // process date table
    uint64_t o_size = 150000000;
    uint64_t l_size = 600000000;

    uint64_t *o_rid = new uint64_t[o_size];
    uint64_t *l_rid = new uint64_t[l_size];

    for (uint64_t i = 0; i < o_size; i++)
    {
        o_rid[i] = i;
    }

    for (uint64_t i = 0; i < l_size; i++)
    {
        l_rid[i] = i;
    }

    uint64_t *o_result_ids = new uint64_t[o_size];
    uint64_t o_result_size = 0;

    for (uint64_t i = 0; i < o_size; i++)
    {
        if (o_orderkey[i] < 150000)
        {
            o_result_ids[o_result_size] = i;
            o_result_size++;
        }
    }

    //build data hash table
    uint64_t *order_hash_table = new uint64_t[hash_tabel_size];
    hash_build_rand(o_result_ids, o_orderkey, o_result_size, order_hash_table, hash_tabel_size);

    uint64_t *l_result = new uint64_t[l_size];
    uint64_t  l_result_size = 0;

    auto t1 = std::chrono::steady_clock::now();

    uint64_t rest = l_size % 8;

    for (uint64_t i = 0; i < l_size - rest; i = i + 8)
    {
        __m512i src = _mm512_loadu_epi64(l_orderkey + i);
        __mmask8 mask = probe_avx512(order_hash_table, hash_tabel_size, src);

        __m512i rids = _mm512_loadu_epi64(l_rid + i);
        _mm512_mask_compressstoreu_epi64(l_result + l_result_size, mask, rids);
        l_result_size += _mm_popcnt_u64(mask);
    }

    for (uint64_t i = l_size - rest; i < l_size; i++)
    {
        if (hash_probe(order_hash_table, hash_tabel_size, l_orderkey[i]))
        {
            l_result[l_result_size] = l_rid[i];
            l_result_size++;
        }
    }
    
    auto t2 = std::chrono::steady_clock::now();
    double dr_ms = std::chrono::duration<double, std::milli>(t2 - t1).count();
    std::cout << "hybrid hash join l3 finished, hash tabel size is " << (unsigned int)(hash_tabel_size) << ", lo_result_size = " << l_result_size << ", use time " << std::setprecision(3) << dr_ms << " ms" << std::endl;


    return dr_ms;
}

int main(int argc, char *argv[])
{
    double runtime = (hashjoin_l3_test());

    std::cout << std::setiosflags(std::ios::fixed) << std::setprecision(3) << runtime;

    return 0;
}