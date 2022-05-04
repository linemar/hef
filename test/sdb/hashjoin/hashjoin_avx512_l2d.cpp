/*
 * @Author: SunZewen
 * @Date: 2022-01-01 16:30:34
 * @LastEditors: SunZewen
 * @LastEditTime: 2022-01-13 23:08:26
 * @Description:   
 * @FilePath: /sunzw/workspace/ecnu/ecnu/hef/test/sdb/hashjoin/hashjoin_avx512_l2d.cpp
 */

#include <iostream>
#include <vector>
#include <chrono>
#include <stdint.h>
#include <immintrin.h>
#include <cmath>
#include <stdlib.h>
#include <iomanip>

#include "../../../include/database/database.hpp"
#include "../../../include/database/table.hpp"
#include "../../../include/database/types.hpp"
#include "../../../include/database/ssb_db.hpp"
#include "../../../include/database/database.hpp"
#include "../../../include/operator/operator.hpp"

double hashjoin_avx512(DataBase *db)
{
    const int hash_tabel_size = 32 * 1024;

    //get table data
    Table t_date = db->get_table(std::string("date"));
    Table t_lineorder = db->get_table(std::string("lineorder"));

    uint64_t *d_datekey = (uint64_t *)t_date.get_column("d_datekey").get_data();
    uint64_t *lo_orderdate = (uint64_t *)t_lineorder.get_column("lo_orderdate").get_data();

    std::cout << "get data is finished." << std::endl;

    // process date table
    uint64_t d_size = t_date.get_column("d_datekey").get_size();
    uint64_t lo_size = t_lineorder.get_column("lo_orderdate").get_size();

    uint64_t *d_rid = new uint64_t[d_size];
    uint64_t *lo_rid = new uint64_t[lo_size];

    for (uint64_t i = 0; i < d_size; i++)
    {
        d_rid[i] = i;
    }
    for (uint64_t i = 0; i < lo_size; i++)
    {
        lo_rid[i] = i;
    }

    //build data hash table
    uint64_t *date_hash_table = new uint64_t[hash_tabel_size];
    hash_build(d_datekey, d_size, date_hash_table, hash_tabel_size);

    uint64_t *lo_result = new uint64_t[lo_size];
    uint64_t lo_result_size = 0;

    auto t1 = std::chrono::steady_clock::now();

    uint64_t rest = lo_size % 8;

    for (uint64_t i = 0; i < lo_size - rest; i = i + 8)
    {
        __m512i src = _mm512_loadu_epi64(lo_orderdate + i);
        __mmask8 mask = probe_avx512(date_hash_table, hash_tabel_size, src);

        __m512i rids = _mm512_loadu_epi64(lo_rid + i);
        _mm512_mask_compressstoreu_epi64(lo_result + lo_result_size, mask, rids);
        lo_result_size += _mm_popcnt_u64(mask);
    }

    for (uint64_t i = lo_size - rest; i < lo_size; i++)
    {
        if (hash_probe(date_hash_table, hash_tabel_size, lo_orderdate[i]))
        {
            lo_result[lo_result_size] = lo_rid[i];
            lo_result_size++;
        }
    }

    auto t2 = std::chrono::steady_clock::now();
    double dr_ms = std::chrono::duration<double, std::milli>(t2 - t1).count();
    std::cout << "simd finished, hash tabel size is " << (unsigned int)(hash_tabel_size) << ", use time " << dr_ms << " ms" << std::endl;
    std::cout << "lo_result_size = " << lo_result_size << std::endl;
    // std::cout << std::endl;

    // for (int i = 0; i < lo_result_size; i++)
    // {
    //     std::cout << lo_result[i] << std::endl;
    // }

    return dr_ms;
}

int main(int argc, char *argv[])
{

    SSB_DB *ssb_db = new SSB_DB();

    //load data
    ssb_db->load_data(std::string("/tmp/ram/ssb/scale_1"));
    DataBase *db = ssb_db->db;

    double runtime = hashjoin_avx512(db);

    std::cout << std::setiosflags(std::ios::fixed) << std::setprecision(3) << runtime;

    return 0;
}