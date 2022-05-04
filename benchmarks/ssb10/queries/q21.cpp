/*
 * @Author: SunZewen
 * @Date: 2022-01-05 15:38:35
 * @LastEditors: SunZewen
 * @LastEditTime: 2022-01-21 13:23:14
 * @Description:   
 * @FilePath: /sunzw/workspace/ecnu/ecnu/hef/benchmarks/ssb10/queries/q21.cpp
 */

/*
Q2.1
select sum(lo_revenue), d_year, p_brand1
    from lineorder, date, part, supplier
    where lo_orderdate = d_datekey
        and lo_partkey = p_partkey
        and lo_suppkey = s_suppkey
        and p_category = 'MFGR#12'
        and s_region = 'AMERICA'
        group by d_year, p_brand1
        order by d_year, p_brand1;
*/

#include <iostream>
#include <vector>
#include <chrono>
#include <stdint.h>
#include <immintrin.h>
#include <cmath>
#include <stdlib.h>
#include <string>
#include <iomanip>

#include "hi.hpp"
#include "../../../include/database/database.hpp"
#include "../../../include/database/table.hpp"
#include "../../../include/database/types.hpp"
#include "../../../include/database/ssb_db.hpp"
#include "../../../include/database/database.hpp"
#include "../../../include/operator/operator.hpp"

const int part_hash_tabel_size = 512 * 1024;
const int supplier_hash_tabel_size = 64 * 1024;
const int date_hash_tabel_size = 64 * 1024;

double q21_scalar(DataBase *db)
{
    //get table data
    Table t_date = db->get_table(std::string("date"));
    Table t_supplier = db->get_table(std::string("supplier"));
    Table t_part = db->get_table(std::string("part"));
    Table t_lineorder = db->get_table(std::string("lineorder"));

    uint64_t *d_datekey = (uint64_t *)t_date.get_column("d_datekey").get_data();
    uint64_t *p_partkey = (uint64_t *)t_part.get_column("p_partkey").get_data();
    std::string *p_category = (std::string *)t_part.get_column("p_category").get_data();
    uint64_t *s_suppkey = (uint64_t *)t_supplier.get_column("s_suppkey").get_data();
    std::string *s_region = (std::string *)t_supplier.get_column("s_region").get_data();
    uint64_t *lo_orderdate = (uint64_t *)t_lineorder.get_column("lo_orderdate").get_data();
    uint64_t *lo_partkey = (uint64_t *)t_lineorder.get_column("lo_partkey").get_data();
    uint64_t *lo_suppkey = (uint64_t *)t_lineorder.get_column("lo_suppkey").get_data();

    std::cout << "get data is finished." << std::endl;

    // process date table
    uint64_t d_size = t_date.get_column("d_datekey").get_size();
    uint64_t p_size = t_part.get_column("p_partkey").get_size();
    uint64_t s_size = t_supplier.get_column("s_region").get_size();
    uint64_t lo_size = t_lineorder.get_column("lo_orderdate").get_size();

    uint64_t *d_rid = new uint64_t[d_size];
    uint64_t *s_rid = new uint64_t[s_size];
    uint64_t *p_rid = new uint64_t[p_size];
    uint64_t *lo_rid = new uint64_t[lo_size];

    for (uint64_t i = 0; i < d_size; i++)
    {
        d_rid[i] = i;
    }
    for (uint64_t i = 0; i < s_size; i++)
    {
        s_rid[i] = i;
    }
    for (uint64_t i = 0; i < p_size; i++)
    {
        p_rid[i] = i;
    }
    for (uint64_t i = 0; i < lo_size; i++)
    {
        lo_rid[i] = i;
    }

    uint64_t *p_result_ids = new uint64_t[p_size];
    uint64_t p_result_size = 0;
    // and p_category = 'MFGR#12'

    for (uint64_t i = 0; i < p_size; i++)
    {
        if (0 == std::string(p_category[i]).compare("MFGR#12"))
        {
            p_result_ids[p_result_size] = i;
            p_result_size++;
        }
    }

    //build data hash table
    uint64_t *part_hash_table = new uint64_t[part_hash_tabel_size];
    hash_build_rand(p_result_ids, p_partkey, p_result_size, part_hash_table, part_hash_tabel_size);

    // and s_region = 'AMERICA'
    uint64_t *s_result_ids = new uint64_t[s_size];
    uint64_t s_result_size = 0;

    for (uint64_t i = 0; i < s_size; i++)
    {
        if (0 == std::string(s_region[i]).compare("AMERICA"))
        {
            s_result_ids[s_result_size] = i;
            s_result_size++;
        }
    }
    //build data hash table
    uint64_t *date_hash_table = new uint64_t[date_hash_tabel_size];
    hash_build(d_datekey, d_size, date_hash_table, date_hash_tabel_size);

    // std::cout << "start build hash table." << std::endl;
    // sleep(3);

    uint64_t *supplier_hash_table = new uint64_t[supplier_hash_tabel_size];
    hash_build_rand(s_result_ids, s_suppkey, s_result_size, supplier_hash_table, supplier_hash_tabel_size);

    uint64_t *lo_result = new uint64_t[lo_size];
    uint64_t lo_result_size = 0;

    auto t1 = std::chrono::steady_clock::now();

    for (uint64_t i = 0; i < lo_size; i++)
    {
        if (hash_probe(part_hash_table, part_hash_tabel_size, lo_partkey[lo_rid[i]]))
        {
            if (hash_probe(supplier_hash_table, supplier_hash_tabel_size, lo_suppkey[lo_rid[i]]))
            {
                if (hash_probe(date_hash_table, date_hash_tabel_size, lo_orderdate[lo_rid[i]]))
                {
                    lo_result[lo_result_size] = lo_rid[i];
                    lo_result_size++;
                }
            }
        }
    }

    auto t2 = std::chrono::steady_clock::now();
    double dr_ms = std::chrono::duration<double, std::milli>(t2 - t1).count();
    std::cout << "scalar finished, use time " << dr_ms << " ms" << std::endl;
    std::cout << "lo_result_size = " << lo_result_size << std::endl;

    return dr_ms;
}

double q21_simd(DataBase *db)
{
    //get table data
    Table t_date = db->get_table(std::string("date"));
    Table t_supplier = db->get_table(std::string("supplier"));
    Table t_part = db->get_table(std::string("part"));
    Table t_lineorder = db->get_table(std::string("lineorder"));

    uint64_t *d_datekey = (uint64_t *)t_date.get_column("d_datekey").get_data();
    uint64_t *p_partkey = (uint64_t *)t_part.get_column("p_partkey").get_data();
    std::string *p_category = (std::string *)t_part.get_column("p_category").get_data();
    uint64_t *s_suppkey = (uint64_t *)t_supplier.get_column("s_suppkey").get_data();
    std::string *s_region = (std::string *)t_supplier.get_column("s_region").get_data();
    uint64_t *lo_orderdate = (uint64_t *)t_lineorder.get_column("lo_orderdate").get_data();
    uint64_t *lo_partkey = (uint64_t *)t_lineorder.get_column("lo_partkey").get_data();
    uint64_t *lo_suppkey = (uint64_t *)t_lineorder.get_column("lo_suppkey").get_data();

    std::cout << "get data is finished." << std::endl;

    // process date table
    uint64_t d_size = t_date.get_column("d_datekey").get_size();
    uint64_t p_size = t_part.get_column("p_partkey").get_size();
    uint64_t s_size = t_supplier.get_column("s_region").get_size();
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

    uint64_t *p_result_ids = new uint64_t[p_size];
    uint64_t p_result_size = 0;
    // and p_category = 'MFGR#12'

    for (uint64_t i = 0; i < p_size; i++)
    {
        if (0 == std::string(p_category[i]).compare("MFGR#12"))
        {
            p_result_ids[p_result_size] = i;
            p_result_size++;
        }
    }

    //build data hash table
    uint64_t *part_hash_table = new uint64_t[part_hash_tabel_size];
    hash_build_rand(p_result_ids, p_partkey, p_result_size, part_hash_table, part_hash_tabel_size);

    //build data hash table
    uint64_t *date_hash_table = new uint64_t[date_hash_tabel_size];
    hash_build(d_datekey, d_size, date_hash_table, date_hash_tabel_size);

    // and s_region = 'AMERICA'
    uint64_t *s_result_ids = new uint64_t[s_size];
    uint64_t s_result_size = 0;

    for (uint64_t i = 0; i < s_size; i++)
    {
        if (0 == std::string(s_region[i]).compare("AMERICA"))
        {
            s_result_ids[s_result_size] = i;
            s_result_size++;
        }
    }

    uint64_t *supplier_hash_table = new uint64_t[supplier_hash_tabel_size];
    hash_build_rand(s_result_ids, s_suppkey, s_result_size, supplier_hash_table, supplier_hash_tabel_size);

    uint64_t *lo_result = new uint64_t[lo_size];
    uint64_t lo_result_size = 0;

    auto t1 = std::chrono::steady_clock::now();
    uint64_t rest = lo_size % 8;

    uint64_t *block_buffer1 = new uint64_t[16];
    uint64_t *block_buffer2 = new uint64_t[16];

    uint64_t block_buffer_size1 = 0;
    uint64_t block_buffer_size2 = 0;

    uint64_t in_size1 = 0;
    uint64_t in_size2 = 0;

    uint64_t left_size1 = 0;
    uint64_t left_size2 = 0;

    for (uint64_t i = 0; i < lo_size - rest; i = i + 8)
    {
        __m512i src1 = _mm512_loadu_epi64(lo_partkey + i);
        __mmask8 mask1 = probe_avx512(part_hash_table, part_hash_tabel_size, src1);

        __m512i rids = _mm512_loadu_epi64(lo_rid + i);
        _mm512_mask_compressstoreu_epi64(block_buffer1 + block_buffer_size1, mask1, rids);
        block_buffer_size1 += _mm_popcnt_u64(mask1);

        if (block_buffer_size1 < 8)
        {
            continue;
        }

        block_buffer_size1 = block_buffer_size1 - 8;
        __m512i index2 = _mm512_loadu_epi64(block_buffer1 + block_buffer_size1);
        __m512i src2 = _mm512_i64gather_epi64(index2, lo_suppkey, 8);
        __mmask8 mask2 = probe_avx512(supplier_hash_table, supplier_hash_tabel_size, src2);

        _mm512_mask_compressstoreu_epi64(block_buffer2 + block_buffer_size2, mask2, index2);
        block_buffer_size2 += _mm_popcnt_u64(mask2);

        if (block_buffer_size2 < 8)
        {
            continue;
        }

        block_buffer_size2 = block_buffer_size2 - 8;

        __m512i index3 = _mm512_loadu_epi64(block_buffer2 + block_buffer_size2);
        __m512i src3 = _mm512_i64gather_epi64(index3, lo_orderdate, 8);
        __mmask8 mask3 = probe_avx512(date_hash_table, date_hash_tabel_size, src3);

        _mm512_mask_compressstoreu_epi64(lo_result + lo_result_size, mask3, index3);
        lo_result_size += _mm_popcnt_u64(mask3);
    }

    for (uint64_t i = lo_size - rest; i < lo_size; i++)
    {
        if (hash_probe(part_hash_table, part_hash_tabel_size, lo_partkey[lo_rid[i]]))
        {
            if (hash_probe(supplier_hash_table, supplier_hash_tabel_size, lo_suppkey[lo_rid[i]]))
            {
                if (hash_probe(date_hash_table, date_hash_tabel_size, lo_orderdate[lo_rid[i]]))
                {
                    lo_result[lo_result_size] = lo_rid[i];
                    lo_result_size++;
                }
            }
        }
    }

    for (uint64_t i = 0; i < block_buffer_size1; i++)
    {
        if (hash_probe(supplier_hash_table, supplier_hash_tabel_size, lo_suppkey[block_buffer1[i]]))
        {
            if (hash_probe(date_hash_table, date_hash_tabel_size, lo_orderdate[block_buffer1[i]]))
            {
                lo_result[lo_result_size] = block_buffer1[i];
                lo_result_size++;
            }
        }
    }

    for (uint64_t i = 0; i < block_buffer_size2; i++)
    {
        if (hash_probe(date_hash_table, date_hash_tabel_size, lo_orderdate[block_buffer2[i]]))
        {
            lo_result[lo_result_size] = block_buffer2[i];
            lo_result_size++;
        }
    }

    auto t2 = std::chrono::steady_clock::now();
    double dr_ms = std::chrono::duration<double, std::milli>(t2 - t1).count();
    std::cout << "simd finished, use time " << dr_ms << " ms" << std::endl;
    std::cout << "lo_result_size = " << lo_result_size << std::endl;

    return dr_ms;
}

double q21_hi(DataBase *db, uint64_t batch_size)
{
    //get table data
    Table t_date = db->get_table(std::string("date"));
    Table t_supplier = db->get_table(std::string("supplier"));
    Table t_part = db->get_table(std::string("part"));
    Table t_lineorder = db->get_table(std::string("lineorder"));

    uint64_t *d_datekey = (uint64_t *)t_date.get_column("d_datekey").get_data();
    uint64_t *p_partkey = (uint64_t *)t_part.get_column("p_partkey").get_data();
    std::string *p_category = (std::string *)t_part.get_column("p_category").get_data();
    uint64_t *s_suppkey = (uint64_t *)t_supplier.get_column("s_suppkey").get_data();
    std::string *s_region = (std::string *)t_supplier.get_column("s_region").get_data();
    uint64_t *lo_orderdate = (uint64_t *)t_lineorder.get_column("lo_orderdate").get_data();
    uint64_t *lo_partkey = (uint64_t *)t_lineorder.get_column("lo_partkey").get_data();
    uint64_t *lo_suppkey = (uint64_t *)t_lineorder.get_column("lo_suppkey").get_data();

    std::cout << "get data is finished." << std::endl;

    // process date table
    uint64_t d_size = t_date.get_column("d_datekey").get_size();
    uint64_t p_size = t_part.get_column("p_partkey").get_size();
    uint64_t s_size = t_supplier.get_column("s_region").get_size();
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

    uint64_t *p_result_ids = new uint64_t[p_size];
    uint64_t p_result_size = 0;
    // and p_category = 'MFGR#12'

    for (uint64_t i = 0; i < p_size; i++)
    {
        if (0 == std::string(p_category[i]).compare("MFGR#12"))
        {
            p_result_ids[p_result_size] = i;
            p_result_size++;
        }
    }

    //build data hash table
    uint64_t *part_hash_table = new uint64_t[part_hash_tabel_size];
    hash_build_rand(p_result_ids, p_partkey, p_result_size, part_hash_table, part_hash_tabel_size);

    //build data hash table
    uint64_t *date_hash_table = new uint64_t[date_hash_tabel_size];
    hash_build(d_datekey, d_size, date_hash_table, date_hash_tabel_size);

    // and s_region = 'AMERICA'
    uint64_t *s_result_ids = new uint64_t[s_size];
    uint64_t s_result_size = 0;

    for (uint64_t i = 0; i < s_size; i++)
    {
        if (0 == std::string(s_region[i]).compare("AMERICA"))
        {
            s_result_ids[s_result_size] = i;
            s_result_size++;
        }
    }

    uint64_t *supplier_hash_table = new uint64_t[supplier_hash_tabel_size];
    hash_build_rand(s_result_ids, s_suppkey, s_result_size, supplier_hash_table, supplier_hash_tabel_size);

    uint64_t *lo_result = new uint64_t[lo_size];
    uint64_t lo_result_size = 0;

    auto t1 = std::chrono::steady_clock::now();

    uint64_t block_size = batch_size; //  batch_size * 16;
    uint64_t rest = lo_size % block_size;

    uint64_t *block_buffer1 = new uint64_t[block_size * 2];
    uint64_t *block_buffer2 = new uint64_t[block_size * 2];

    uint64_t block_buffer_size1 = 0;
    uint64_t block_buffer_size2 = 0;

    uint64_t in_size1 = 0;
    uint64_t in_size2 = 0;

    uint64_t left_size1 = 0;
    uint64_t left_size2 = 0;

    for (uint64_t i = 0; i < lo_size - rest; i = i + block_size)
    {

        hashjoin_i64_l1(part_hash_table, part_hash_tabel_size, lo_rid + i, lo_partkey + i, block_size, batch_size, block_buffer1, block_buffer_size1);
        // hashjoin_i64_l1(part_hash_table, part_hash_tabel_size, lo_rid + i, lo_partkey + i, block_size, batch_size, lo_result, lo_result_size);

        if (block_buffer_size1 < block_size)
        {
            continue;
        }

        left_size1 = block_buffer_size1 % batch_size;
        in_size1 = block_buffer_size1 - left_size1;

        block_buffer_size1 = left_size1;

        hashjoin_gather_i64_l1(supplier_hash_table, supplier_hash_tabel_size, block_buffer1, lo_suppkey, in_size1, batch_size, block_buffer2, block_buffer_size2);
        // hashjoin_gather_i64_l1(supplier_hash_table, supplier_hash_tabel_size, block_buffer1, lo_suppkey, in_size1, batch_size, lo_result, lo_result_size);

        for (uint j = 0; j < left_size1; j++)
        {
            block_buffer1[j] = block_buffer1[j + in_size1];
        }

        if (block_buffer_size2 < block_size)
        {
            continue;
        }

        left_size2 = block_buffer_size2 % batch_size;
        in_size2 = block_buffer_size2 - left_size2;
        block_buffer_size2 = left_size2;

        hashjoin_gather_i64_l1(date_hash_table, date_hash_tabel_size, block_buffer2, lo_orderdate, in_size2, batch_size, lo_result, lo_result_size);

        for (uint j = 0; j < left_size2; j++)
        {
            block_buffer2[j] = block_buffer2[j + in_size2];
        }
    }

    for (uint64_t i = lo_size - rest; i < lo_size; i++)
    {
        if (hash_probe(part_hash_table, part_hash_tabel_size, lo_partkey[lo_rid[i]]))
        {
            if (hash_probe(supplier_hash_table, supplier_hash_tabel_size, lo_suppkey[lo_rid[i]]))
            {
                if (hash_probe(date_hash_table, date_hash_tabel_size, lo_orderdate[lo_rid[i]]))
                {
                    lo_result[lo_result_size] = lo_rid[i];
                    lo_result_size++;
                }
            }
        }
    }

    for (uint64_t i = 0; i < block_buffer_size1; i++)
    {
        if (hash_probe(supplier_hash_table, supplier_hash_tabel_size, lo_suppkey[block_buffer1[i]]))
        {
            if (hash_probe(date_hash_table, date_hash_tabel_size, lo_orderdate[block_buffer1[i]]))
            {
                lo_result[lo_result_size] = block_buffer1[i];
                lo_result_size++;
            }
        }
    }

    for (uint64_t i = 0; i < block_buffer_size2; i++)
    {
        if (hash_probe(date_hash_table, date_hash_tabel_size, lo_orderdate[block_buffer2[i]]))
        {
            lo_result[lo_result_size] = block_buffer1[i];
            lo_result_size++;
        }
    }

    auto t2 = std::chrono::steady_clock::now();
    double dr_ms = std::chrono::duration<double, std::milli>(t2 - t1).count();
    std::cout << "hi finished, use time " << dr_ms << " ms" << std::endl;
    std::cout << "lo_result_size = " << lo_result_size << std::endl;

    return dr_ms;
}

double q21_hi_block(DataBase *db, uint64_t batch_size, uint64_t block_size)
{
    //get table data
    Table t_date = db->get_table(std::string("date"));
    Table t_supplier = db->get_table(std::string("supplier"));
    Table t_part = db->get_table(std::string("part"));
    Table t_lineorder = db->get_table(std::string("lineorder"));

    uint64_t *d_datekey = (uint64_t *)t_date.get_column("d_datekey").get_data();
    uint64_t *p_partkey = (uint64_t *)t_part.get_column("p_partkey").get_data();
    std::string *p_category = (std::string *)t_part.get_column("p_category").get_data();
    uint64_t *s_suppkey = (uint64_t *)t_supplier.get_column("s_suppkey").get_data();
    std::string *s_region = (std::string *)t_supplier.get_column("s_region").get_data();
    uint64_t *lo_orderdate = (uint64_t *)t_lineorder.get_column("lo_orderdate").get_data();
    uint64_t *lo_partkey = (uint64_t *)t_lineorder.get_column("lo_partkey").get_data();
    uint64_t *lo_suppkey = (uint64_t *)t_lineorder.get_column("lo_suppkey").get_data();

    std::cout << "get data is finished." << std::endl;

    // process date table
    uint64_t d_size = t_date.get_column("d_datekey").get_size();
    uint64_t p_size = t_part.get_column("p_partkey").get_size();
    uint64_t s_size = t_supplier.get_column("s_region").get_size();
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

    uint64_t *p_result_ids = new uint64_t[p_size];
    uint64_t p_result_size = 0;
    // and p_category = 'MFGR#12'

    for (uint64_t i = 0; i < p_size; i++)
    {
        if (0 == std::string(p_category[i]).compare("MFGR#12"))
        {
            p_result_ids[p_result_size] = i;
            p_result_size++;
        }
    }

    //build data hash table
    uint64_t *part_hash_table = new uint64_t[part_hash_tabel_size];
    hash_build_rand(p_result_ids, p_partkey, p_result_size, part_hash_table, part_hash_tabel_size);

    //build data hash table
    uint64_t *date_hash_table = new uint64_t[date_hash_tabel_size];
    hash_build(d_datekey, d_size, date_hash_table, date_hash_tabel_size);

    // and s_region = 'AMERICA'
    uint64_t *s_result_ids = new uint64_t[s_size];
    uint64_t s_result_size = 0;

    for (uint64_t i = 0; i < s_size; i++)
    {
        if (0 == std::string(s_region[i]).compare("AMERICA"))
        {
            s_result_ids[s_result_size] = i;
            s_result_size++;
        }
    }

    uint64_t *supplier_hash_table = new uint64_t[supplier_hash_tabel_size];
    hash_build_rand(s_result_ids, s_suppkey, s_result_size, supplier_hash_table, supplier_hash_tabel_size);

    uint64_t *lo_result = new uint64_t[lo_size];
    uint64_t lo_result_size = 0;

    auto t1 = std::chrono::steady_clock::now();

    // uint64_t block_size = batch_size * 128; //  batch_size * 16;
    uint64_t rest = lo_size % block_size;

    uint64_t *block_buffer1 = new uint64_t[block_size * 2];
    uint64_t *block_buffer2 = new uint64_t[block_size * 2];

    uint64_t block_buffer_size1 = 0;
    uint64_t block_buffer_size2 = 0;

    uint64_t in_size1 = 0;
    uint64_t in_size2 = 0;

    uint64_t left_size1 = 0;
    uint64_t left_size2 = 0;

    for (uint64_t i = 0; i < lo_size - rest; i = i + block_size)
    {

        hashjoin_i64_l1(part_hash_table, part_hash_tabel_size, lo_rid + i, lo_partkey + i, block_size, batch_size, block_buffer1, block_buffer_size1);
        // hashjoin_i64_l1(part_hash_table, part_hash_tabel_size, lo_rid + i, lo_partkey + i, block_size, batch_size, lo_result, lo_result_size);

        if (block_buffer_size1 < block_size)
        {
            continue;
        }

        left_size1 = block_buffer_size1 % batch_size;
        in_size1 = block_buffer_size1 - left_size1;

        block_buffer_size1 = left_size1;

        hashjoin_gather_i64_l1(supplier_hash_table, supplier_hash_tabel_size, block_buffer1, lo_suppkey, in_size1, batch_size, block_buffer2, block_buffer_size2);
        // hashjoin_gather_i64_l1(supplier_hash_table, supplier_hash_tabel_size, block_buffer1, lo_suppkey, in_size1, batch_size, lo_result, lo_result_size);

        for (uint j = 0; j < left_size1; j++)
        {
            block_buffer1[j] = block_buffer1[j + in_size1];
        }

        if (block_buffer_size2 < block_size)
        {
            continue;
        }

        left_size2 = block_buffer_size2 % batch_size;
        in_size2 = block_buffer_size2 - left_size2;
        block_buffer_size2 = left_size2;

        hashjoin_gather_i64_l1(date_hash_table, date_hash_tabel_size, block_buffer2, lo_orderdate, in_size2, batch_size, lo_result, lo_result_size);

        for (uint j = 0; j < left_size2; j++)
        {
            block_buffer2[j] = block_buffer2[j + in_size2];
        }
    }

    for (uint64_t i = lo_size - rest; i < lo_size; i++)
    {
        if (hash_probe(part_hash_table, part_hash_tabel_size, lo_partkey[lo_rid[i]]))
        {
            if (hash_probe(supplier_hash_table, supplier_hash_tabel_size, lo_suppkey[lo_rid[i]]))
            {
                if (hash_probe(date_hash_table, date_hash_tabel_size, lo_orderdate[lo_rid[i]]))
                {
                    lo_result[lo_result_size] = lo_rid[i];
                    lo_result_size++;
                }
            }
        }
    }

    for (uint64_t i = 0; i < block_buffer_size1; i++)
    {
        if (hash_probe(supplier_hash_table, supplier_hash_tabel_size, lo_suppkey[block_buffer1[i]]))
        {
            if (hash_probe(date_hash_table, date_hash_tabel_size, lo_orderdate[block_buffer1[i]]))
            {
                lo_result[lo_result_size] = block_buffer1[i];
                lo_result_size++;
            }
        }
    }

    for (uint64_t i = 0; i < block_buffer_size2; i++)
    {
        if (hash_probe(date_hash_table, date_hash_tabel_size, lo_orderdate[block_buffer2[i]]))
        {
            lo_result[lo_result_size] = block_buffer1[i];
            lo_result_size++;
        }
    }

    auto t2 = std::chrono::steady_clock::now();
    double dr_ms = std::chrono::duration<double, std::milli>(t2 - t1).count();
    std::cout << "hi block finished, block size = " << block_size << ", use time " << dr_ms << " ms" << std::endl;
    std::cout << "lo_result_size = " << lo_result_size << std::endl;

    return dr_ms;
}

int main()
{
    SSB_DB *ssb_db = new SSB_DB();

    //load data
    ssb_db->load_data(std::string("/home/sunzw/data/ssb/scale_10"));
    DataBase *db = ssb_db->db;

    double runtime = 0.0;

    runtime = q21_scalar(db);
    std::cout << std::setiosflags(std::ios::fixed) << std::setprecision(6) << runtime << std::endl;
    sleep(30);

    runtime = q21_simd(db);
    std::cout << std::setiosflags(std::ios::fixed) << std::setprecision(6) << runtime << std::endl;
    sleep(30);

    runtime = q21_hi(db, 36);
    std::cout << std::setiosflags(std::ios::fixed) << std::setprecision(6) << runtime << std::endl;
    // sleep(30);

    // // for (int i = 1; i < 4096; i = i * 4)
    // {
    //     uint64_t block_size = 36 * 512;
    //     runtime = q21_hi_block(db, 36, block_size);
    //     std::cout << std::setiosflags(std::ios::fixed) << std::setprecision(6) << runtime << std::endl;
    //     // sleep(30);
    // }


    return 0;
}