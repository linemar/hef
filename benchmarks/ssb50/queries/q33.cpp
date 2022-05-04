/*
 * @Author: SunZewen
 * @Date: 2022-01-05 15:38:35
 * @LastEditors: SunZewen
 * @LastEditTime: 2022-01-23 18:00:18
 * @Description:   
 * @FilePath: /sunzw/workspace/ecnu/ecnu/hef/benchmarks/ssb50/queries/q33.cpp
 */

/*
Q2.1
select sum(lo_revenue), d_year, c_city
    from lineorder, date, part, supplier
    where lo_orderdate = d_datekey
        and lo_partkey = p_partkey
        and lo_suppkey = s_suppkey
        and c_city = 'MFGR#12'
        and s_city = 'UNITED STATES'
        group by d_year, c_city
        order by d_year, c_city;
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
#include "../../../include/common.hpp"
#include "../../../include/operator/operator.hpp"

const int customer_hash_tabel_size = 1024 * 1024;
const int supplier_hash_tabel_size = 64 * 1024;
const int date_hash_tabel_size = 64 * 1024;

uint64_t *d_datekey;
uint64_t *p_partkey;
std::string *p_category;
uint64_t *s_suppkey;
std::string *s_city;
uint64_t *lo_orderdate;
uint64_t *lo_partkey;
uint64_t *lo_suppkey;
uint64_t *lo_custkey;
uint64_t *d_year;
std::string *c_city;
uint64_t *c_custkey;

uint64_t d_size = 0;
uint64_t p_size = 0;
uint64_t s_size = 0;
uint64_t c_size = 0;
uint64_t lo_size = 0;

double q31_scalar()
{
   uint64_t *lo_rid = new uint64_t[lo_size];

   for (uint64_t i = 0; i < lo_size; i++)
   {
      lo_rid[i] = i;
   }

   uint64_t *d_result_ids = new uint64_t[d_size];
   uint64_t d_result_size = 0;
   for (uint64_t i = 0; i < d_size; i++)
   {
      if (d_year[i] >= 1992 && d_year[i] <= 1997)
      {
         d_result_ids[d_result_size] = i;
         d_result_size++;
      }
   }

   //build data hash table
   uint64_t *date_hash_table = new uint64_t[date_hash_tabel_size];
   hash_build_rand(d_result_ids, d_datekey, d_result_size, date_hash_table, date_hash_tabel_size);

   // std::cout << "start build hash table." << std::endl;
   // sleep(3);

   // and s_city = 'UNITED STATES'
   uint64_t *s_result_ids = new uint64_t[s_size];
   uint64_t s_result_size = 0;

   for (uint64_t i = 0; i < s_size; i++)
   {
      if ((0 == std::string(s_city[i]).compare("UNITED KI1")) || (0 == std::string(s_city[i]).compare("UNITED KI5")))
      {
         s_result_ids[s_result_size] = i;
         s_result_size++;
      }
   }

   uint64_t *supplier_hash_table = new uint64_t[supplier_hash_tabel_size];
   hash_build_rand(s_result_ids, s_suppkey, s_result_size, supplier_hash_table, supplier_hash_tabel_size);

   uint64_t *c_result_ids = new uint64_t[c_size];
   uint64_t c_result_size = 0;
   // and c_city = 'MFGR#12'

   for (uint64_t i = 0; i < c_size; i++)
   {
      if ((0 == std::string(c_city[i]).compare("UNITED KI1")) || (0 == std::string(c_city[i]).compare("UNITED KI5")))
      {
         c_result_ids[c_result_size] = i;
         c_result_size++;
      }
   }

   //build data hash table
   uint64_t *customer_hash_table = new uint64_t[customer_hash_tabel_size];
   hash_build_rand(c_result_ids, c_custkey, c_result_size, customer_hash_table, customer_hash_tabel_size);

   uint64_t *lo_result = new uint64_t[lo_size];
   uint64_t lo_result_size = 0;

   auto t1 = std::chrono::steady_clock::now();

   for (uint64_t i = 0; i < lo_size; i++)
   {
      if (hash_probe(customer_hash_table, customer_hash_tabel_size, lo_custkey[lo_rid[i]]))
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

   delete lo_rid;
   delete lo_result;
   delete c_result_ids;
   delete s_result_ids;
   delete customer_hash_table;
   delete supplier_hash_table;

   return dr_ms;
}

double q31_simd()
{
   uint64_t *lo_rid = new uint64_t[lo_size];

   for (uint64_t i = 0; i < lo_size; i++)
   {
      lo_rid[i] = i;
   }

   uint64_t *d_result_ids = new uint64_t[d_size];
   uint64_t d_result_size = 0;
   for (uint64_t i = 0; i < d_size; i++)
   {
      if (d_year[i] >= 1992 && d_year[i] <= 1997)
      {
         d_result_ids[d_result_size] = i;
         d_result_size++;
      }
   }

   //build data hash table
   uint64_t *date_hash_table = new uint64_t[date_hash_tabel_size];
   hash_build_rand(d_result_ids, d_datekey, d_result_size, date_hash_table, date_hash_tabel_size);

   // and s_city = 'UNITED STATES'
   uint64_t *s_result_ids = new uint64_t[s_size];
   uint64_t s_result_size = 0;

   for (uint64_t i = 0; i < s_size; i++)
   {
      if ((0 == std::string(s_city[i]).compare("UNITED KI1")) || (0 == std::string(s_city[i]).compare("UNITED KI5")))
      {
         s_result_ids[s_result_size] = i;
         s_result_size++;
      }
   }

   uint64_t *supplier_hash_table = new uint64_t[supplier_hash_tabel_size];
   hash_build_rand(s_result_ids, s_suppkey, s_result_size, supplier_hash_table, supplier_hash_tabel_size);

   uint64_t *c_result_ids = new uint64_t[c_size];
   uint64_t c_result_size = 0;
   // and c_city = 'MFGR#12'

   for (uint64_t i = 0; i < c_size; i++)
   {
      if ((0 == std::string(c_city[i]).compare("UNITED KI1")) || (0 == std::string(c_city[i]).compare("UNITED KI5")))
      {
         c_result_ids[c_result_size] = i;
         c_result_size++;
      }
   }

   //build data hash table
   uint64_t *customer_hash_table = new uint64_t[customer_hash_tabel_size];
   hash_build_rand(c_result_ids, c_custkey, c_result_size, customer_hash_table, customer_hash_tabel_size);

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
      __m512i src1 = _mm512_loadu_epi64(lo_custkey + i);
      __mmask8 mask1 = probe_avx512(customer_hash_table, customer_hash_tabel_size, src1);

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
      if (hash_probe(customer_hash_table, customer_hash_tabel_size, lo_custkey[lo_rid[i]]))
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

   delete lo_rid;
   delete lo_result;
   delete c_result_ids;
   delete s_result_ids;
   delete customer_hash_table;
   delete supplier_hash_table;

   return dr_ms;
}

double q31_hi(uint64_t batch_size)
{
   uint64_t *lo_rid = new uint64_t[lo_size];
   for (uint64_t i = 0; i < lo_size; i++)
   {
      lo_rid[i] = i;
   }

   uint64_t *d_result_ids = new uint64_t[d_size];
   uint64_t d_result_size = 0;
   for (uint64_t i = 0; i < d_size; i++)
   {
      if (d_year[i] >= 1992 && d_year[i] <= 1997)
      {
         d_result_ids[d_result_size] = i;
         d_result_size++;
      }
   }

   //build data hash table
   uint64_t *date_hash_table = new uint64_t[date_hash_tabel_size];
   hash_build_rand(d_result_ids, d_datekey, d_result_size, date_hash_table, date_hash_tabel_size);

   // and s_city = 'UNITED STATES'
   uint64_t *s_result_ids = new uint64_t[s_size];
   uint64_t s_result_size = 0;

   for (uint64_t i = 0; i < s_size; i++)
   {
      if ((0 == std::string(s_city[i]).compare("UNITED KI1")) || (0 == std::string(s_city[i]).compare("UNITED KI5")))
      {
         s_result_ids[s_result_size] = i;
         s_result_size++;
      }
   }

   uint64_t *supplier_hash_table = new uint64_t[supplier_hash_tabel_size];
   hash_build_rand(s_result_ids, s_suppkey, s_result_size, supplier_hash_table, supplier_hash_tabel_size);

   uint64_t *c_result_ids = new uint64_t[c_size];
   uint64_t c_result_size = 0;
   // and c_city = 'MFGR#12'

   for (uint64_t i = 0; i < c_size; i++)
   {
      if ((0 == std::string(c_city[i]).compare("UNITED KI1")) || (0 == std::string(c_city[i]).compare("UNITED KI5")))
      {
         c_result_ids[c_result_size] = i;
         c_result_size++;
      }
   }

   //build data hash table
   uint64_t *customer_hash_table = new uint64_t[customer_hash_tabel_size];
   hash_build_rand(c_result_ids, c_custkey, c_result_size, customer_hash_table, customer_hash_tabel_size);

   uint64_t *lo_result = new uint64_t[lo_size];
   uint64_t lo_result_size = 0;

   auto t1 = std::chrono::steady_clock::now();

   uint64_t block_size = batch_size * 128; //  batch_size * 16;
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

      hashjoin_i64_l1(customer_hash_table, customer_hash_tabel_size, lo_rid + i, lo_custkey + i, block_size, batch_size, block_buffer1, block_buffer_size1);
      // hashjoin_i64_l1(customer_hash_table, customer_hash_tabel_size, lo_rid + i, lo_partkey + i, block_size, batch_size, lo_result, lo_result_size);

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
      if (hash_probe(customer_hash_table, customer_hash_tabel_size, lo_custkey[lo_rid[i]]))
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
   std::cout << "hi finished, use time " << dr_ms << " ms" << std::endl;
   std::cout << "lo_result_size = " << lo_result_size << std::endl;

   delete lo_rid;
   delete lo_result;
   delete c_result_ids;
   delete s_result_ids;
   delete customer_hash_table;
   delete supplier_hash_table;

   return dr_ms;
}

double q31_hi_block(uint64_t batch_size)
{
   uint64_t *lo_rid = new uint64_t[lo_size];

   for (uint64_t i = 0; i < lo_size; i++)
   {
      lo_rid[i] = i;
   }

   uint64_t *d_result_ids = new uint64_t[d_size];
   uint64_t d_result_size = 0;
   for (uint64_t i = 0; i < d_size; i++)
   {
      if (d_year[i] >= 1992 && d_year[i] <= 1997)
      {
         d_result_ids[d_result_size] = i;
         d_result_size++;
      }
   }

   //build data hash table
   uint64_t *date_hash_table = new uint64_t[date_hash_tabel_size];
   hash_build_rand(d_result_ids, d_datekey, d_result_size, date_hash_table, date_hash_tabel_size);

   // and s_city = 'UNITED STATES'
   uint64_t *s_result_ids = new uint64_t[s_size];
   uint64_t s_result_size = 0;

   for (uint64_t i = 0; i < s_size; i++)
   {
      if ((0 == std::string(s_city[i]).compare("UNITED KI1")) || (0 == std::string(s_city[i]).compare("UNITED KI5")))
      {
         s_result_ids[s_result_size] = i;
         s_result_size++;
      }
   }

   uint64_t *supplier_hash_table = new uint64_t[supplier_hash_tabel_size];
   hash_build_rand(s_result_ids, s_suppkey, s_result_size, supplier_hash_table, supplier_hash_tabel_size);

   uint64_t *c_result_ids = new uint64_t[c_size];
   uint64_t c_result_size = 0;
   // and c_city = 'MFGR#12'

   for (uint64_t i = 0; i < c_size; i++)
   {
      if ((0 == std::string(c_city[i]).compare("UNITED KI1")) || (0 == std::string(c_city[i]).compare("UNITED KI5")))
      {
         c_result_ids[c_result_size] = i;
         c_result_size++;
      }
   }

   //build data hash table
   uint64_t *customer_hash_table = new uint64_t[customer_hash_tabel_size];
   hash_build_rand(c_result_ids, c_custkey, c_result_size, customer_hash_table, customer_hash_tabel_size);

   uint64_t *lo_result = new uint64_t[lo_size];
   uint64_t lo_result_size = 0;

   auto t1 = std::chrono::steady_clock::now();

   uint64_t block_size = batch_size * 128; //  batch_size * 16;
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

      hashjoin_i64_l1(customer_hash_table, customer_hash_tabel_size, lo_rid + i, lo_custkey + i, block_size, batch_size, block_buffer1, block_buffer_size1);
      // hashjoin_i64_l1(customer_hash_table, customer_hash_tabel_size, lo_rid + i, lo_partkey + i, block_size, batch_size, lo_result, lo_result_size);

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
      if (hash_probe(customer_hash_table, customer_hash_tabel_size, lo_custkey[lo_rid[i]]))
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
   std::cout << "hi block finished, use time " << dr_ms << " ms" << std::endl;
   std::cout << "lo_result_size = " << lo_result_size << std::endl;

   delete lo_rid;
   delete lo_result;
   delete c_result_ids;
   delete s_result_ids;
   delete customer_hash_table;
   delete supplier_hash_table;

   return dr_ms;
}

int main()
{
   uint64_t size = 0;

   d_size = 0;
   p_size = 0;
   s_size = 0;
   lo_size = 0;

   //load data
   std::vector<uint64_t> int_data1;
   d_size = load_data_int(int_data1, std::string("/home/sunzw/data/ssb/scale_100/date_key.txt"));
   d_datekey = int_data1.data();
   std::cout << "d_size = " << d_size << std::endl;

   std::vector<uint64_t> int_data2;
   c_size = load_data_int(int_data2, std::string("/home/sunzw/data/ssb/scale_100/customer_key.txt"));
   c_custkey = int_data2.data();
   std::cout << "c_size = " << c_size << std::endl;

   std::vector<std::string> str_data1;
   size = load_data_str(str_data1, std::string("/home/sunzw/data/ssb/scale_100/customer_city.txt"));
   c_city = str_data1.data();

   std::vector<uint64_t> int_data3;
   s_size = load_data_int(int_data3, std::string("/home/sunzw/data/ssb/scale_100/supplier_key.txt"));
   s_suppkey = int_data3.data();
   std::cout << "s_size = " << s_size << std::endl;

   std::vector<std::string> str_data2;
   size = load_data_str(str_data2, std::string("/home/sunzw/data/ssb/scale_100/supplier_city.txt"));
   s_city = str_data2.data();

   std::vector<uint64_t> int_data4;
   lo_size = load_data_int(int_data4, std::string("/home/sunzw/data/ssb/scale_100/lineorder_date.txt"));
   lo_orderdate = int_data4.data();
   std::cout << "lo_size = " << lo_size << std::endl;

   std::vector<uint64_t> int_data5;
   size = load_data_int(int_data5, std::string("/home/sunzw/data/ssb/scale_100/lineorder_customer.txt"));
   lo_custkey = int_data5.data();

   std::vector<uint64_t> int_data6;
   size = load_data_int(int_data6, std::string("/home/sunzw/data/ssb/scale_100/lineorder_supplier.txt"));
   lo_suppkey = int_data6.data();

   std::vector<uint64_t> int_data7;
   size = load_data_int(int_data7, std::string("/home/sunzw/data/ssb/scale_100/date_year.txt"));
   d_year = int_data7.data();

   std::cout << "get data is finished." << std::endl;
   double runtime = 0.0;

   runtime = q31_scalar();
   std::cout << std::setiosflags(std::ios::fixed) << std::setprecision(6) << runtime << std::endl;
   sleep(30);

   runtime = q31_simd();
   std::cout << std::setiosflags(std::ios::fixed) << std::setprecision(6) << runtime << std::endl;
   sleep(30);

   runtime = q31_hi(36);
   std::cout << std::setiosflags(std::ios::fixed) << std::setprecision(6) << runtime << std::endl;
   // sleep(30);

   // runtime = q31_hi_block( 36);
   // std::cout << std::setiosflags(std::ios::fixed) << std::setprecision(6) << runtime << std::endl;

   return 0;
}