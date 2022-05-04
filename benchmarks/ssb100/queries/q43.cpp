/*
 * @Author: SunZewen
 * @Date: 2022-01-05 15:38:35
 * @LastEditors: SunZewen
 * @LastEditTime: 2022-01-23 17:20:08
 * @Description:   
 * @FilePath: /sunzw/workspace/ecnu/ecnu/hef/benchmarks/ssb50/queries/q43.cpp
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

const int supplier_hash_tabel_size = 64 * 1024;
const int customer_hash_tabel_size = 8 * 1024 * 1024;
const int part_hash_tabel_size = 1024 * 1024;
const int date_hash_tabel_size = 16 * 1024;

uint64_t *d_datekey;
uint64_t *p_partkey;
std::string *p_category;
uint64_t *s_suppkey;
std::string *s_nation;
uint64_t *lo_orderdate;
uint64_t *lo_partkey;
uint64_t *lo_suppkey;
uint64_t *lo_custkey;
uint64_t *d_year;
std::string *c_region;
uint64_t *c_custkey;

uint64_t d_size = 0;
uint64_t p_size = 0;
uint64_t s_size = 0;
uint64_t c_size = 0;
uint64_t lo_size = 0;

double q43_scalar()
{
   uint64_t *lo_rid = new uint64_t[lo_size];

   for (uint64_t i = 0; i < lo_size; i++)
   {
      lo_rid[i] = i;
   }

   uint64_t *p_result_ids = new uint64_t[p_size];
   uint64_t p_result_size = 0;
   // and p_category = 'MFGR#12'

   for (uint64_t i = 0; i < p_size; i++)
   {
      if (0 == std::string(p_category[i]).compare("MFGR#14"))
      {
         p_result_ids[p_result_size] = i;
         p_result_size++;
      }
   }

   //build data hash table
   uint64_t *part_hash_table = new uint64_t[part_hash_tabel_size];
   hash_build_rand(p_result_ids, p_partkey, p_result_size, part_hash_table, part_hash_tabel_size);

   // and s_nation = 'AMERICA'
   uint64_t *s_result_ids = new uint64_t[s_size];
   uint64_t s_result_size = 0;

   for (uint64_t i = 0; i < s_size; i++)
   {
      if (0 == std::string(s_nation[i]).compare("UNITED STATES"))
      {
         s_result_ids[s_result_size] = i;
         s_result_size++;
      }
   }
   //build data hash table
   uint64_t *d_result_ids = new uint64_t[d_size];
   uint64_t d_result_size = 0;
   // and c_region = 'MFGR#12'

   for (uint64_t i = 0; i < d_size; i++)
   {
      if (d_year[i] == 1997 || d_year[i] == 1998)
      {
         d_result_ids[d_result_size] = i;
         d_result_size++;
      }
   }

   //build data hash table
   uint64_t *date_hash_table = new uint64_t[date_hash_tabel_size];
   hash_build_rand(d_result_ids, d_datekey, d_result_size, date_hash_table, date_hash_tabel_size);

   uint64_t *c_result_ids = new uint64_t[c_size];
   uint64_t c_result_size = 0;
   // and c_region = 'MFGR#12'

   for (uint64_t i = 0; i < c_size; i++)
   {
      if (0 == std::string(c_region[i]).compare("AMERICA"))
      {
         c_result_ids[c_result_size] = i;
         c_result_size++;
      }
   }

   //build data hash table
   uint64_t *customer_hash_table = new uint64_t[customer_hash_tabel_size];
   hash_build_rand(c_result_ids, c_custkey, c_result_size, customer_hash_table, customer_hash_tabel_size);

   uint64_t *supplier_hash_table = new uint64_t[supplier_hash_tabel_size];
   hash_build_rand(s_result_ids, s_suppkey, s_result_size, supplier_hash_table, supplier_hash_tabel_size);

   uint64_t *lo_result = new uint64_t[lo_size];
   uint64_t lo_result_size = 0;

   auto t1 = std::chrono::steady_clock::now();

   for (uint64_t i = 0; i < lo_size; i++)
   {
      if (hash_probe(supplier_hash_table, supplier_hash_tabel_size, lo_suppkey[lo_rid[i]]))
      {
         if (hash_probe(customer_hash_table, customer_hash_tabel_size, lo_custkey[lo_rid[i]]))
         {
            if (hash_probe(part_hash_table, part_hash_tabel_size, lo_partkey[lo_rid[i]]))
            {
               if (hash_probe(date_hash_table, date_hash_tabel_size, lo_orderdate[lo_rid[i]]))
               {
                  lo_result[lo_result_size] = lo_rid[i];
                  lo_result_size++;
               }
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

double q43_simd()
{
   uint64_t *lo_rid = new uint64_t[lo_size];

   for (uint64_t i = 0; i < lo_size; i++)
   {
      lo_rid[i] = i;
   }

   //build data hash table
   uint64_t *d_result_ids = new uint64_t[d_size];
   uint64_t d_result_size = 0;
   // and c_region = 'MFGR#12'

   for (uint64_t i = 0; i < d_size; i++)
   {
      if (d_year[i] == 1997 || d_year[i] == 1998)
      {
         d_result_ids[d_result_size] = i;
         d_result_size++;
      }
   }

   //build data hash table
   uint64_t *date_hash_table = new uint64_t[date_hash_tabel_size];
   hash_build_rand(d_result_ids, d_datekey, d_result_size, date_hash_table, date_hash_tabel_size);

   uint64_t *p_result_ids = new uint64_t[p_size];
   uint64_t p_result_size = 0;
   // and p_category = 'MFGR#12'

   for (uint64_t i = 0; i < p_size; i++)
   {
      if (0 == std::string(p_category[i]).compare("MFGR#14"))
      {
         p_result_ids[p_result_size] = i;
         p_result_size++;
      }
   }

   //build data hash table
   uint64_t *part_hash_table = new uint64_t[part_hash_tabel_size];
   hash_build_rand(p_result_ids, p_partkey, p_result_size, part_hash_table, part_hash_tabel_size);

   // and s_nation = 'AMERICA'
   uint64_t *s_result_ids = new uint64_t[s_size];
   uint64_t s_result_size = 0;

   for (uint64_t i = 0; i < s_size; i++)
   {
      if (0 == std::string(s_nation[i]).compare("UNITED STATES"))
      {
         s_result_ids[s_result_size] = i;
         s_result_size++;
      }
   }

   uint64_t *supplier_hash_table = new uint64_t[supplier_hash_tabel_size];
   hash_build_rand(s_result_ids, s_suppkey, s_result_size, supplier_hash_table, supplier_hash_tabel_size);

   uint64_t *c_result_ids = new uint64_t[c_size];
   uint64_t c_result_size = 0;
   // and c_region = 'MFGR#12'

   for (uint64_t i = 0; i < c_size; i++)
   {
      if (0 == std::string(c_region[i]).compare("AMERICA"))
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
   uint64_t *block_buffer3 = new uint64_t[16];

   uint64_t block_buffer_size1 = 0;
   uint64_t block_buffer_size2 = 0;
   uint64_t block_buffer_size3 = 0;

   uint64_t in_size1 = 0;
   uint64_t in_size2 = 0;
   uint64_t in_size3 = 0;

   uint64_t left_size1 = 0;
   uint64_t left_size2 = 0;
   uint64_t left_size3 = 0;

   for (uint64_t i = 0; i < lo_size - rest; i = i + 8)
   {
      __m512i src1 = _mm512_loadu_epi64(lo_suppkey + i);
      __mmask8 mask1 = probe_avx512(supplier_hash_table, supplier_hash_tabel_size, src1);

      __m512i rids = _mm512_loadu_epi64(lo_rid + i);
      _mm512_mask_compressstoreu_epi64(block_buffer1 + block_buffer_size1, mask1, rids);
      block_buffer_size1 += _mm_popcnt_u64(mask1);

      if (block_buffer_size1 < 8)
      {
         continue;
      }

      block_buffer_size1 = block_buffer_size1 - 8;

      __m512i index2 = _mm512_loadu_epi64(block_buffer1 + block_buffer_size1);
      __m512i src2 = _mm512_i64gather_epi64(index2, lo_custkey, 8);
      __mmask8 mask2 = probe_avx512(customer_hash_table, customer_hash_tabel_size, src2);
      _mm512_mask_compressstoreu_epi64(block_buffer2 + block_buffer_size2, mask2, index2);
      block_buffer_size2 += _mm_popcnt_u64(mask2);

      if (block_buffer_size2 < 8)
      {
         continue;
      }

      block_buffer_size2 = block_buffer_size2 - 8;

      __m512i index3 = _mm512_loadu_epi64(block_buffer2 + block_buffer_size2);

      __m512i src3 = _mm512_i64gather_epi64(index3, lo_partkey, 8);
      __mmask8 mask3 = probe_avx512(part_hash_table, part_hash_tabel_size, src3);
      _mm512_mask_compressstoreu_epi64(block_buffer3 + block_buffer_size3, mask3, index3);
      block_buffer_size3 += _mm_popcnt_u64(mask3);

      if (block_buffer_size3 < 8)
      {
         continue;
      }

      block_buffer_size3 = block_buffer_size3 - 8;

      __m512i index4 = _mm512_loadu_epi64(block_buffer3 + block_buffer_size3);
      __m512i src4 = _mm512_i64gather_epi64(index4, lo_orderdate, 8);
      __mmask8 mask4 = probe_avx512(date_hash_table, date_hash_tabel_size, src4);

      _mm512_mask_compressstoreu_epi64(lo_result + lo_result_size, mask4, index4);
      lo_result_size += _mm_popcnt_u64(mask4);
   }

   for (uint64_t i = lo_size - rest; i < lo_size; i++)
   {
      if (hash_probe(supplier_hash_table, supplier_hash_tabel_size, lo_suppkey[lo_rid[i]]))
      {
         if (hash_probe(customer_hash_table, customer_hash_tabel_size, lo_custkey[lo_rid[i]]))
         {
            if (hash_probe(part_hash_table, part_hash_tabel_size, lo_partkey[lo_rid[i]]))
            {
               if (hash_probe(date_hash_table, date_hash_tabel_size, lo_orderdate[lo_rid[i]]))
               {
                  lo_result[lo_result_size] = lo_rid[i];
                  lo_result_size++;
               }
            }
         }
      }
   }

   for (uint64_t i = 0; i < block_buffer_size1; i++)
   {
      if (hash_probe(customer_hash_table, customer_hash_tabel_size, lo_custkey[block_buffer1[i]]))
      {
         if (hash_probe(part_hash_table, part_hash_tabel_size, lo_partkey[block_buffer1[i]]))
         {
            if (hash_probe(date_hash_table, date_hash_tabel_size, lo_orderdate[block_buffer1[i]]))
            {
               lo_result[lo_result_size] = block_buffer1[i];
               lo_result_size++;
            }
         }
      }
   }

   for (uint64_t i = 0; i < block_buffer_size2; i++)
   {
      if (hash_probe(part_hash_table, part_hash_tabel_size, lo_partkey[block_buffer2[i]]))
      {
         if (hash_probe(date_hash_table, date_hash_tabel_size, lo_orderdate[block_buffer2[i]]))
         {
            lo_result[lo_result_size] = block_buffer2[i];
            lo_result_size++;
         }
      }
   }

   for (uint64_t i = 0; i < block_buffer_size3; i++)
   {
      if (hash_probe(date_hash_table, date_hash_tabel_size, lo_orderdate[block_buffer3[i]]))
      {
         lo_result[lo_result_size] = block_buffer3[i];
         lo_result_size++;
      }
   }
   /**/
   auto t2 = std::chrono::steady_clock::now();
   double dr_ms = std::chrono::duration<double, std::milli>(t2 - t1).count();
   std::cout << "simd finished, use time " << dr_ms << " ms" << std::endl;
   std::cout << "lo_result_size = " << lo_result_size << std::endl;

   return dr_ms;
}

double q43_hi(uint64_t batch_size)
{
   uint64_t *lo_rid = new uint64_t[lo_size];

   for (uint64_t i = 0; i < lo_size; i++)
   {
      lo_rid[i] = i;
   }

   uint64_t *p_result_ids = new uint64_t[p_size];
   uint64_t p_result_size = 0;
   // and p_category = 'MFGR#12'

   for (uint64_t i = 0; i < p_size; i++)
   {
      if (0 == std::string(p_category[i]).compare("MFGR#14"))
      {
         p_result_ids[p_result_size] = i;
         p_result_size++;
      }
   }

   //build data hash table
   uint64_t *part_hash_table = new uint64_t[part_hash_tabel_size];
   hash_build_rand(p_result_ids, p_partkey, p_result_size, part_hash_table, part_hash_tabel_size);

   // and s_nation = 'AMERICA'
   uint64_t *s_result_ids = new uint64_t[s_size];
   uint64_t s_result_size = 0;

   for (uint64_t i = 0; i < s_size; i++)
   {
      if (0 == std::string(s_nation[i]).compare("UNITED STATES"))
      {
         s_result_ids[s_result_size] = i;
         s_result_size++;
      }
   }

   uint64_t *d_result_ids = new uint64_t[d_size];
   uint64_t d_result_size = 0;
   // and c_region = 'MFGR#12'

   for (uint64_t i = 0; i < d_size; i++)
   {
      if (d_year[i] == 1997 || d_year[i] == 1998)
      {
         d_result_ids[d_result_size] = i;
         d_result_size++;
      }
   }

   //build data hash table
   uint64_t *date_hash_table = new uint64_t[date_hash_tabel_size];
   hash_build_rand(d_result_ids, d_datekey, d_result_size, date_hash_table, date_hash_tabel_size);

   uint64_t *c_result_ids = new uint64_t[c_size];
   uint64_t c_result_size = 0;
   // and c_region = 'MFGR#12'

   for (uint64_t i = 0; i < c_size; i++)
   {
      if (0 == std::string(c_region[i]).compare("AMERICA"))
      {
         c_result_ids[c_result_size] = i;
         c_result_size++;
      }
   }

   //build data hash table
   uint64_t *customer_hash_table = new uint64_t[customer_hash_tabel_size];
   hash_build_rand(c_result_ids, c_custkey, c_result_size, customer_hash_table, customer_hash_tabel_size);

   uint64_t *supplier_hash_table = new uint64_t[supplier_hash_tabel_size];
   hash_build_rand(s_result_ids, s_suppkey, s_result_size, supplier_hash_table, supplier_hash_tabel_size);

   uint64_t *lo_result = new uint64_t[lo_size];
   uint64_t lo_result_size = 0;

   auto t1 = std::chrono::steady_clock::now();

   uint64_t block_size = batch_size; //  batch_size * 16;
   uint64_t rest = lo_size % block_size;

   uint64_t *block_buffer1 = new uint64_t[block_size * 2];
   uint64_t *block_buffer2 = new uint64_t[block_size * 2];
   uint64_t *block_buffer3 = new uint64_t[block_size * 2];

   uint64_t block_buffer_size1 = 0;
   uint64_t block_buffer_size2 = 0;
   uint64_t block_buffer_size3 = 0;

   uint64_t in_size1 = 0;
   uint64_t in_size2 = 0;
   uint64_t in_size3 = 0;

   uint64_t left_size1 = 0;
   uint64_t left_size2 = 0;
   uint64_t left_size3 = 0;

   for (uint64_t i = 0; i < lo_size - rest; i = i + block_size)
   {
      hashjoin_i64_l1(supplier_hash_table, supplier_hash_tabel_size, lo_rid + i, lo_suppkey + i, block_size, batch_size, block_buffer1, block_buffer_size1);

      if (block_buffer_size1 < block_size)
      {
         continue;
      }

      left_size1 = block_buffer_size1 % batch_size;
      in_size1 = block_buffer_size1 - left_size1;

      block_buffer_size1 = left_size1;

      hashjoin_gather_i64_l1(customer_hash_table, customer_hash_tabel_size, block_buffer1, lo_custkey, in_size1, batch_size, block_buffer2, block_buffer_size2);

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

      hashjoin_gather_i64_l1(part_hash_table, part_hash_tabel_size, block_buffer2, lo_partkey, in_size2, batch_size, block_buffer3, block_buffer_size3);

      for (uint j = 0; j < left_size2; j++)
      {
         block_buffer2[j] = block_buffer2[j + in_size2];
      }

      if (block_buffer_size3 < block_size)
      {
         continue;
      }

      left_size3 = block_buffer_size3 % batch_size;
      in_size3 = block_buffer_size3 - left_size3;
      block_buffer_size3 = left_size3;

      hashjoin_gather_i64_l1(date_hash_table, date_hash_tabel_size, block_buffer3, lo_orderdate, in_size3, batch_size, lo_result, lo_result_size);

      for (uint j = 0; j < left_size3; j++)
      {
         block_buffer3[j] = block_buffer3[j + in_size3];
      }
   }

   for (uint64_t i = lo_size - rest; i < lo_size; i++)
   {
      if (hash_probe(supplier_hash_table, supplier_hash_tabel_size, lo_suppkey[lo_rid[i]]))
      {
         if (hash_probe(customer_hash_table, customer_hash_tabel_size, lo_custkey[lo_rid[i]]))
         {
            if (hash_probe(part_hash_table, part_hash_tabel_size, lo_partkey[lo_rid[i]]))
            {
               if (hash_probe(date_hash_table, date_hash_tabel_size, lo_orderdate[lo_rid[i]]))
               {
                  lo_result[lo_result_size] = lo_rid[i];
                  lo_result_size++;
               }
            }
         }
      }
   }

   for (uint64_t i = 0; i < block_buffer_size1; i++)
   {
      if (hash_probe(customer_hash_table, customer_hash_tabel_size, lo_custkey[block_buffer1[i]]))
      {
         if (hash_probe(part_hash_table, part_hash_tabel_size, lo_partkey[block_buffer1[i]]))
         {
            if (hash_probe(date_hash_table, date_hash_tabel_size, lo_orderdate[block_buffer1[i]]))
            {
               lo_result[lo_result_size] = block_buffer1[i];
               lo_result_size++;
            }
         }
      }
   }

   for (uint64_t i = 0; i < block_buffer_size2; i++)
   {
      if (hash_probe(part_hash_table, part_hash_tabel_size, lo_partkey[block_buffer2[i]]))
      {
         if (hash_probe(date_hash_table, date_hash_tabel_size, lo_orderdate[block_buffer2[i]]))
         {
            lo_result[lo_result_size] = block_buffer2[i];
            lo_result_size++;
         }
      }
   }

   for (uint64_t i = 0; i < block_buffer_size3; i++)
   {
      if (hash_probe(date_hash_table, date_hash_tabel_size, lo_orderdate[block_buffer3[i]]))
      {
         lo_result[lo_result_size] = block_buffer3[i];
         lo_result_size++;
      }
   }

   auto t2 = std::chrono::steady_clock::now();
   double dr_ms = std::chrono::duration<double, std::milli>(t2 - t1).count();
   std::cout << "hi finished, use time " << dr_ms << " ms" << std::endl;
   std::cout << "lo_result_size = " << lo_result_size << std::endl;

   return dr_ms;
}

double q43_hi_block(uint64_t batch_size)
{
   uint64_t *lo_rid = new uint64_t[lo_size];

   for (uint64_t i = 0; i < lo_size; i++)
   {
      lo_rid[i] = i;
   }

   uint64_t *p_result_ids = new uint64_t[p_size];
   uint64_t p_result_size = 0;
   // and p_category = 'MFGR#12'

   for (uint64_t i = 0; i < p_size; i++)
   {
      if (0 == std::string(p_category[i]).compare("MFGR#14"))
      {
         p_result_ids[p_result_size] = i;
         p_result_size++;
      }
   }

   //build data hash table
   uint64_t *part_hash_table = new uint64_t[part_hash_tabel_size];
   hash_build_rand(p_result_ids, p_partkey, p_result_size, part_hash_table, part_hash_tabel_size);

   // and s_nation = 'AMERICA'
   uint64_t *s_result_ids = new uint64_t[s_size];
   uint64_t s_result_size = 0;

   for (uint64_t i = 0; i < s_size; i++)
   {
      if (0 == std::string(s_nation[i]).compare("UNITED STATES"))
      {
         s_result_ids[s_result_size] = i;
         s_result_size++;
      }
   }

   uint64_t *d_result_ids = new uint64_t[d_size];
   uint64_t d_result_size = 0;
   // and c_region = 'MFGR#12'

   for (uint64_t i = 0; i < d_size; i++)
   {
      if (d_year[i] == 1997 || d_year[i] == 1998)
      {
         d_result_ids[d_result_size] = i;
         d_result_size++;
      }
   }

   //build data hash table
   uint64_t *date_hash_table = new uint64_t[date_hash_tabel_size];
   hash_build_rand(d_result_ids, d_datekey, d_result_size, date_hash_table, date_hash_tabel_size);

   uint64_t *c_result_ids = new uint64_t[c_size];
   uint64_t c_result_size = 0;
   // and c_region = 'MFGR#12'

   for (uint64_t i = 0; i < c_size; i++)
   {
      if (0 == std::string(c_region[i]).compare("AMERICA"))
      {
         c_result_ids[c_result_size] = i;
         c_result_size++;
      }
   }

   //build data hash table
   uint64_t *customer_hash_table = new uint64_t[customer_hash_tabel_size];
   hash_build_rand(c_result_ids, c_custkey, c_result_size, customer_hash_table, customer_hash_tabel_size);

   uint64_t *supplier_hash_table = new uint64_t[supplier_hash_tabel_size];
   hash_build_rand(s_result_ids, s_suppkey, s_result_size, supplier_hash_table, supplier_hash_tabel_size);

   uint64_t *lo_result = new uint64_t[lo_size];
   uint64_t lo_result_size = 0;

   auto t1 = std::chrono::steady_clock::now();

   uint64_t block_size = batch_size * 128; //  batch_size * 16;
   uint64_t rest = lo_size % block_size;

   uint64_t *block_buffer1 = new uint64_t[block_size * 2];
   uint64_t *block_buffer2 = new uint64_t[block_size * 2];
   uint64_t *block_buffer3 = new uint64_t[block_size * 2];

   uint64_t block_buffer_size1 = 0;
   uint64_t block_buffer_size2 = 0;
   uint64_t block_buffer_size3 = 0;

   uint64_t in_size1 = 0;
   uint64_t in_size2 = 0;
   uint64_t in_size3 = 0;

   uint64_t left_size1 = 0;
   uint64_t left_size2 = 0;
   uint64_t left_size3 = 0;

   for (uint64_t i = 0; i < lo_size - rest; i = i + block_size)
   {
      hashjoin_i64_l1(supplier_hash_table, supplier_hash_tabel_size, lo_rid + i, lo_suppkey + i, block_size, batch_size, block_buffer1, block_buffer_size1);

      if (block_buffer_size1 < block_size)
      {
         continue;
      }

      left_size1 = block_buffer_size1 % batch_size;
      in_size1 = block_buffer_size1 - left_size1;

      block_buffer_size1 = left_size1;

      hashjoin_gather_i64_l1(customer_hash_table, customer_hash_tabel_size, block_buffer1, lo_custkey, in_size1, batch_size, block_buffer2, block_buffer_size2);

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

      hashjoin_gather_i64_l1(part_hash_table, part_hash_tabel_size, block_buffer2, lo_partkey, in_size2, batch_size, block_buffer3, block_buffer_size3);

      for (uint j = 0; j < left_size2; j++)
      {
         block_buffer2[j] = block_buffer2[j + in_size2];
      }

      if (block_buffer_size3 < block_size)
      {
         continue;
      }

      left_size3 = block_buffer_size3 % batch_size;
      in_size3 = block_buffer_size3 - left_size3;
      block_buffer_size3 = left_size3;

      hashjoin_gather_i64_l1(date_hash_table, date_hash_tabel_size, block_buffer3, lo_orderdate, in_size3, batch_size, lo_result, lo_result_size);

      for (uint j = 0; j < left_size3; j++)
      {
         block_buffer3[j] = block_buffer3[j + in_size3];
      }
   }

   for (uint64_t i = lo_size - rest; i < lo_size; i++)
   {
      if (hash_probe(supplier_hash_table, supplier_hash_tabel_size, lo_suppkey[lo_rid[i]]))
      {
         if (hash_probe(customer_hash_table, customer_hash_tabel_size, lo_custkey[lo_rid[i]]))
         {
            if (hash_probe(part_hash_table, part_hash_tabel_size, lo_partkey[lo_rid[i]]))
            {
               if (hash_probe(date_hash_table, date_hash_tabel_size, lo_orderdate[lo_rid[i]]))
               {
                  lo_result[lo_result_size] = lo_rid[i];
                  lo_result_size++;
               }
            }
         }
      }
   }

   for (uint64_t i = 0; i < block_buffer_size1; i++)
   {
      if (hash_probe(customer_hash_table, customer_hash_tabel_size, lo_custkey[block_buffer1[i]]))
      {
         if (hash_probe(part_hash_table, part_hash_tabel_size, lo_partkey[block_buffer1[i]]))
         {
            if (hash_probe(date_hash_table, date_hash_tabel_size, lo_orderdate[block_buffer1[i]]))
            {
               lo_result[lo_result_size] = block_buffer1[i];
               lo_result_size++;
            }
         }
      }
   }

   for (uint64_t i = 0; i < block_buffer_size2; i++)
   {
      if (hash_probe(part_hash_table, part_hash_tabel_size, lo_partkey[block_buffer2[i]]))
      {
         if (hash_probe(date_hash_table, date_hash_tabel_size, lo_orderdate[block_buffer2[i]]))
         {
            lo_result[lo_result_size] = block_buffer2[i];
            lo_result_size++;
         }
      }
   }

   for (uint64_t i = 0; i < block_buffer_size3; i++)
   {
      if (hash_probe(date_hash_table, date_hash_tabel_size, lo_orderdate[block_buffer3[i]]))
      {
         lo_result[lo_result_size] = block_buffer3[i];
         lo_result_size++;
      }
   }

   auto t2 = std::chrono::steady_clock::now();
   double dr_ms = std::chrono::duration<double, std::milli>(t2 - t1).count();
   std::cout << "hi block finished, use time " << dr_ms << " ms" << std::endl;
   std::cout << "lo_result_size = " << lo_result_size << std::endl;

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
   d_size = load_data_int(int_data1, std::string("/home/sunzw/data/ssb/scale_50_spilt/date_key.txt"));
   d_datekey = int_data1.data();
   std::cout << "d_size = " << d_size << std::endl;

   std::vector<uint64_t> int_data2;
   p_size = load_data_int(int_data2, std::string("/home/sunzw/data/ssb/scale_50_spilt/part_key.txt"));
   p_partkey = int_data2.data();
   std::cout << "p_size = " << p_size << std::endl;

   std::vector<std::string> str_data1;
   size = load_data_str(str_data1, std::string("/home/sunzw/data/ssb/scale_50_spilt/part_category.txt"));
   p_category = str_data1.data();

   std::vector<uint64_t> int_data3;
   s_size = load_data_int(int_data3, std::string("/home/sunzw/data/ssb/scale_50_spilt/supplier_key.txt"));
   s_suppkey = int_data3.data();
   std::cout << "s_size = " << s_size << std::endl;

   std::vector<std::string> str_data2;
   size = load_data_str(str_data2, std::string("/home/sunzw/data/ssb/scale_50_spilt/supplier_nation.txt"));
   s_nation = str_data2.data();

   std::vector<uint64_t> int_data4;
   lo_size = load_data_int(int_data4, std::string("/home/sunzw/data/ssb/scale_50_spilt/lineorder_date.txt"));
   lo_orderdate = int_data4.data();
   std::cout << "lo_size = " << lo_size << std::endl;

   std::vector<uint64_t> int_data5;
   size = load_data_int(int_data5, std::string("/home/sunzw/data/ssb/scale_50_spilt/lineorder_part.txt"));
   lo_partkey = int_data5.data();

   std::vector<uint64_t> int_data6;
   size = load_data_int(int_data6, std::string("/home/sunzw/data/ssb/scale_50_spilt/lineorder_supplier.txt"));
   lo_suppkey = int_data6.data();

   std::vector<uint64_t> int_data7;
   size = load_data_int(int_data7, std::string("/home/sunzw/data/ssb/scale_50_spilt/date_year.txt"));
   d_year = int_data7.data();

   std::vector<uint64_t> int_data9;
   c_size = load_data_int(int_data9, std::string("/home/sunzw/data/ssb/scale_50_spilt/customer_key.txt"));
   c_custkey = int_data9.data();
   std::cout << "c_size = " << c_size << std::endl;

   std::vector<std::string> str_data3;
   size = load_data_str(str_data3, std::string("/home/sunzw/data/ssb/scale_50_spilt/customer_region.txt"));
   c_region = str_data3.data();

   std::vector<uint64_t> int_data8;
   size = load_data_int(int_data8, std::string("/home/sunzw/data/ssb/scale_50_spilt/lineorder_customer.txt"));
   lo_custkey = int_data8.data();

   std::cout << "get data is finished." << std::endl;
   double runtime = 0.0;

   runtime = q43_scalar();
   std::cout << std::setiosflags(std::ios::fixed) << std::setprecision(6) << runtime << std::endl;
   sleep(30);

   runtime = q43_simd();
   std::cout << std::setiosflags(std::ios::fixed) << std::setprecision(6) << runtime << std::endl;
   sleep(30);

   runtime = q43_hi(36);
   std::cout << std::setiosflags(std::ios::fixed) << std::setprecision(6) << runtime << std::endl;
   // sleep(5);

   // runtime = q43_hi_block(36);
   // std::cout << std::setiosflags(std::ios::fixed) << std::setprecision(6) << runtime << std::endl;

   return 0;
}