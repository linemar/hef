/*
 * @Author: SunZewen
 * @Date: 2022-01-01 16:30:34
 * @LastEditors: SunZewen
 * @LastEditTime: 2022-01-06 18:31:33
 * @Description:   
 * @FilePath: /workspace/ecnu/ecnu/hef/pretest/hashjoin_gather_i64_mem.cpp
 */

$header

#include <iostream>
#include <vector>
#include <chrono>
#include <stdint.h>
#include <immintrin.h>
#include <cmath>
#include <stdlib.h>

#include "../../../../include/database/database.hpp"
#include "../../../../include/database/table.hpp"
#include "../../../../include/database/types.hpp"
#include "../../../../include/database/ssb_db.hpp"
#include "../../../../include/database/database.hpp"
#include "../../../../include/operator/operator.hpp"

double hashjoin_mem_test(DataBase *db, uint64_t batch_size)
{
    const int hash_tabel_size = 0x0000000007ffffff;

    //get table data
    Table t_part = db->get_table(std::string("part"));
    Table t_lineorder = db->get_table(std::string("lineorder"));

    uint64_t *p_partkey = (uint64_t *)t_part.get_column("p_partkey").get_data();
    uint64_t *lo_orderdate = (uint64_t *)t_lineorder.get_column("lo_orderdate").get_data();

    // std::cout << "get data is finished." << std::endl;

    // process date table
    uint64_t p_size = t_part.get_column("p_partkey").get_size();
    uint64_t lo_size = t_lineorder.get_column("lo_orderdate").get_size();

    uint64_t *p_rid = new uint64_t[p_size];
    uint64_t *lo_rid = new uint64_t[lo_size];

    for (uint64_t i = 0; i < p_size; i++)
    {
        p_rid[i] = i;
    }
    
    for (uint64_t i = 0; i < lo_size; i++)
    {
        lo_rid[i] = i;
    }

    //build data hash table
    uint64_t *part_hash_table = new uint64_t[hash_tabel_size];
    hash_build(p_partkey, p_size, part_hash_table, hash_tabel_size);

    uint64_t *lo_result = new uint64_t[lo_size];
    uint64_t lo_result_size = 0;

    auto t1 = std::chrono::steady_clock::now();
    
    uint64_t rest = lo_size % batch_size;

    hashjoin_i64_mem(part_hash_table, hash_tabel_size, lo_rid, lo_orderdate, lo_size - rest, batch_size, lo_result, lo_result_size);

    for (uint64_t i = lo_size - rest; i < lo_size; i++)
    {
        if (hash_probe(part_hash_table, hash_tabel_size, lo_orderdate[lo_rid[i]]))
        {
            lo_result[lo_result_size] = lo_rid[i];
            lo_result_size++;
        }
    }

    auto t2 = std::chrono::steady_clock::now();
    double dr_ms = std::chrono::duration<double, std::milli>(t2 - t1).count();
    std::cout << "hybird finished, hash tabel size is " << (unsigned int)(hash_tabel_size) << "lo_result_size = " << lo_result_size << ", use time " << dr_ms << " ms" << std::endl;
    // std::cout << "lo_result_size = " << lo_result_size << std::endl;
    // std::cout << std::endl;

    // for (int i = 0; i < lo_result_size; i++)
    // {
    //     std::cout << lo_result[i] << std::endl;
    // }

    return dr_ms;
}


int main(int argc, char * argv[])
{

    SSB_DB *ssb_db = new SSB_DB();

    //load data
    ssb_db->load_data(std::string("/tmp/ram/scale_1"));
    DataBase *db = ssb_db->db;

    uint64_t batch_size = atoi(argv[1]);

    double time = (int)roundf((hashjoin_mem_test(db, batch_size)) * 100.0);

    int runtime = (int)(time * 100.0);

    std::cout << "runtime : ";
    std::cout << runtime;
    std::cout << ", " ;
    std::cout << runtime / 100.0 << " ms" << std::endl;
    
    return runtime;
}