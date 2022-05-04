/*
 * @Author: SunZewen
 * @Date: 2022-01-01 16:30:34
 * @LastEditors: SunZewen
 * @LastEditTime: 2022-01-15 23:00:32
 * @Description:   
 * @FilePath: /sunzw/workspace/ecnu/ecnu/hef/pretest/hashjoin_i64_l2.cpp
 */

$header

#include <iostream>
#include <vector>
#include <chrono>
#include <stdint.h>
#include <immintrin.h>
#include <cmath>
#include <stdlib.h>
#include <iomanip>

#include "../../../../include/database/database.hpp"
#include "../../../../include/database/table.hpp"
#include "../../../../include/database/types.hpp"
#include "../../../../include/database/ssb_db.hpp"
#include "../../../../include/database/database.hpp"
#include "../../../../include/operator/operator.hpp"

double hashjoin_l2_test(DataBase *db, uint64_t batch_size)
{
    const int hash_tabel_size = 8 * 1024;

    //get table data
    Table t_part = db->get_table(std::string("part"));
    Table t_lineorder = db->get_table(std::string("lineorder"));

    uint64_t *p_partkey = (uint64_t *)t_part.get_column("p_partkey").get_data();
    uint64_t *lo_partkey = (uint64_t *)t_lineorder.get_column("lo_partkey").get_data();

    // std::cout << "get data is finished." << std::endl;

    // process date table
    uint64_t p_size = t_part.get_column("p_partkey").get_size();
    uint64_t lo_size = t_lineorder.get_column("lo_partkey").get_size();

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

    hashjoin_i64_l2(part_hash_table, hash_tabel_size, lo_rid, lo_partkey, lo_size - rest, batch_size, lo_result, lo_result_size);

    for (uint64_t i = lo_size - rest; i < lo_size; i++)
    {
        if (hash_probe(part_hash_table, hash_tabel_size, lo_partkey[i]))
        {
            lo_result[lo_result_size] = lo_rid[i];
            lo_result_size++;
        }
    }

    auto t2 = std::chrono::steady_clock::now();
    double dr_ms = std::chrono::duration<double, std::milli>(t2 - t1).count();
    std::cout << "hybrid hash join l2 finished, hash tabel size is " << (unsigned int)(hash_tabel_size) << ", lo_result_size = " << lo_result_size << ", use time " << std::setprecision(3) << dr_ms << " ms" << std::endl;
    // std::cout << "lo_result_size = " << lo_result_size << std::endl;
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

    uint64_t batch_size = atoi(argv[1]);

    double runtime = (hashjoin_l2_test(db, batch_size));
    std::cout << std::setiosflags(std::ios::fixed) << std::setprecision(3) << runtime; //<< std::endl;

    return 0;
}