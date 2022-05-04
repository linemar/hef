/*
 * @Author: SunZewen
 * @Date: 2022-01-01 16:30:34
 * @LastEditors: SunZewen
 * @LastEditTime: 2022-01-13 15:48:00
 * @Description:   
 * @FilePath: /sunzw/workspace/ecnu/ecnu/hef/pretest/hashjoin_i64_l1.cpp
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

double hashjoin_l1_test(DataBase *db, uint64_t batch_size)
{
    const int hash_tabel_size = 128;

    //get table data
    Table t_date = db->get_table(std::string("date"));
    Table t_lineorder = db->get_table(std::string("lineorder"));

    uint64_t *d_datekey = (uint64_t *)t_date.get_column("d_datekey").get_data();
    uint64_t *lo_orderdate = (uint64_t *)t_lineorder.get_column("lo_orderdate").get_data();

    // std::cout << "get data is finished." << std::endl;

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

    uint64_t rest = lo_size % batch_size;

    hashjoin_i64_l1(date_hash_table, hash_tabel_size, lo_rid, lo_orderdate, lo_size - rest, batch_size, lo_result, lo_result_size);

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
    std::cout << "hybrid hash join l1 finished, hash tabel size is " << (unsigned int)(hash_tabel_size) << ", lo_result_size = " << lo_result_size << ", use time " << std::setprecision(3) << dr_ms << " ms" << std::endl;
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

    // load operator information
    // std::string config_path = "../object/lib/config.json";
    // std::ifstream fin(config_path.c_str());

    // std::string strline;

    // while(std::getline(fin, strline))
    // {
    //     if(strline.find("batch_size"))
    //     {
    //         std::cout << strline << std::endl;
    //         break;
    //     }
    // }

    uint64_t batch_size = atoi(argv[1]);

    double runtime = (hashjoin_l1_test(db, batch_size));
    std::cout << std::setiosflags(std::ios::fixed) << std::setprecision(3) << runtime; //<< std::endl;

    // int runtime = (int)(time * 100.0);

    // std::cout << "runtime : " << runtime << ", " << runtime / 100.0 << " ms" << std::endl;

    return 0;
}