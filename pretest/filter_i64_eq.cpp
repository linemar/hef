/*
 * @Author: SunZewen
 * @Date: 2022-01-01 16:30:34
 * @LastEditors: SunZewen
 * @LastEditTime: 2022-01-04 13:07:58
 * @Description:   
 * @FilePath: /workspace/ecnu/ecnu/hef/pretest/filter_i64_eq.cpp
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

int hashjoin_l1_test(DataBase *db, uint64_t batch_size)
{
    const int hash_tabel_size = 512;

    //get table data
    Table t_date = db->get_table(std::string("date"));
    Table t_part = db->get_table(std::string("part"));
    Table t_lineorder = db->get_table(std::string("lineorder"));

    std::string *p_mfgr = (std::string *)t_part.get_column("p_mfgr").get_data();
    uint64_t *d_year = (uint64_t *)t_date.get_column("d_year").get_data();

    uint64_t *d_yearmonth = (uint64_t *)t_date.get_column("d_yearmonthnum").get_data();

    uint64_t *d_datekey = (uint64_t *)t_date.get_column("d_datekey").get_data();
    uint64_t *p_partkey = (uint64_t *)t_part.get_column("p_partkey").get_data();

    uint64_t *lo_partkey = (uint64_t *)t_lineorder.get_column("lo_partkey").get_data();
    uint64_t *lo_orderdate = (uint64_t *)t_lineorder.get_column("lo_orderdate").get_data();

    std::cout << "get data is finished." << std::endl;

    // process date table
    uint64_t d_size = t_date.get_column("d_datekey").get_size();
    uint64_t p_size = t_date.get_column("p_partkey").get_size();
    uint64_t lo_size = t_lineorder.get_column("lo_orderdate").get_size();

    uint64_t *d_rid = new uint64_t[d_size];
    uint64_t *p_rid = new uint64_t[d_size];
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

    hashjoin_i64_l1(date_hash_table, hash_tabel_size, lo_rid, lo_orderdate, lo_size, batch_size, lo_result, lo_result_size);

    for (uint64_t i = lo_size - rest; i < lo_size; i++)
    {
        if (hash_probe(date_hash_table, hash_tabel_size, lo_orderdate[lo_rid[i]]))
        {
            lo_result[lo_result_size] = lo_rid[i];
            lo_result_size++;
        }
    }

    auto t2 = std::chrono::steady_clock::now();
    double dr_ms = std::chrono::duration<double, std::milli>(t2 - t1).count();
    std::cout << "simd 1 finished, hash tabel size is " << (unsigned int)(hash_tabel_size) << ", use time " << dr_ms << " ms" << std::endl;
    std::cout << "lo_result_size = " << lo_result_size << std::endl;
    std::cout << std::endl;

    // for (int i = 0; i < lo_result_size; i++)
    // {
    //     std::cout << lo_result[i] << std::endl;
    // }

    return (int)dr_ms;
}


int main(int argc, char * argv[])
{

    SSB_DB *ssb_db = new SSB_DB();

    //load data
    ssb_db->load_data(std::string("/home/sunzw/data/scale_10"));
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

    int run_time = hashjoin_l1_test(db, batch_size);
    return (int)run_time;
}