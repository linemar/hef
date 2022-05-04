/*
 * @Author: SunZewen
 * @Date: 2022-01-01 16:55:28
 * @LastEditors: SunZewen
 * @LastEditTime: 2022-01-02 20:51:05
 * @Description:   
 * @FilePath: /ecnu/hef/pretest/run.cpp
 */


#include "hashjoin.hpp"
#include <string>
#include <fstream>
#include <iostream>

int main()
{
    SSB_DB *ssb_db = new SSB_DB();

    //load data
    ssb_db->load_data(std::string("/home/sunzw/data/scale_10"));
    DataBase *db = ssb_db->db;

    // load operator information
    std::string config_path = "../object/lib/config.json";
    std::ifstream fin(config_path.c_str());
    
    std::string strline;

    while(std::getline(fin, strline))
    {
        if(strline.find("batch_size"))
        {
            std::cout << strline << std::endl;
            break;
        }
    }

    uint64_t batch_size = 36;

    //query
    hashjoin_l1_test(db, 36);
    return 0;
}