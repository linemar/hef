/*
 * @Author: SunZewen
 * @Date: 2022-01-08 12:45:04
 * @LastEditors: SunZewen
 * @LastEditTime: 2022-01-20 17:00:24
 * @Description:   
 * @FilePath: /sunzw/workspace/ecnu/ecnu/hef/include/common.hpp
 */

#include <iostream>
#include <fstream>
#include <string>
#include <cstring>
#include <vector>
#include <cstdint>
#include <stdlib.h>
#include <typeinfo>

uint64_t load_data_int(std::vector<uint64_t> &data, std::string file_path)
{
    // std::cout << file_path << std::endl;
    std::ifstream col_file(file_path.c_str());

    if (!col_file.is_open())
    {
        std::cout << "open tbl file failed!" << std::endl;
        exit(0) ;
    }

    std::string line;
    int i = 0;

    while (getline(col_file, line))
    {
        if (line.size() == 0)
        {
            break;
        }

        i++;
        data.push_back(atol(line.c_str()));
    }

    // std::cout << "load data size " << i << std::endl;
    col_file.close();

    return i;
}


uint64_t load_data_str(std::vector<std::string> &data, std::string file_path)
{
    // std::cout << file_path << std::endl;
    std::ifstream col_file(file_path.c_str());

    if (!col_file.is_open())
    {
        std::cout << "open tbl file failed!" << std::endl;
        exit(0) ;
    }

    std::string line;
    int i = 0;

    while (getline(col_file, line))
    {
        if (line.size() == 0)
        {
            break;
        }

        i++;
        data.push_back(std::string(line.c_str()));
    }

    // std::cout << "load data size " << i << std::endl;
    col_file.close();

    return i;
}



int reset_freq()
{
    unsigned int size = 128 * 1024 * 1024;
    unsigned int sum = 0;

    for(int i = 0; i < size; i++)
    {
        sum += i;
    }

    return sum;
}