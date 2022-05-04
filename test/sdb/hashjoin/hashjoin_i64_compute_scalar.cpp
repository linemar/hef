/*
 * @Author: SunZewen
 * @Date: 2022-01-01 16:30:34
 * @LastEditors: SunZewen
 * @LastEditTime: 2022-01-14 10:51:48
 * @Description:   
 * @FilePath: /sunzw/workspace/ecnu/ecnu/hef/test/sdb/hashjoin/hashjoin_i64_compute_scalar.cpp
 */

#include <iostream>
#include <vector>
#include <chrono>
#include <stdint.h>
#include <immintrin.h>
#include <cmath>
#include <stdlib.h>
#include <iomanip>

#include "../../../include/operator/operator.hpp"

double hashjoin_comp_test()
{
    uint64_t size = 10 * 10000 * 10000;
    //build data hash table
    uint64_t *src = new uint64_t[size];
    uint64_t *dst = new uint64_t[size];

    for(uint64_t i = 0; i < size; i++)
    {
        src[i] = i;
    }

    auto t1 = std::chrono::steady_clock::now();

    for (uint64_t i = 0; i < size; i++)
    {
        dst[i] = hash(src[i], 0);
    }

    auto t2 = std::chrono::steady_clock::now();
    double dr_ms = std::chrono::duration<double, std::milli>(t2 - t1).count();
    std::cout << "scalar hash compute finished, use time " << std::setprecision(3) << dr_ms << " ms" << std::endl;
 
    return dr_ms;
}


int main(int argc, char *argv[])
{
    double runtime = (hashjoin_comp_test());
    std::cout << std::setiosflags(std::ios::fixed) << std::setprecision(3) << runtime; 
    
    return 0;
}