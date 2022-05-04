/*
 * @Author: SunZewen
 * @Date: 2021-01-09 15:44:28
 * @LastEditors: SunZewen
 * @LastEditTime: 2021-04-23 10:18:00
 * @Description:   
 * @FilePath: /workspace/uoperator/include/database/types.hpp
 */

#ifndef DATATYPE
#define DATATYPE

// typedef unsigned long long int uint_64;
// typedef unsigned int uint_32;
// typedef unsigned short int uint_16;
// typedef char uint_8;

enum DataType
{
    Integer,
    Float,
    String,
    Date,
    Unknow
};

#endif