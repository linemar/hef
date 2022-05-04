/*
 * @Author: SunZewen
 * @Date: 2021-01-08 09:29:09
 * @LastEditors: SunZewen
 * @LastEditTime: 2021-04-23 10:21:22
 * @Description:   
 * @FilePath: /workspace/uoperator/include/database/column.hpp
 */

#ifndef COLUMN
#define COLUMN

#pragma pack(8)

#include <vector>
#include <string>
#include <cstring>
#include "types.hpp"
#include <iostream>
#include <unistd.h>
#include <cstdint>
// #include "data.hpp"

class Column
{
public:
    Column();
    //Column(const Column &);
    Column(std::string name, DataType type);
    ~Column();

    void *get_data();
    std::string get_name();
    uint64_t get_size();
    bool insert_item(std::string item);
    bool remove_item(void *item);
    bool remove_item(uint64_t index);

private:
    std::string col_name;
    uint64_t size;
    DataType type;
    void *data;
};

Column::Column()
{
    this->col_name = "Error";
    this->type = Unknow;
    this->size = 0;
}

// Column::Column(const Column &c)
// {
//     this->col_name = c.col_name;
//     this->size = c.size;
//     this->type = c.type;
//     this->data = c.data;
// }

Column::Column(std::string name, DataType type)
{
    this->col_name = name;
    this->type = type;
    this->size = 0;

    switch (type)
    {
    case Integer:
        data = new std::vector<uint64_t>();
        break;

    case Float:
        data = new std::vector<float>();
        break;

    case String:
        data = new std::vector<std::string>();
        break;

    case Date:
        break;

    default:
        break;
    }

    /**/
}

Column::~Column()
{
}

std::string Column::get_name()
{
    return this->col_name;
}

void *Column::get_data()
{
    switch (type)
    {
    case Integer:
        return (static_cast<std::vector<uint64_t> *>(data))->data();
        break;

    case Float:
        return (static_cast<std::vector<float> *>(data))->data();
        break;

    case String:
        return (static_cast<std::vector<std::string> *>(data))->data();
        return 0;
        break;

    case Date:
        return nullptr;
        break;

    default:
        return nullptr;
        break;
    }
}

bool Column::insert_item(std::string item)
{
    //std::cout << "insert " << item << "  to  " << col_name << ", col type is " << type << std::endl;
    switch (type)
    {
    case Integer:
        (static_cast<std::vector<uint64_t> *>(data))->push_back(atoll(item.c_str()));
        break;

    case Float:
        (static_cast<std::vector<float> *>(data))->push_back(atof(item.c_str()));
        break;

    case String:
        (static_cast<std::vector<std::string> *>(data))->push_back(std::string(item));
        break;

    case Date:
        break;

    default:
        break;
    }

    this->size++;
    return 0;
}

uint64_t Column::get_size()
{
    return this->size;
}

#endif
