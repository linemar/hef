/*
 * @Author: SunZewen
 * @Date: 2020-12-30 16:42:57
 * @LastEditors: SunZewen
 * @LastEditTime: 2021-02-22 13:50:31
 * @Description:   
 * @FilePath: /workspace/ssb/v0.01/include/database/database.hpp
 */

#ifndef DATABASE
#define DATABASE

#include "table.hpp"
#include <string>
#include <vector>
#include <string>
#include <unordered_map>
#include <iostream>
#include "types.hpp"

class DataBase
{
public:
DataBase();
    DataBase(std::string);
    ~DataBase();

    void creat_table(std::string table_name, std::vector<std::pair<std::string, DataType>> table_title);
    void creat_table(Table t);
    void drop_table();

    Table get_table(std::string);

    int load_table_data(std::string table_name, std::string tbl_path);
    void load_table_data(std::string table_name, std::vector<std::pair<std::string, DataType>> table_title, std::string tbl_path);
    
private:
    std::unordered_map<std::string, Table> tables;
    std::string db_name;
};


DataBase::DataBase()
{
    
}

DataBase::DataBase(std::string name)
{
    this->db_name = name;
}

DataBase::~DataBase()
{
    
}

void DataBase::creat_table(std::string table_name, std::vector<std::pair<std::string, DataType>> table_title)
{
    Table *t = new Table(table_name, table_title);
    tables.insert(std::make_pair(table_name, *t));
}

void DataBase::creat_table(Table t)
{
    tables.insert(std::make_pair(t.get_table_name(), t));
}

Table DataBase::get_table(std::string table_name)
{
    return tables[table_name];
}

void DataBase::drop_table()
{
}

void DataBase::load_table_data(std::string table_name, std::vector<std::pair<std::string, DataType>> table_title, std::string tbl_path)
{

    /*
    //首先判断当前表是否存在，如果不存在则创建新的表
    Table * t = nullptr;
    if(tables.count(table_name) == 0)
    {
       t = new Table(table_name, table_title);
       tables.insert(std::make_pair<std::string, Table>(table_name, *t));
    }

    *t = tables[table_name];

    t->load_data(tbl_path);
    */

    return;
}

int DataBase::load_table_data(std::string table_name, std::string tbl_path)
{

    //首先判断当前表是否存在，如果不存在则创建新的表
    if (tables.count(table_name) == 0)
    {
        return -1;
    }

    //std::cout << "table load data." << std::endl;

    // Table t = tables[table_name];
    tables[table_name].load_data(tbl_path);

    return 0;
}

#endif