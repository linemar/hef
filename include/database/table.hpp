/*
 * @Author: SunZewen
 * @Date: 2021-01-07 13:32:21
 * @LastEditors: SunZewen
 * @LastEditTime: 2021-03-17 16:03:32
 * @Description:   
 * @FilePath: /workspace/ssb/v0.04/include/database/table.hpp
 */

#ifndef TABLE
#define TABLE

#include <string>
#include <vector>
#include <unordered_map>
#include <fstream>
#include "column.hpp"
#include "types.hpp"
#include <iostream>
#include <unistd.h>

class Table
{

public:
    Table();
    Table(std::string table_name, std::vector<std::pair<std::string, DataType>> title);

    std::string get_table_name();

    Column get_column(std::string);
    //Column get_column_data(std::string col_name);

    unsigned long get_col_size();
    unsigned long get_row_size();
    //std::unordered_map<std::string, DataType> get_title();
    //std::vector<DataType> get_title_types();
    //std::vector<std::string> get_title_names();

    int insert_col(std::unordered_map<std::string, DataType>);
    int insert_row(std::vector<std::string>);

    int remove();
    int clear();

    int load_data(std::string);

    // private:
    unsigned int long row_size; //多少行，元素的数量
    unsigned int long col_size; //多少列，属性的数量

    //table name
    std::string table_name;

    //table head
    std::vector<std::pair<std::string, DataType>> title;

    //table data
    std::vector<Column> columns;
};

Table::Table()
{
}

Table::Table(std::string table_name, std::vector<std::pair<std::string, DataType>> table_title)
{
    this->table_name = table_name;
    this->title.assign(table_title.begin(), table_title.end());
    this->col_size = table_title.size();

    for (auto it = title.begin(); it != title.end(); it++)
    {
        Column *col = new Column(it->first, it->second);
        columns.push_back(*col);
    }
}

std::string Table::get_table_name()
{
    return table_name;
}

Column Table::get_column(std::string col_name)
{
    int i = 0;
    for (i = 0; i < columns.size(); i++)
    {
        if (title[i].first == col_name)
            break;
    }

    if (i < columns.size())
        return columns[i];

    else
        return Column();
}

unsigned long Table::get_col_size()
{
    return col_size;
}

unsigned long Table::get_row_size()
{
    return row_size;
}

// std::unordered_map<std::string, DataType> Table::get_title()
// {
//     return title;
// }
/*
std::vector<DataType> Table::get_title_types()
{
    std::vector<DataType> title_types;
    for (auto it = title.begin(); it != title.end(); it++)
    {
        title_types.push_back(it->second);
    }
    return title_types;
}

std::vector<std::string> Table::get_title_names()
{
    std::vector<std::string> titles;
    for (auto it = title.begin(); it != title.end(); it++)
    {
        titles.push_back(it->first);
    }

    return titles;
}
*/
int Table::insert_row(std::vector<std::string> values)
{
    int i = 0;
    for (auto it = columns.begin(); it != columns.end(); it++)
    {
        //Column col = columns[it->first];
        //it.insert_item(values[i]);
    }

    return 0;
}

int Table::remove()
{
    return 0;
}

int Table::load_data(const std::string file_path)
{
    //std::cout << "table loading data ....." << std::endl;

    std::ifstream tbl_file(file_path.c_str());

    if (!tbl_file.is_open())
    {
        //std::cout << "open tbl file failed!" << std::endl;
        return -1;
    }

    std::string data;

    const char *spliter = "|";
    char *s = nullptr;

    int i = 0;

    while (getline(tbl_file, data))
    {
        //std::cout << "spliter : " << spliter << std::endl;
        //std::cout << "data : " << data << std::endl;

        if (data.size() == 0)
        {
            break;
        }

        char *p = new char[data.length() + 1];
        strcpy(p, data.c_str());
        s = strtok(p, spliter);

        for (auto i = 0; i < columns.size(); i++)
        {
            //std::cout << "s : " << s << std::endl;
            columns[i].insert_item(s);
            s = strtok(NULL, spliter);
        }

        i++;
        //std::cout << "loading data size : " << i << std::endl;
    }

    //std::cout << "load data size " << i << std::endl;

    tbl_file.close();

    return 0;
}

#endif

/*
        char *p = new char[values.length() + 1];
        strcpy(p, values.c_str());
        s = strtok(p, spliter);

        c_custkey.push_back(atoi(s));

        s = strtok(NULL, spliter);
        c_name.push_back(s);
        //std::cout << s << std::endl;

        s = strtok(NULL, spliter);
        c_address.push_back(s);
        //std::cout << s << std::endl;

        s = strtok(NULL, spliter);
        c_city.push_back(s);
        //std::cout << s << std::endl;

        s = strtok(NULL, spliter);
        c_nation.push_back(s);
        //std::cout << s << std::endl;

        s = strtok(NULL, spliter);
        c_region.push_back(s);
        //std::cout << s << std::endl;

        s = strtok(NULL, spliter);
        c_phone.push_back(s);
        //std::cout << s << std::endl;

        s = strtok(NULL, spliter);

        size++;

*/