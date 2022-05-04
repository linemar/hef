/*
 * @Author: SunZewen
 * @Date: 2021-01-11 15:55:26
 * @LastEditors: SunZewen
 * @LastEditTime: 2021-01-12 19:38:12
 * @Description:   
 * @FilePath: /workspace/ssb/base/v0.011/include/database/data.hpp
 */
#ifndef DATA
#define DATA

#include <vector>
#include <string>
#include <cstring>
#include "types.hpp"
#include <iostream>
#include <cstdlib>

class Data
{
public:
    virtual void *get_data() { return nullptr; };
    virtual int insert(std::string item) { return 0; };
    virtual int remove() { return 0; };
    virtual int clear() { return 0; };
};

template <class T>
class MetaData : public Data
{
public:
    MetaData(DataType type);
    ~MetaData();

    void *get_data();
    int insert(std::string item);
    int remove();

private:
    DataType type;
    std::vector<T> data;
};

template <class T>
MetaData<T>::MetaData(DataType type)
{
    this->type = type;
}

template <class T>
MetaData<T>::~MetaData()
{
}

template <class T>
void *MetaData<T>::get_data()
{
    return (void *)data.data();
}

template <class T>
int MetaData<T>::insert(std::string item)
{
    std::cout << "insert int data : " << item << std::endl;
    
    switch (type)
    {
    case Integer:
        std::cout << "insert int data : " << item << std::endl;
        data.push_back(std::stoi(std::string(item)));
        break;

    case Float:
        data.push_back(std::stof(std::string(item)));
        break;

    case String:
        data.push_back(std::string(item));
        break;

    case Date:
        break;

    default:
        break;
    }
/*clear*/
    return 0;
}

template <class T>
int MetaData<T>::remove()
{
    /*
    switch (type)
    {
    case Integer:
        break;
    case Float:
        break;

    case String:
        break;

    case Date:
        break;

    default:
        break;
    }*/
    return 0;
}

#endif