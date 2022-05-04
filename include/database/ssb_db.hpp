/*
 * @Author: SunZewen
 * @Date: 2021-02-03 11:51:45
 * @LastEditors: SunZewen
 * @LastEditTime: 2021-04-22 18:46:59
 * @Description:   
 * @FilePath: /workspace/uoperator/include/database/ssb_db.hpp
 */

#ifndef SSBDB
#define SSBDB

#pragma pack(8)

#include <iostream>
#include <vector>
#include <string>
#include <chrono>
#include <cmath>
#include "database.hpp"
#include "table.hpp"
#include "types.hpp"

class SSB_DB
{
public:
    SSB_DB();
    void create_ssb_db();
    void load_data(std::string dir);

    DataBase *db;
};

SSB_DB::SSB_DB()
{
    //create DataBase
    db = new DataBase(std::string("SSB"));
}

void SSB_DB::create_ssb_db()
{
    //define  tables
    //date
    std::vector<std::pair<std::string, DataType>> date_title;
    date_title.push_back(std::make_pair(std::string("d_datekey"), Integer));
    date_title.push_back(std::make_pair(std::string("d_date"), String));
    date_title.push_back(std::make_pair(std::string("d_dayofweek"), String));
    date_title.push_back(std::make_pair(std::string("d_month"), String));
    date_title.push_back(std::make_pair(std::string("d_year"), Integer));
    date_title.push_back(std::make_pair(std::string("d_yearmonthnum"), Integer));
    date_title.push_back(std::make_pair(std::string("d_yearmonth"), String));
    date_title.push_back(std::make_pair(std::string("d_daynumweek"), Integer));
    date_title.push_back(std::make_pair(std::string("d_daynuminmonth"), Integer));
    date_title.push_back(std::make_pair(std::string("d_daynuminyear"), Integer));
    date_title.push_back(std::make_pair(std::string("d_monthnuminyear"), Integer));
    date_title.push_back(std::make_pair(std::string("d_weeknuminyear"), Integer));
    date_title.push_back(std::make_pair(std::string("d_sellingseason"), String));
    date_title.push_back(std::make_pair(std::string("d_lastdayinweekfl"), Integer));
    date_title.push_back(std::make_pair(std::string("d_lastdayinmonthfl"), Integer));
    date_title.push_back(std::make_pair(std::string("d_holidayfl"), Integer));
    date_title.push_back(std::make_pair(std::string("d_weekdayfl"), Integer));

    //lineorer
    std::vector<std::pair<std::string, DataType>> lineorer_title;
    lineorer_title.push_back(std::make_pair(std::string("lo_orderkey"), Integer));
    lineorer_title.push_back(std::make_pair(std::string("lo_linenumber"), Integer));
    lineorer_title.push_back(std::make_pair(std::string("lo_custkey"), Integer));
    lineorer_title.push_back(std::make_pair(std::string("lo_partkey"), Integer));
    lineorer_title.push_back(std::make_pair(std::string("lo_suppkey"), Integer));
    lineorer_title.push_back(std::make_pair(std::string("lo_orderdate"), Integer));
    lineorer_title.push_back(std::make_pair(std::string("lo_orderpriotity"), String));
    lineorer_title.push_back(std::make_pair(std::string("lo_shippriotity"), Integer));
    lineorer_title.push_back(std::make_pair(std::string("lo_quantity"), Integer));
    lineorer_title.push_back(std::make_pair(std::string("lo_extendedprice"), Integer));
    lineorer_title.push_back(std::make_pair(std::string("lo_ordtotalprice"), Integer));
    lineorer_title.push_back(std::make_pair(std::string("lo_discount"), Integer));
    lineorer_title.push_back(std::make_pair(std::string("lo_revenue"), Integer));
    lineorer_title.push_back(std::make_pair(std::string("lo_supplycost"), Integer));
    lineorer_title.push_back(std::make_pair(std::string("lo_tax"), Integer));
    lineorer_title.push_back(std::make_pair(std::string("lo_commitdate"), Integer));
    lineorer_title.push_back(std::make_pair(std::string("lo_shipmode"), String));

    //customer
    std::vector<std::pair<std::string, DataType>> customer_title;
    customer_title.push_back(std::make_pair(std::string("c_custkey"), Integer));
    customer_title.push_back(std::make_pair(std::string("c_name"), String));
    customer_title.push_back(std::make_pair(std::string("c_address"), String));
    customer_title.push_back(std::make_pair(std::string("c_city"), String));
    customer_title.push_back(std::make_pair(std::string("c_nation"), String));
    customer_title.push_back(std::make_pair(std::string("c_region"), String));
    customer_title.push_back(std::make_pair(std::string("c_phone"), String));
    customer_title.push_back(std::make_pair(std::string("c_mktsegment"), String));

    //part
    std::vector<std::pair<std::string, DataType>> part_title;
    part_title.push_back(std::make_pair(std::string("p_partkey"), Integer));
    part_title.push_back(std::make_pair(std::string("p_name"), String));
    part_title.push_back(std::make_pair(std::string("p_mfgr"), String));
    part_title.push_back(std::make_pair(std::string("p_category"), String));
    part_title.push_back(std::make_pair(std::string("p_brand"), String));
    part_title.push_back(std::make_pair(std::string("p_color"), String));
    part_title.push_back(std::make_pair(std::string("p_type"), String));
    part_title.push_back(std::make_pair(std::string("p_size"), Integer));
    part_title.push_back(std::make_pair(std::string("p_container"), String));

    //supplier.tbl
    std::vector<std::pair<std::string, DataType>> supplier_title;
    supplier_title.push_back(std::make_pair(std::string("s_suppkey"), Integer));
    supplier_title.push_back(std::make_pair(std::string("s_name"), String));
    supplier_title.push_back(std::make_pair(std::string("s_address"), String));
    supplier_title.push_back(std::make_pair(std::string("s_city"), String));
    supplier_title.push_back(std::make_pair(std::string("s_nation"), String));
    supplier_title.push_back(std::make_pair(std::string("s_region"), String));
    supplier_title.push_back(std::make_pair(std::string("s_phone"), String));

    // create tables
    db->creat_table("date", date_title);
    db->creat_table("part", part_title);
    db->creat_table("customer", customer_title);
    db->creat_table("supplier", supplier_title);
    db->creat_table("lineorder", lineorer_title);
}

void SSB_DB::load_data(std::string dir)
{
    create_ssb_db();

    //load data
    db->load_table_data(std::string("part"), std::string(dir + "/part.tbl"));
    db->load_table_data(std::string("date"), std::string(dir + "/date.tbl"));
    db->load_table_data(std::string("customer"), std::string(dir + "/customer.tbl")); 
    db->load_table_data(std::string("supplier"), std::string(dir + "/supplier.tbl"));
    db->load_table_data(std::string("lineorder"), std::string(dir + "/lineorder.tbl"));
}

#endif