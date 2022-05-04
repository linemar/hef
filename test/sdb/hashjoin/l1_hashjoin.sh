###
 # @Author: SunZewen
 # @Date: 2022-01-10 10:50:07
 # @LastEditors: SunZewen
 # @LastEditTime: 2022-01-10 10:52:35
 # @Description:   
 # @FilePath: /sunzw/workspace/ecnu/ecnu/hef/test/sdb/hashjoin/l1_hashjoin.sh
### 

/home/sunzw/usr/gcc-10.2.0/bin/g++ -std=c++20 -O3 -mavx512f -mavx512dq hashjoin_avx512_l1.cpp -o hashjoin_avx512_l1
sleep 10

taskset -c 10 ./hashjoin_avx512_l1
sleep 10

taskset -c 10 ./hashjoin_avx512_l1
sleep 10

taskset -c 10 ./hashjoin_avx512_l1
sleep 10

taskset -c 10 ./hashjoin_avx512_l1
sleep 10

taskset -c 10 ./hashjoin_avx512_l1