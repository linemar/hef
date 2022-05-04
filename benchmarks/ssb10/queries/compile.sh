###
# @Author: SunZewen
# @Date: 2022-01-18 00:01:55
 # @LastEditors: SunZewen
 # @LastEditTime: 2022-01-20 20:59:16
# @Description:
 # @FilePath: /sunzw/workspace/ecnu/ecnu/hef/benchmarks/ssb10/queries/compile.sh
###

/home/sunzw/usr/gcc-10.2.0/bin/g++ -std=c++20 -O3 -mavx512f -mavx512dq -fno-tree-vectorize q21.cpp -o q21
/home/sunzw/usr/gcc-10.2.0/bin/g++ -std=c++20 -O3 -mavx512f -mavx512dq -fno-tree-vectorize q22.cpp -o q22
/home/sunzw/usr/gcc-10.2.0/bin/g++ -std=c++20 -O3 -mavx512f -mavx512dq -fno-tree-vectorize q23.cpp -o q23

/home/sunzw/usr/gcc-10.2.0/bin/g++ -std=c++20 -O3 -mavx512f -mavx512dq -fno-tree-vectorize q31.cpp -o q31
/home/sunzw/usr/gcc-10.2.0/bin/g++ -std=c++20 -O3 -mavx512f -mavx512dq -fno-tree-vectorize q32.cpp -o q32
/home/sunzw/usr/gcc-10.2.0/bin/g++ -std=c++20 -O3 -mavx512f -mavx512dq -fno-tree-vectorize q33.cpp -o q33
/home/sunzw/usr/gcc-10.2.0/bin/g++ -std=c++20 -O3 -mavx512f -mavx512dq -fno-tree-vectorize q34.cpp -o q34

/home/sunzw/usr/gcc-10.2.0/bin/g++ -std=c++20 -O3 -mavx512f -mavx512dq -fno-tree-vectorize q41.cpp -o q41
/home/sunzw/usr/gcc-10.2.0/bin/g++ -std=c++20 -O3 -mavx512f -mavx512dq -fno-tree-vectorize q42.cpp -o q42
/home/sunzw/usr/gcc-10.2.0/bin/g++ -std=c++20 -O3 -mavx512f -mavx512dq -fno-tree-vectorize q43.cpp -o q43
