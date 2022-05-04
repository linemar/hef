###
# @Author: SunZewen
# @Date: 2022-01-18 00:03:47
 # @LastEditors: SunZewen
 # @LastEditTime: 2022-01-22 23:07:04
# @Description:
 # @FilePath: /sunzw/workspace/ecnu/ecnu/hef/benchmarks/ssb50/queries/run.sh
###

echo "q21"
taskset -c 10 ./q21
sleep 60

echo "q22"
taskset -c 10 ./q22
sleep 60

echo "q23"
taskset -c 10 ./q23
sleep 60

echo "q31"
taskset -c 10 ./q31
sleep 60

echo "q32"
taskset -c 10 ./q32
sleep 60

echo "q33"
taskset -c 10 ./q33
sleep 60

echo "q34"
taskset -c 10 ./q34
sleep 60

echo "q41"
taskset -c 10 ./q41
sleep 60

echo "q42"
taskset -c 10 ./q42
sleep 60

echo "q43"
taskset -c 10 ./q43
