'''
Author: SunZewen
Date: 2022-01-10 14:52:57
LastEditors: SunZewen
LastEditTime: 2022-01-10 19:27:39
Description:   
FilePath: /workspace/ecnu/ecnu/hef/test/sdb/hashjoin/l1_run.py
'''


import os
import time
import subprocess
import sys
import psutil

if __name__ == '__main__':
    
    if len(sys.argv) <= 1:
        print("enter a arg")
    
    candidate_dir_path = "/home/sunzw/workspace/ecnu/ecnu/hef/test/sdb/hashjoin/"

    compile_cmd = "taskset -c 1 /home/sunzw/usr/gcc-10.2.0/bin/g++ -std=c++20 -O3 -mavx512f -mavx512dq " + \
    candidate_dir_path + sys.argv[1] + '.cpp -o ' + candidate_dir_path + sys.argv[1]
    os.system(compile_cmd)
    time.sleep(5)

    # run_cmd = "/home/sunzw/workspace/ecnu/ecnu/hef/test/sdb/hashjoin/" + sys.argv[1]
    run_cmd = candidate_dir_path  + sys.argv[1]

    loop_times = 5
    run_time = 0.0

    p = psutil.Process()
    p.cpu_affinity([10])

    for i in range(0, loop_times):
        std_out_put = subprocess.run(
                [run_cmd], timeout=None, shell=False, stderr=subprocess.PIPE, stdout=subprocess.PIPE, encoding="utf-8")

        output_string = std_out_put.stdout
        output_lines = output_string.split("\n")
        print(output_string)
        run_time += float(output_lines[len(output_lines)-1])
        time.sleep(5)

    run_time = round(run_time/5.0, 3)
    print(sys.argv[1] + ' runtime is : ' + str(run_time))
