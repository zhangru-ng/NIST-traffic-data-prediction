#!/usr/bin/env python3
import os
import sys

base_dir = "/home/rui/ds/"
base_url = "https://s3-us-west-2.amazonaws.com/2015nist7/"

sequence = ["00", "01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12", "13", "14"]
year = {"06" : sequence[9:13],
        "07" : sequence[1:13],
        "08" : sequence[1:13],
        "09" : sequence[1:13],
        "10" : sequence[1:13],
        "11" : sequence[1:13],
        "12" : sequence[1:13],
        "13" : sequence[1:13],
        "14" : sequence[1:13],
        "15" : sequence[1:9]}

count = 0

for year, months in year.items():
    for month in months:
        name = "cleaning_test_" + year + "_" + month
        path = base_dir + name
        if not os.path.exists(path):
            os.mkdir(path)
        os.chdir(path)
        if not os.path.exists("_SUCCESS"):
            os.system("wget " + base_url + name + "/_SUCCESS")
            print("Downloading " + name + "/_SUCCESS...")
        for i in sequence:
            if not os.path.exists("part-r-000" + i):
                os.system("wget " + base_url + name + "/part-r-000" + i)
                print("Downloading " + name + "/part-r-000" + i + "...")
        count += 1
        print(str(count) + " files finished")




