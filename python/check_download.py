#!/usr/bin/env python3
import os

base_dir = "/home/rui/ds/"
dirs = os.listdir(base_dir)
count = 0
for d in dirs:
    if os.path.isdir(d):
        files = os.listdir(base_dir + d)
        if len(files) != 16:
            print("Lost file in " + d)
        count += 1
if count != 108:
    print("Lost directory!")
