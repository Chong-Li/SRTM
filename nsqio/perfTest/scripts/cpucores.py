#!/usr/bin/python

import os
import sys
import commands
if len(sys.argv) != 2 :
    print("Wrong argument, should ./cpucores.py")

coreNum = int(commands.getoutput("nproc --all"))
coreEnable = 0
if sys.argv[1] == "all":
    coreEnable = coreNum
else :
    coreEnable = int(sys.argv[1])

for i in xrange(coreNum):
    if i+1 <= coreEnable:
        os.system("echo 1 > /sys/devices/system/cpu/cpu"+str(i)+"/online")
    else:
        os.system("echo 0 > /sys/devices/system/cpu/cpu"+str(i)+"/online")
