#!/bin/python

import os
import time

intervals = [1000, 800, 600, 400, 200, 100, 80, 60, 40, 20, 10, 0];
stopCounts = [10000000/interval*1.5 for interval in intervals[:-1]] 
stopCounts.append(10000000/10)

pktLenList = [1500, 1000, 500, 100, 20]

for pktlen in pktLenList:
	print("For packet size ", pktlen, " Bytes\n")
	for i in range(len(intervals)):
		print("For interval ", intervals[i], " :")
		os.system("./udpclient 10.0.0.27 1100 " + str(stopCounts[i]) + " " + str(pktlen) + " " + str(intervals[i]) + " > udplog/udp_"+str(pktlen) + "_" + str(intervals[i]) + " &")
		time.sleep(2)
		os.system("python measure.py -n udpclient -t 0")
		time.sleep(5)
		print("\n")
	print("\n")

