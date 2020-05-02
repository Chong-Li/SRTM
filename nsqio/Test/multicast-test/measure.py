#!/usr/bin/python

import psutil
import commands
import time
import argparse
import numpy as np

if __name__ == '__main__':


	p = {} # record each process
	result = {}
	for prog in ["nsqd", "nsqlookupd"]
		pid = commands.getoutput("pgrep ")
		if pid == "":
			print("no process for " + prog)
			continue
		pids = pid.split('\n')
		if (len(pids) > 1) :
			print("more than one process of " + prog + ":" + str(pids))
			exit(1)
		p[prog] = psutil.Process(int(pid[0]))

	# initialize result
	results = {}
	for key in p.keys():
		results[key] = (0,0)
	for i in ["usr", "sys"]:
		results[i] = (0,0)


	for 

	print("Number of process running is " + str(len(pids)))
	print("The average user CPU usage is " +str(sum(ucpu)) + "%, " + \
	"system CPU usage is " + str(sum(scpu)) + "%, " + \
	"overall CPU usage is " + str(sum(ucpu) + sum(scpu)) + "%")
	print("Total user CPU usage is " + str(utcpu) + "%, " + \
	"system CPU usage is " + str(stcpu) + "%, " + \
	"overall CPU usage is " + str(utcpu+stcpu) + "%")
	print("Total packets transmitted  " + str(endNum - startNum))
