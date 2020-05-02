#!/bin/python

import psutil
import commands
import time
import argparse
import numpy as np

if __name__ == '__main__':
#	for i in xrange(1000):
#		print i*10, processJob(i*10) 

#else:
	parser = argparse.ArgumentParser()
	parser.add_argument("-n", "--name",
					help="the name of the program we are interested in", default="iperf3")
	parser.add_argument("-m", "--mode", 
					help="the mode for showing status", default=0)
	parser.add_argument("-t", "--time", 
					help="seconds to run the measure", default=10)
	parser.add_argument("-k", "--keep", action='store_true',
					help="whether keep trying or not")

	args = parser.parse_args()

	pid = commands.getoutput("pgrep "+args.name)
	pids = pid.split('\n')
	if len(pids) > 1 :
		print(pids)
	pid = int(pids[0])

	while True:
		try:
			p = psutil.Process(pid)
			break
		except:
			# sleep for 1 second to try again
			print("No process named \""+args.name+"\"")
			if not args.keep:
				exit()
			time.sleep(1)

	results = p.cpu_times()
	lastutime = results[0]
	laststime = results[1]

	utimeList = []
	stimeList = []

	passTime = 0
	if args.time == 0:
		args.time = inf
	while passTime < args.time:
		time.sleep(1)
		passTime += 1
		try :
			results = p.cpu_times()
		except:
			print("Program "+args.name+" exits")
			break
		utime = results[0]
		stime = results[1]
		print(utime - lastutime, "\t", stime - laststime)
		utimeList.append(utime - lastutime)
		stimeList.append(stime - laststime)
		lastutime = utime 
		laststime = stime
		#print(utimeList)
	print("The average user CPU usage is " +str(100*np.mean(utimeList)) + "%")
	print("The average system CPU usage is " + str(100*np.mean(stimeList)) + "%")




