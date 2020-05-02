#!/usr/bin/python

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
	parser.add_argument("-n", "--name",type=str,
					help="the name of the program we are interested in", default="iperf3")
	parser.add_argument("-m", "--mode", type=int,
					help="the mode for showing status", default=0)
	parser.add_argument("-t", "--time", type=int,
					help="seconds to run the measure", default=10)
	parser.add_argument("-k", "--keep", action='store_true',
					help="whether keep trying or not")
	parser.add_argument("-p", "--printout", action='store_true',
					help="whether printout cpu consumption each second")

	args = parser.parse_args()
	p = []

	startNum = 0
	if args.name == "Producer":
		startNum = int(commands.getoutput("echo `ifconfig em1 | grep 'TX packets'`| cut -d \" \" -f3"))
	elif args.name == "Consumer":
		startNum = int(commands.getoutput("echo `ifconfig em1 | grep 'RX packets'`| cut -d \" \" -f3"))

	pid = commands.getoutput("pgrep "+args.name)
	pids = pid.split('\n')
	if (len(pids) > 1) :
		print(pids)

	try:
		for pid in pids:
		    p.append(psutil.Process(int(pid)))
	except:
		# sleep for 1 second to try again
		print("No process named \""+args.name+"\"")
		if not args.keep:
			exit()
	time.sleep(1)
	cur_time = time.time()
	lastutime = [0 for i in xrange(len(pids))]
	laststime = [0 for i in xrange(len(pids))]

	for i in xrange(len(pids)):
		results = p[i].cpu_times()
    	lastutime[i] = results[0]
    	laststime[i] = results[1]

	rTotal = psutil.cpu_times()
	lastutotal = rTotal.user
	laststotal = rTotal.system


	utimeList = [[] for i in xrange(len(pids))]
	stimeList = [[] for i in xrange(len(pids))]
	uTotalList = []
	sTotalList = []

	quitFlag = False
	passTime = 0
	if args.time == 0:
		args.time = np.inf

	while passTime < args.time:
		if (quitFlag):
			break
		last_time = cur_time
		time.sleep(1)
		cur_time = time.time()
		passTime += 1
		if args.printout: print("For time " + str(passTime))
		for i in xrange(len(p)):
			try :
				results = p[i].cpu_times()
			except:
				print("Program "+args.name+" exits")
				quitFlag = True
				break
			utime = results[0]
			stime = results[1]

			uPercent = (utime - lastutime[i])*1.0/(cur_time - last_time)
			sPercent = (stime - laststime[i])*1.0/(cur_time - last_time)
			if args.printout : print("For process id " + str(pid[i]) + " user cpu consumption is "+ str(uPercent) + "% system cpu consumption is  " + str(sPercent) +"%")
			utimeList[i].append(uPercent)
			stimeList[i].append(sPercent)
			lastutime[i] = utime
			laststime[i] = stime
		rTotal = psutil.cpu_times()
		uTotal = rTotal.user
		sTotal = rTotal.system
		uTPercent = (uTotal - lastutotal)*1.0/(cur_time - last_time)
		sTPercent = (sTotal - laststotal)*1.0/(cur_time - last_time)
		if args.printout : print("Total user cpu consumption " + str(uTPercent) + "% and system cpu consumption " + str(sTPercent) + "%")
		uTotalList.append(uTPercent)
		sTotalList.append(sTPercent)
		lastutotal = uTotal
		laststotal = sTotal

	sizes = [len(utimeList[i]) for i in xrange(len(pids))]
	ucpu = [np.mean(utimeList[i][sizes[i]/4:sizes[i]*3/4])*100 for i in xrange(len(pids))]
	scpu = [np.mean(stimeList[i][sizes[i]/4:sizes[i]*3/4])*100 for i in xrange(len(pids))]
	size = len(uTotalList)
	utcpu = np.mean(uTotalList[size/4:size*3/4])*100
	stcpu = np.mean(sTotalList[size/4:size*3/4])*100


	endNum = 0
	if args.name == "Producer":
		endNum = int(commands.getoutput("echo `ifconfig em1 | grep 'TX packets'`| cut -d \" \" -f3"))
	elif args.name == "Consumer":
		endNum = int(commands.getoutput("echo `ifconfig em1 | grep 'RX packets'`| cut -d \" \" -f3"))

	print("Number of process running is " + str(len(pids)))
	print("The average user CPU usage is " +str(sum(ucpu)) + "%, " + \
	"system CPU usage is " + str(sum(scpu)) + "%, " + \
	"overall CPU usage is " + str(sum(ucpu) + sum(scpu)) + "%")
	print("Total user CPU usage is " + str(utcpu) + "%, " + \
	"system CPU usage is " + str(stcpu) + "%, " + \
	"overall CPU usage is " + str(utcpu+stcpu) + "%")
	print("Total packets transmitted  " + str(endNum - startNum))
