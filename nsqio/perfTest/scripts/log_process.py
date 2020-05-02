#!/usr/bin/python

import os.path
import argparse
import numpy as np
import fileinput
if __name__ == '__main__':
#	for i in xrange(1000):
#		print i*10, processJob(i*10)

#else:
	parser = argparse.ArgumentParser()

	parser.add_argument("-d", "--dir", type=str, default=".",
						help="the directory to read log file")
	args = parser.parse_args()

	if not os.path.isdir(args.dir):
		print("Error: given directory does not exist!")
		exit(-1)

	for ctype in ['tcp', 'udp']:
		for mode in ['fanout', 'fanin']:
			for num in [64, 128, 256, 512]:
				print("For "+ctype+ " in "+mode+" with "+str(num)+":\n")
				print("Delay Info:")
				meanStat = []
				varStat = []
				if mode == 'fanout':
					for x in xrange(num):
						fileName = args.dir+"/"+ctype+"_"+mode+"_oneway_"+str(num)+"_"+str(x)
						if os.path.isfile(fileName):
							a = np.loadtxt(fileName)
							if sum(a <= 0) > 1 :
								print("error", sum(a<=0), fileName)
							#a = a[a>0]
							meanStat.append(np.mean(a))
							varStat.append(np.var(a))
				if mode == 'fanin':
					fileName = args.dir+"/"+ctype+"_"+mode+"_oneway_"+str(num)+"_1"
					if os.path.isfile(fileName):
						a = np.loadtxt(fileName)
						if sum(a <= 0) > 1 :
							print("error", sum(a<=0), fileName)
						#a = a[a>0]
						meanStat.append(np.mean(a))
						varStat.append(np.var(a))
				print("Mean delay: ")
				meanStat = np.array(meanStat)
				meanStat.astype(int)
				varStat = np.array(varStat)
				varStat.astype(int)
				print(meanStat/1000.0)
				print("delay Var: ")
				print(varStat/1000.0/1000.0)
				if len(meanStat) > 1:
					print("Overall Mean delay: ")
					print(np.mean(meanStat)/1000.0)
					print("Overall Var delay: ")
					print(np.var(meanStat)/1000.0/1000.0)

				print("CPU utilization info for producer:")
				fileName = args.dir+"/"+ctype+"_"+mode+"_prod_"+str(num)+".log"
				if os.path.isfile(fileName):
					for line in fileinput.input(fileName):
					    print line,
				print("CPU utilization info for consumer:")
				fileName = args.dir+"/"+ctype+"_"+mode+"_cons_"+str(num)+".log"
				if os.path.isfile(fileName):
					for line in fileinput.input(fileName):
					    print line,
