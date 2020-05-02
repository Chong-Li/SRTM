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

	parser.add_argument("-f", "--file", type=str, default=".",
						help="the log file to read")
	args = parser.parse_args()

	if not os.path.isfile(args.file):
		print("Error: given file does not exist!")
		exit(-1)

	a = np.loadtxt(args.file)
	a = a[a>0]
	print("Mean delay: ")
	print(np.mean(a)/1000.0)
	print("delay Var: ")
	print(np.var(a)/1000.0/1000.0)
