#!/usr/bin/python
import os
import time
import argparse

parser = argparse.ArgumentParser()
parser.add_argument('-c',"--ctype", default = "all",
                    help='test for tcp, udp, or all')
parser.add_argument('-b', "--buildonly", help="go build everything only",
                    action="store_true")
parser.add_argument("-t", "--type", default="all",
                    help="type of test, fanin, fanout, or fanout")
parser.add_argument("-a", "--address", default="10.0.0.26",
                    help="ip address of producer")
parser.add_argument("-s", "--stopnum", type= int, default=20000,
                    help="number of message to send before stop")
parser.add_argument("-l", "--length", type=int, default=1000,
                    help="message length")

args = parser.parse_args()

if (args.buildonly) :
    os.system("go build Consumer.go")
    os.system("go build Producer.go")
    exit(0)

if args.type == "fanout" or args.type == "all":
    if args.ctype == "tcp" or args.ctype == "all":
        for i in [6,7,8,9]:
            print("\nTest TCP for Fanout model with "+ str(2**i) +" concurrent consumers and 1 producer")
            os.system("./Producer -c tcp -l="+str(args.length)+" -s="+ str(args.stopnum) +" -pn 1 -n="+str(2**i) + " &")
            time.sleep(1)
            os.system("python ../scripts/measure.py -n Producer -t 0 > log/tcp_fanout_prod_"+str(2**i) + ".log")
    if args.ctype == "udp" or args.ctype == "all":
        for i in [6,7,8,9]:
            print("\nTest UDP for Fanout model with "+ str(2**i) +" concurrent consumers and 1 producer")
            os.system("./Producer -c udp -l="+str(args.length)+" -s="+ str(args.stopnum) +" -pn 1 -n="+str(2**i) + " &")
            time.sleep(1)
            os.system("python ../scripts/measure.py -n Producer -t 0 > log/udp_fanout_prod_"+str(2**i) + ".log")
    if args.ctype == "multicast" or args.ctype == "all":
        for i in [6,7,8,9]:
            print("\nTest Multicast for Fanout model with "+ str(2**i) +" concurrent consumers and 1 producer")
            os.system("./Producer -c multicast -l="+str(args.length)+" -s="+ str(args.stopnum) +" -pn 1 -n="+str(2**i) + " &")
            time.sleep(1)
            os.system("python ../scripts/measure.py -n Producer -t 0 > log/mtc_fanout_prod_"+str(2**i) + ".log")
print("\n\n")
if args.type == "fanin" or args.type == "all":
    if args.ctype == "tcp" or args.ctype == "all":
        for i in [6,7,8,9]:
            print("\nTest TCP for Fanin model with 1 consumers and "+ str(2**i) +" producer")
            os.system("./Producer -c tcp -l="+str(args.length)+" -s=" + str(args.stopnum) + " -n=1 -pn="+str(2**i)+" &")
            time.sleep(1)
            os.system("python ../scripts/measure.py -n Producer -t 0 > log/tcp_fanin_prod_"+str(2**i) + ".log")
    if args.ctype == "udp" or args.ctype == "all":
        for i in [6,7,8,9]:
            print("\nTest UDP for Fanin model with 1 consumers and "+ str(2**i) +" producer")
            os.system("./Producer -c udp -l="+str(args.length)+" -s=" + str(args.stopnum) + " -n=1 -pn="+str(2**i)+" &")
            time.sleep(1)
            os.system("python ../scripts/measure.py -n Producer -t 0 > log/udp_fanin_prod_"+str(2**i) + ".log")
    if args.ctype == "multicast" or args.ctype == "all":
        for i in [6,7,8,9]:
            print("\nTest Multicast for Fanin model with 1 consumers and "+ str(2**i) +" producer")
            os.system("./Producer -c multicast -l="+str(args.length)+" -s=" + str(args.stopnum) + " -n=1 -pn="+str(2**i)+" &")
            time.sleep(1)
            os.system("python ../scripts/measure.py -n Producer -t 0 > log/mtc_fanin_prod_"+str(2**i) + ".log")
