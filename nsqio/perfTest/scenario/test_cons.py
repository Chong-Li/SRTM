#!/usr/bin/python
import os
import time
import argparse

parser = argparse.ArgumentParser()
parser.add_argument('-c',"--ctype", default="all",
                    help='test for tcp, udp, or all')
parser.add_argument('-b', "--buildonly", help="go build everything only",
                    action="store_true")
parser.add_argument("-t", "--type", default="all",
                    help="type of test,  fanin, fanout, or all")
parser.add_argument("-a", "--address", default="10.0.0.26",
                    help="ip address of producer")
parser.add_argument("-s", "--stopnum", type= int, default=20000,
                    help="number of message to send before stop")
parser.add_argument("-l", "--length", type=int, default=1000,
                    help="message length")
parser.add_argument("-r", "--records", type=int, default=10000,
                    help="number of records")

args = parser.parse_args()

if (args.buildonly) :
    os.system("go build Consumer.go")
    os.system("go build Producer.go")
    exit(0)

if args.type == "fanout" or args.type == "all":
    if args.ctype == "tcp" or args.ctype == "all":
        for i in [6,7,8,9]:
            print("\nTest TCP for Fanout model with "+ str(2**i) +" concurrent consumers and 1 producer")
            os.system("./Consumer -c tcp -a="+ args.address +" -l="+str(args.length)+" -s="+ str(args.stopnum) +" -rl="+str(args.records)+" -pn 1 -n="+str(2**i)+ \
                " -of=log/tcp_fanout_oneway_"+str(2**i) + " -rf=log/tcp_fanout_roundtrip_" + str(2**i)+" &")
            time.sleep(1)
            os.system("python ../scripts/measure.py -n Consumer -t 0 > log/tcp_fanout_cons_"+str(2**i) + ".log" )
            time.sleep(5)
    if args.ctype == "udp" or args.ctype == "all":
        for i in [6,7,8,9]:
            print("\nTest UDP for Fanout model with "+ str(2**i) +" concurrent consumers and 1 producer")
            os.system("./Consumer -c udp -a="+ args.address +" -l="+str(args.length)+" -s="+ str(args.stopnum) +" -rl="+str(args.records)+" -pn 1 -n="+str(2**i)+ \
                " -of=log/udp_fanout_oneway_"+str(2**i) + " -rf=log/udp_fanout_roundtrip_" + str(2**i)+" &")
            time.sleep(1)
            os.system("python ../scripts/measure.py -n Consumer -t 0 > log/udp_fanout_cons_"+str(2**i) + ".log")
            time.sleep(5)
    if args.ctype == "multicast" or args.ctype == "all":
        for i in [6,7,8,9]:
            print("\nTest Multicast for Fanout model with "+ str(2**i) +" concurrent consumers and 1 producer")
            os.system("./Consumer -c multicast -a="+ args.address +" -l="+str(args.length)+" -s="+ str(args.stopnum) +" -rl="+str(args.records)+" -pn 1 -n="+str(2**i)+ \
                " -of=log/mtc_fanout_oneway_"+str(2**i) + " -rf=log/umtc_fanout_roundtrip_" + str(2**i)+" &")
            time.sleep(1)
            os.system("python ../scripts/measure.py -n Consumer -t 0 > log/mtc_fanout_cons_"+str(2**i) + ".log")
            time.sleep(5)
print("\n\n")
if args.type == "fanin" or args.type == "all":
    if args.ctype == "tcp" or args.ctype == "all":
        for i in [6,7,8,9]:
            print("\nTest TCP for Fanin model with "+ str(2**i) +" concurrent producers and 1 consumer")
            os.system("./Consumer -c tcp -a="+ args.address +" -l="+str(args.length)+" -s="+ str(args.stopnum) +" -rl="+str(args.records)+" -n=1 -pn="+str(2**i)+ \
                " -of=log/tcp_fanin_oneway_"+str(2**i) + " -rf=log/tcp_fanin_roundtrip_" + str(2**i)+" &")
            time.sleep(1)
            os.system("python ../scripts/measure.py -n Consumer -t 0 > log/tcp_fanin_cons_"+str(2**i) + ".log")
            time.sleep(5)
    if args.ctype == "udp" or args.ctype == "all":
        for i in [6,7,8,9]:
            print("\nTest UDP for Fanin model with "+ str(2**i) +" concurrent producers and 1 consumer")
            os.system("./Consumer -c udp -a="+ args.address +" -l="+str(args.length)+" -s="+ str(args.stopnum) +" -rl="+str(args.records)+" -n=1 -pn="+str(2**i)+ \
                " -of=log/udp_fanin_oneway_"+str(2**i) + " -rf=log/udp_fanin_roundtrip_" + str(2**i)+" &")
            time.sleep(1)
            os.system("python ../scripts/measure.py -n Consumer -t 0 > log/udp_fanin_cons_"+str(2**i) + ".log")
            time.sleep(5)
    if args.ctype == "multicast" or args.ctype == "all":
        for i in [6,7,8,9]:
            print("\nTest Multicast for Fanin model with "+ str(2**i) +" concurrent producers and 1 consumer")
            os.system("./Consumer -c multicast -a="+ args.address +" -l="+str(args.length)+" -s="+ str(args.stopnum) +" -rl="+str(args.records)+" -n=1 -pn="+str(2**i)+ \
                " -of=log/mtc_fanin_oneway_"+str(2**i) + " -rf=log/mtc_fanin_roundtrip_" + str(2**i)+" &")
            time.sleep(1)
            os.system("python ../scripts/measure.py -n Consumer -t 0 > log/mtc_fanin_cons_"+str(2**i) + ".log")
            time.sleep(5)
