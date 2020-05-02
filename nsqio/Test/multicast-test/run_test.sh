#!/bin/bash
pkill publisher
pkill subscriber
sleep 1
# default setting for script
numTopic=1 # number of processes
numTest=1 # number of gortouine for each topic-channel

interface="em1"
nsqlookupaddr="127.0.0.1"

# parse parameter
if [ "$#" -ge 1 ]; then
	numTopic=$1
fi

if [ "$#" -ge 2 ]; then
	numTest=$2
fi

if [ "$#" -ge 3 ]; then
	interface=$3
fi

if [ "$#" -ge 4 ]; then
	nsqlookupaddr=$4
fi

sh publisher.sh 1 $numTest $numTopic
sleep 1

sh subscriber.sh 1 $numTest $numTopic 1 $interface $nsqlookupaddr
