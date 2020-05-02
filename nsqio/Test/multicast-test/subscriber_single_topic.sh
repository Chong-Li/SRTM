#!/bin/bash

# default setting for script
num=1 # number of processes
numTest=1 # number of gortouine for each topic-channel
numTopic=1 # number of topics in each process (same channel)
numChannel=1 # number of channels used, one channel=one goroutine
nsqlookupaddr="10.0.0.26"
nsqlookupport="4161"
interface="em1"
resendTimeout="100ms"
maxHardTimeout="400ms"

# parse parameter
if [ "$#" -ge 1 ]; then
	num=$1
fi

if [ "$#" -ge 2 ]; then
	numTest=$2
fi

if [ "$#" -ge 3 ]; then
	numTopic=$3
fi

if [ "$#" -ge 4 ]; then
	numChannel=$4
fi

if [ "$#" -ge 5 ]; then
	interface=$5
fi

if [ "$#" -ge 6 ]; then
	nsqlookupaddr=$6
fi

if [ "$#" -ge 7 ]; then
	nsqlookupport=$7
fi

i=1

while [ "$i" -le "$num" ]; do
	$GOPATH/bin/subscriber_mtc $numTest $numTopic $numChannel $nsqlookupaddr:$nsqlookupport $interface $resendTimeout $maxHardTimeout &
	chrt -r -p 95 $!
	i=$(($i+1))
done
