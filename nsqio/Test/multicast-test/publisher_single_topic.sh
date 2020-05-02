#!/bin/bash

# default setting for script
num=1 # number of publisher processes
numTest=1 # number of goroutines for each topic.
numTopic=1 # number of topics in each publisher process
nsqlookupaddr="10.0.0.26"
nsqlookupport="4161"

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
	nsqlookupaddr=$4
fi

if [ "$#" -ge 5 ]; then
	nsqlookupport=$5
fi

i=1


while [ "$i" -le "$num" ]; do
	$GOPATH/bin/publisher_mtc $numTest $numTopic 1000 $nsqlookupaddr:$nsqlookupport HIGH &
	chrt -r -p 95 $!
	i=$(($i+1))
done
