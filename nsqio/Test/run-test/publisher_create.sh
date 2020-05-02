#!/bin/bash
i=1
while [ "$i" -le "$1" ]; do
	#taskset -c 0-8,11-39 ../bin/publisher_dump 1 "$i" 1 127.0.0.1:4161 HIGH &
	taskset -c 0-8,11-39 $GOPATH/bin/publisher 1 "$i" 2048 127.0.0.1:4161 HIGH &
	chrt -r -p 95 $!
	i=$(($i+1))
done

#taskset -c 0-8,11-39 ../bin/publisher_func 3 1 1 127.0.0.1:4161 HIGH &
#taskset -c 0-8,11-39 ../bin/publisher_dump 1 1 5 127.0.0.1:4161 HIGH &
#chrt -r -p 95 $!

taskset -c 0-8,11-39 $GOPATH/bin/publisher 1 0 2048 127.0.0.1:4161 HIGH &
chrt -r -p 95 $!
