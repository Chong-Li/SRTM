#!/bin/bash
i=1
while [ "$i" -le "$1" ]; do
	taskset -c 0-5,8-39 $GOPATH/bin/publisher_dump 3 "$i" 10 127.0.0.1:4161 HIGH &
	chrt -r -p 95 $!
	i=$(($i+1))
done


#taskset -c 0-5,8-39 ../bin/publisher 1 0 2048 127.0.0.1:4161 HIGH &
#chrt -r -p 95 $!

taskset -c 0-5,8-39 $GOPATH/bin/publisher_dump 1 0 10 127.0.0.1:4161 LOW &
chrt -r -p 94 $!
