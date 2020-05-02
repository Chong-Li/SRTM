#!/bin/bash
i=1
while [ "$i" -le "$1" ]; do
#	../bin/subscriber_dump 1 "$i" 1 10.0.0.26:4161 &
#	chrt -r -p 94 $!
        $GOPATH/bin/subscriber 1 "$i" 10.0.0.26:4161 &
	chrt -r -p 95 $!
	i=$(($i+1))
done

$GOPATH/bin/subscriber 1 0 10.0.0.26:4161 &
chrt -r -p 95 $!

#../bin/subscriber_func 1 0 3 192.168.1.11:4161 &
#chrt -r -p 95 $!
