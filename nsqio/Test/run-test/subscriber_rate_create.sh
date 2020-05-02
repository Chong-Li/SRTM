#!/bin/bash
i=1
while [ "$i" -le "$1" ]; do
	$GOPATH/bin/subscriber_dump 1 "$i" 10 10.0.0.26:4161 HIGH &
	chrt -r -p 95 $!
	i=$(($i+1))
done


#../bin/subscriber 1 0 10.0.0.26:4161 HIGH &
#chrt -r -p 95 $!

$GOPATH/bin/subscriber_dump_record 1 0 1 10.0.0.26:4161 LOW &
chrt -r -p 94 $!
