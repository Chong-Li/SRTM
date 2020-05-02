#!/bin/bash

broadcastaddr="10.0.0.26"

lookupaddr="10.0.0.26"
lookupport="4160"

interface="em1"

if [ "$#" -ge 1 ]; then
	interface=$1
fi

if [ "$#" -ge 2 ]; then
	lookupaddr=$2
fi

if [ "$#" -ge 3 ]; then
	lookupport=$3
fi

if [ "$#" -ge 4 ]; then
    broadcastaddr=$4
fi



$GOPATH/bin/nsqd -verbose=false -mem-queue-size=20000 -data-path="htmp"  -tls-required=false -tls-min-version='ssl3.0' --broadcast-address=$broadcastaddr --interface=$interface --tcp-address=0.0.0.0:5010 --http-address=0.0.0.0:5011 --lookupd-tcp-address=$lookupaddr:$lookupport  -daemon-priority=HIGH &
chrt -r -p 95 $!

$GOPATH/bin/nsqd -verbose=false -mem-queue-size=20000 -data-path="ltmp"  -tls-required=false -tls-min-version='ssl3.0' --broadcast-address=$broadcastaddr --interface=$interface --tcp-address=0.0.0.0:5012 --http-address=0.0.0.0:5013 --lookupd-tcp-address=$lookupaddr:$lookupport -daemon-priority=LOW &
chrt -r -p 94 $!
