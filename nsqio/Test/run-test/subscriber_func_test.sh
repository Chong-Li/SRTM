#!/bin/bash
$GOPATH/bin/subscriber_func 1 1 3 10.0.0.26:4161 &
chrt -r -p 95 $!
sleep 10
$GOPATH/bin/subscriber_func 1 2 3 10.0.0.26:4161 &
chrt -r -p 95 $!
