#!/bin/bash
taskset -c 0-8,11-39 $GOPATH/bin/publisher_func_v3 3 1 1 127.0.0.1:4161 HIGH &
chrt -r -p 95 $!

sleep 10
taskset -c 0-8,11-39 $GOPATH/bin/publisher_func_v3 3 1 1 127.0.0.1:4161 HIGH &
chrt -r -p 95 $!

taskset -c 0-8,11-39 $GOPATH/bin/publisher_func_v3 3 2 1 127.0.0.1:4161 HIGH &
chrt -r -p 95 $!
