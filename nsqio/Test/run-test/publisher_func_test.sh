#!/bin/bash
taskset -c 0-8,11-39 $GOPATH/bin/publisher_func 3 1 1 127.0.0.1:4161 HIGH &
chrt -r -p 95 $!
