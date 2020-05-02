#!/bin/bash
../bin/publisher_func 3 0 1 127.0.0.1:4161 HIGH &
chrt -r -p 95 $!

