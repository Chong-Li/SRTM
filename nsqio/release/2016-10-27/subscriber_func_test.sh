#!/bin/bash
../bin/subscriber_func 1 0 3 10.0.0.26:4161 &
chrt -r -p 95 $!
