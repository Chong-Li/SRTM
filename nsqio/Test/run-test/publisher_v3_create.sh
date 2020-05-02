#!/bin/bash
i=1
while [ "$i" -le "$1" ]; do
	$GOPATH/bin/publisher_dump_v3_poisson "$2" "$i" 3 192.168.40.7:4161 LOW &
	#taskset -c 0-8,16-31 $GOPATH/bin/publisher_v3 1 "$i" 2048 192.168.40.4:4161 HIGH &
	chrt -r -p 94 $!
	i=$(($i+1))
#	result=$(python -c "import random;print(random.uniform(1.0, 2.0))")
#	echo $result
#	sleep $result
	sleep 0.01
done

$GOPATH/bin/publisher_v3_burst "$3" 0 "$4" 192.168.40.7:4161 HIGH &
#$GOPATH/bin/publisher_v3_dyn "$3" 0 "$4" 192.168.40.4:4161 HIGH &
chrt -r -p 95 $!
printf "last ~~~~~~~\n"
#sleep 22.56
#$GOPATH/bin/publisher_v3_burst 10 0 1 192.168.40.7:4161 HIGH &
#chrt -r -p 95 $!
#sleep 13.22
#$GOPATH/bin/publisher_v3_burst 10 0 1 192.168.40.7:4161 HIGH &
#chrt -r -p 95 $!
#printf "lastlast ~~~~~~~\n"
