#!/bin/bash
cpupower idle-set -D 1
i=0
while [ "$i" -le "15" ]; do
	echo performance > /sys/devices/system/cpu/cpu"$i"/cpufreq/scaling_governor
	i=$(($i+1))
done


