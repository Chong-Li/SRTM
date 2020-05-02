# perfTest
Simple Client and Server applications for test throughput, latency and measure CPU utilization of linux socket using c and golang.

## One-to-one connection Test
One-to-one connection intends to test for the performance with continuous sending using one core.

c_sock directory contains the code for one-to-one connection performance test program for c socket using TCP or UDP

go_sock directory contains the code for one-to-one connection performance test programs for go socket using TCP or UDP

## Scenario Test
We try to mimic the NSQ/RTM sending scenarios, fan-in and fan-out scenarios, see slides for more details.

scenarios directory contains the code and scripts for running fan-in and fan-out scenarios test. 

## Others
draw directory contains the matlab files to draw the latency results. Put the log files from test in each directory c_go/scenarios and run the matlab script to show the results.

scripts directory contains some scripts
- cpucores.py can quickly enable/disable the cpu cores
- measure.py used to log the cpu utilization for each program.
- log_process,py and log_p.py, quickly get the mean and variance of the latency from log files.


