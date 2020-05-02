package main

import (
	"os"
	"runtime"
	"strconv"
	"time"

	"github.com/WU-CPSL/RTM-0.1/nsqio/Test/subscriber_mtc/mq"
)

func newTest(msgCount int, id string, topic string, channel string, lookupd string, intf string, resendTimeout time.Duration, maxHardTimeout time.Duration) {
	mq.NewNsq(msgCount, id, topic, channel, lookupd, intf, resendTimeout, maxHardTimeout)
}

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	num, _ := strconv.Atoi(os.Args[1])
	numTopic, _ := strconv.Atoi(os.Args[2])
	numChannel, _ := strconv.Atoi(os.Args[3])
	intf := "eth0"
	resendTimeout, _ := time.ParseDuration("10ms")
	maxHardTimeout, _ := time.ParseDuration("40ms")

	if len(os.Args) > 5 {
		intf = os.Args[5]
	}
	if len(os.Args) > 6 {
		resendTimeout, _ = time.ParseDuration(os.Args[6])

	}
	if len(os.Args) > 7 {
		maxHardTimeout, _ = time.ParseDuration(os.Args[7])
	}
	for k := 0; k < num; k++ {
		for i := 0; i < numTopic; i++ {
			for j := 0; j < numChannel; j++ {
				if numChannel == 1 {
					go newTest(130, strconv.Itoa(k)+"_"+strconv.Itoa(i)+"_"+strconv.Itoa(j), strconv.Itoa(i), "multicast", os.Args[4], intf, resendTimeout, maxHardTimeout)
				} else {
					go newTest(130, strconv.Itoa(k)+"_"+strconv.Itoa(i)+"_"+strconv.Itoa(j), strconv.Itoa(i), strconv.Itoa(j), os.Args[4], intf, resendTimeout, maxHardTimeout) //parseArgs(usage)
				}
			}
		}
	}
	for {
		time.Sleep(20 * time.Second)
	}
}
