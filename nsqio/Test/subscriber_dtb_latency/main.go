package main

import (
	"os"
	"runtime"
	"strconv"
	"time"

	"github.com/WU-CPSL/RTM-0.1/nsqio/Test/subscriber_dtb_latency/mq"
)

func newTest(msgCount int, channel string, lookupd string) {
	mq.NewNsq(msgCount, channel, lookupd)
}

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	num, _ := strconv.Atoi(os.Args[1])
	topic, _ := strconv.Atoi(os.Args[2])
	for i := 0; i < num; i++ {
		go newTest(13000, strconv.Itoa(topic+i), os.Args[3]) //parseArgs(usage)
	}
	for {
		time.Sleep(20 * time.Second)
	}
}
