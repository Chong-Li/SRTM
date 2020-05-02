package main

import (
	"os"
	"runtime"
	"strconv"
	"time"

	"github.com/WU-CPSL/RTM-0.1/nsqio/Test/subscriber_poi/mq"
)

func newTest(msgCount int, channel string, index int, lookupd string) {
	mq.NewNsq(msgCount, channel, index, lookupd)
}

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	num, _ := strconv.Atoi(os.Args[1])
	topic, _ := strconv.Atoi(os.Args[2])
	for i := 0; i < num; i++ {
		go newTest(60000, strconv.Itoa(topic+i), 0, os.Args[3]) //parseArgs(usage)
		go newTest(60000, strconv.Itoa(topic+i), 1, os.Args[3])
	}
	for {
		time.Sleep(20 * time.Second)
	}
}
