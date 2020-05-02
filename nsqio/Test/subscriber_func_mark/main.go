package main

import (
	"os"
	"runtime"
	"strconv"
	"time"

	"github.com/WU-CPSL/RTM-0.1/nsqio/Test/subscriber_func_mark/mq"
)

func newTest(msgSize int, channel string, sources int, lookupd string) {
	mq.NewNsq(msgSize, channel, sources, lookupd)
}

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	num, _ := strconv.Atoi(os.Args[1])
	topic, _ := strconv.Atoi(os.Args[2])
	sources, _ := strconv.Atoi(os.Args[3])
	for i := 0; i < num; i++ {
		go newTest(512, strconv.Itoa(topic+i), sources, os.Args[4]) //parseArgs(usage)
	}
	for {
		time.Sleep(20 * time.Second)
	}
}
