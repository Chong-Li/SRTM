package main

import (
	"encoding/binary"
	"log"
	"os"
	"runtime"
	"strconv"
	"time"

	"github.com/WU-CPSL/RTM-0.1/nsqio/Test/publisher/mq"
)

func newTest(msgCount, msgSize int, topic string, gap int, lookupd string, priority string) {
	nsq := mq.NewNsq(msgCount, msgSize, topic, lookupd, priority)
	start := time.Now().UnixNano()
	b := make([]byte, 24)
	id := make([]byte, 5)
	for i := 0; i < msgCount; i++ {
		if i == 1 {
			time.Sleep(5 * time.Second)
		}
		binary.PutVarint(b, time.Now().UnixNano())
		binary.PutVarint(id, int64(i))
		copy(b[19:23], id[:])
		nsq.Send(b)

		// inter-msg gap
		time.Sleep(time.Duration(gap) * time.Microsecond)
	}

	stop := time.Now().UnixNano()
	ms := float32(stop-start) / 1000000
	log.Printf("Sent %d messages in %f ms\n", msgCount, ms)
	log.Printf("Sent %f per second\n", 1000*float32(msgCount)/ms)

	//	nsq.Teardown()
	for {
		time.Sleep(50 * time.Second)
	}
}

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	num, _ := strconv.Atoi(os.Args[1])
	topic, _ := strconv.Atoi(os.Args[2])
	gap, _ := strconv.Atoi(os.Args[3])
	for i := 0; i < num; i++ {
		go newTest(13001, 512, strconv.Itoa(topic+i), gap, os.Args[4], os.Args[5])
	}
	for {
		time.Sleep(50 * time.Second)
	}

}
