// +build rtm

package main

import (
	"encoding/binary"
	"log"
	"os"
	"runtime"
	"strconv"
	"time"
	"sync"

	"github.com/WU-CPSL/RTM-0.1/nsqio/Test/publisher_v3_burst2/mq"
)

func newTest(msgCount int, c *sync.Cond, msgSize int, topic string, index int, gap int, lookupd string, priority string) {
	nsq := mq.NewNsq(msgCount, msgSize, topic, index, lookupd, priority)
	start := time.Now().UnixNano()
	b := make([]byte, msgSize)
	strIndex:=[]byte(strconv.Itoa(index))
	strIndex=append(strIndex, "\n"...)
	i:=1

	repeat:=1
	binary.PutVarint(b[0:], time.Now().UnixNano())
	copy(b[24:],strIndex)
	nsq.Send(b)
	i++
	c.L.Lock()
	c.Wait()
	c.L.Unlock()
	ticker :=time.NewTicker(time.Millisecond * 30)
	for _= range ticker.C{
		/*if i >= 60001{
			break
		}*/
		for j:=0; j< repeat; j++ {
			binary.PutVarint(b, time.Now().UnixNano())
			copy(b[24:], strIndex)
			nsq.Send(b)
			i++
		}
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
	burst, _ := strconv.Atoi(os.Args[3])
	//var m sync.Mutex
	batchNum := num/burst
	m := make([]sync.Mutex, batchNum)
	c := make([]*sync.Cond, batchNum)
	//c := sync.NewCond(&m)
	i:=0
	for ; i < batchNum; i++ {
		c[i] = sync.NewCond(&m[i])
		for j:=0; j< burst; j++ {
			time.Sleep(100* time.Microsecond)
			go newTest(60001, c[i], 110, strconv.Itoa(topic), 0, burst, os.Args[4], os.Args[5])
		}
		//time.Sleep(time.Microsecond*(time.Duration(10000000/batchNum)))
	}
	for i:=0; i< batchNum; i++ {
		c[i].L.Lock()
		c[i].Broadcast()
		c[i].L.Unlock()
		//RTM.dtb
		time.Sleep(time.Microsecond*(time.Duration(30000/batchNum)))
	}
	for {
		time.Sleep(50 * time.Second)
	}

}
