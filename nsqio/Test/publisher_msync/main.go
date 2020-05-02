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
	//"math/rand"

	"github.com/WU-CPSL/RTM-0.1/nsqio/Test/publisher_msync/mq"
)
var periodMilli int
var repeat int

func newTest(msgCount int, c *sync.Cond, msgSize int, topic string, index int, gap int, lookupd string, priority string) {
	nsq := mq.NewNsq(msgCount, msgSize, topic, index, lookupd, priority)
	start := time.Now().UnixNano()
	b := make([]byte, msgSize)
	strIndex:=[]byte(strconv.Itoa(index))
	strIndex=append(strIndex, "\n"...)
	i:=1

	//repeat:=1
	binary.PutVarint(b[0:], time.Now().UnixNano())
	copy(b[24:],strIndex)
	nsq.Send(b)
	i++
	c.L.Lock()
	c.Wait()
	c.L.Unlock()
	ticker :=time.NewTicker(time.Microsecond * time.Duration(periodMilli))
	for _= range ticker.C{
		for j:=0; j< repeat; j++ {
			//binary.PutVarint(b, time.Now().UnixNano())
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
	periodMilli, _ = strconv.Atoi(os.Args[4])
	repeat, _ = strconv.Atoi(os.Args[5])
	batchNum := num
	m := make([]sync.Mutex, batchNum)
	c := make([]*sync.Cond, batchNum)
	for i:=0; i< batchNum; i++ {
		c[i] = sync.NewCond(&m[i])
	}
	j:=0
	index :=0
	for i:=0; i < batchNum; i++ {
		for j =0; j< burst; j++ {
			time.Sleep(100000* time.Microsecond)
			if i == 0 {
				go newTest(60001, c[0], 150, strconv.Itoa(topic), j, burst, os.Args[6], os.Args[7])
			} else {
				go newTest(60001, c[0], 150, strconv.Itoa(topic+i), j+1, burst, os.Args[6], os.Args[7])
			
			}
			index++
		}
	}
	
	time.Sleep(10* time.Second)
	for i:=0; i< batchNum; i++ {
		c[i].L.Lock()
		c[i].Broadcast()
		c[i].L.Unlock()
		//RTM.dtb
		time.Sleep(time.Microsecond*(time.Duration(1*periodMilli/batchNum)))
	}
	for {
		time.Sleep(50 * time.Second)
	}

}
