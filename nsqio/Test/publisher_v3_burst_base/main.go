// +build rtm

package main

import (
	"encoding/binary"
	//"log"
	"os"
	"runtime"
	"strconv"
	"time"
	"sync"
	//"math/rand"

	"github.com/WU-CPSL/RTM-0.1/nsqio/Test/publisher_v3_burst_base/mq"
)
var periodMicro int
var repeat int

func newTest(msgCount int, c *sync.Cond, wg *sync.WaitGroup,index_str int, msgSize int, topic string, index int, msgNum int, lookupd string, priority string) {
	nsq := mq.NewNsq(msgCount, msgSize, topic, index, lookupd, priority)
	b := make([]byte, msgSize)
	strIndex:=[]byte(strconv.Itoa(index_str))
	strIndex=append(strIndex, "\n"...)
	i:=1

	binary.PutVarint(b[0:], time.Now().UnixNano())
	copy(b[24:],strIndex)
	nsq.Send(b)
	i++
	c.L.Lock()
	c.Wait()
	c.L.Unlock()
	ticker :=time.NewTicker(time.Microsecond * time.Duration(periodMicro))
	for _= range ticker.C{
		if i >= msgNum {
			break
		}
		for j:=0; j< repeat; j++ {
			binary.PutVarint(b, time.Now().UnixNano())
			copy(b[24:], strIndex)
			nsq.Send(b)
			i++
		}
	}
	nsq.Teardown()
	wg.Done()
}

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	num, _ := strconv.Atoi(os.Args[1])
	//topic, _ := strconv.Atoi(os.Args[2])
	topic := os.Args[2]
	burst, _ := strconv.Atoi(os.Args[3])
	periodMicro, _ = strconv.Atoi(os.Args[4])
	repeat, _ = strconv.Atoi(os.Args[5])
	msgNum := 11000000000000
	var wg sync.WaitGroup
	batchNum := num/burst
	m := make([]sync.Mutex, batchNum)
	c := make([]*sync.Cond, batchNum)
	for i:=0; i< batchNum; i++ {
		c[i] = sync.NewCond(&m[i])
	}

	j:=0
	index :=0
	for i:=0; i < batchNum; i++ {
		for j =0; j< burst; j++ {
			time.Sleep(5000* time.Microsecond)
			wg.Add(1)
			go newTest(60001, c[i], &wg, 0, 150, topic, index, msgNum, os.Args[6], os.Args[7])
			index++
		}
	}
	
	time.Sleep(10* time.Second)
	for i:=0; i< batchNum; i++ {
		c[i].L.Lock()
		c[i].Broadcast()
		c[i].L.Unlock()
		//RTM.dtb
		time.Sleep(time.Microsecond*(time.Duration(1*periodMicro/batchNum)))
	}

	wg.Wait()

}
