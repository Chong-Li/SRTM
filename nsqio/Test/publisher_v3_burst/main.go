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
	"strings"
	//"math/rand"

	"github.com/WU-CPSL/RTM-0.1/nsqio/Test/publisher_v3_burst/mq"
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
	topic := "p"+os.Args[2]
	burst, _ := strconv.Atoi(os.Args[3])
	periodMicro, _ = strconv.Atoi(os.Args[4])
	repeat, _ = strconv.Atoi(os.Args[5])
	msgNum := 1100
	var wg sync.WaitGroup
	batchNum := num/burst
	m := make([]sync.Mutex, batchNum)
	c := make([]*sync.Cond, batchNum)
	for i:=0; i< batchNum; i++ {
		c[i] = sync.NewCond(&m[i])
	}

realrun:
	j:=0
	index :=0
	for i:=0; i < batchNum; i++ {
		for j =0; j< burst; j++ {
			time.Sleep(5000* time.Microsecond)
			wg.Add(1)
			if strings.HasPrefix(topic, "p"){
				go newTest(60001, c[i], &wg, index, 150, topic, index, msgNum, os.Args[6], os.Args[7])
			} else {
				go newTest(60001, c[i], &wg, 0, 150, topic, index, msgNum, os.Args[6], os.Args[7])
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
		time.Sleep(time.Microsecond*(time.Duration(1*periodMicro/batchNum)))
	}

	wg.Wait()
	time.Sleep(5 * time.Second)
	if msgNum > 1000000000 {
		for {
			time.Sleep(50 * time.Second)
		}
	}
        msgNum = 100000000000000
	topic = os.Args[2]
	goto realrun

}
