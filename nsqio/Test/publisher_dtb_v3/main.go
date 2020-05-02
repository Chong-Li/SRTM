// +build rtm

package main

import (
	"encoding/binary"
	"os"
	"sync"
	"runtime"
	"strconv"
	"time"

	"github.com/WU-CPSL/RTM-0.1/nsqio/Test/publisher_dtb_v3/mq"
)

func newTest(msgSize int, topic string, index int, burst int, lookupd string, priority string) {
	nsq := mq.NewNsq(msgSize, topic, index, lookupd, priority)

	b := make([]byte, 24)
	id := make([]byte, 5)
	i := 0
	repeat :=1
	rwm :=&sync.RWMutex{}
	if burst > 0 {
		/*fire1 := time.NewTimer(time.Second * 30)
		fire2 := time.NewTimer(time.Second * 50)
		go func() {
			<-fire1.C
			repeat = 3
			<-fire2.C
			repeat = 1
		}()*/
		fire := time.NewTimer(time.Second * 30)
		go func() {
			<- fire.C
			for {
				rwm.Lock()
				repeat = 3
				rwm.Unlock()
				time.Sleep(2* time.Second)
				rwm.Lock()
				repeat = 1
				rwm.Unlock()
				time.Sleep(4* time.Second)
			}
		}()
	}

	ticker := time.NewTicker(time.Microsecond * 1000)
	repeat2:=repeat
	for _ = range ticker.C {
		if i == 1 {
			time.Sleep(3 * time.Second)
		}
		rwm.RLock()
		repeat2=repeat
		rwm.RUnlock()
		for j := 0; j < repeat2; j++ {
			binary.PutVarint(b, time.Now().UnixNano())
			binary.PutVarint(id, int64(i))
			//b=append(b, strconv.FormatInt(int64(i), 10)...)
			copy(b[19:23], id[:])
			nsq.Send(b)
			i++
		}
	}
	for {
		time.Sleep(50 * time.Second)
	}
}

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	index, _ := strconv.Atoi(os.Args[1])
	topic, _ := strconv.Atoi(os.Args[2])
	burst, _ := strconv.Atoi(os.Args[3])
	//for i := 0; i < num; i++ {
	go newTest(512, strconv.Itoa(topic), index, burst, os.Args[4], os.Args[5])
	//}
	for {
		time.Sleep(50 * time.Second)
	}

}
