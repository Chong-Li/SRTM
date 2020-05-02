// +build rtm

package main

import (
	"encoding/binary"
	"os"
	"sync"
	"runtime"
	"strconv"
	"time"

	"github.com/WU-CPSL/RTM-0.1/nsqio/Test/publisher_dump_v3/mq"
)

func newTest(msgSize int, topic string, index int, repeat int, lookupd string, priority string) {
	nsq := mq.NewNsq(msgSize, topic, index, lookupd, priority)

	b := make([]byte, 24)
	id := make([]byte, 5)
	i := 0
	rwm := &sync.RWMutex{}
	go func(){
		for {
			rwm.Lock()
			repeat = 300
			rwm.Unlock()
			time.Sleep(2* time.Second)
			rwm.Lock()
			repeat = 300
			rwm.Unlock()
			time.Sleep(2* time.Second)
		}
	}()
	repeat2:=repeat
	ticker := time.NewTicker(time.Millisecond * 100)
	for _ = range ticker.C {
		if i == 1 {
			time.Sleep(1 * time.Second)
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
	num, _ := strconv.Atoi(os.Args[1])
	topic, _ := strconv.Atoi(os.Args[2])
	repeat, _ := strconv.Atoi(os.Args[3])
	for i := 0; i < num; i++ {
		// We create num producers publishing num different topics,
		// To create num producers, all publishing the same topic, replace
		// strconv.Itoa(topic+i) to strconv.Itoa(topic)
		go newTest(512, strconv.Itoa(topic), i, repeat, os.Args[4], os.Args[5])
	}
	for {
		time.Sleep(50 * time.Second)
	}

}
