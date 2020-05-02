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
	//"strings"
	"math/rand"
	"math"

	"github.com/WU-CPSL/RTM-0.1/nsqio/Test/publisher_v3_bpi_base/mq"
)
var repeat int
func ExpRnd(mean float64, random *rand.Rand) float64 {
        return -mean * math.Log(1-random.Float64())
}

var rnd_mean float64

func newTest(msgCount int, wg *sync.WaitGroup,index_str int, msgSize int, topic string, index int, msgNum int, lookupd string, priority string) {
	nsq := mq.NewNsq(msgCount, msgSize, topic, index, lookupd, priority)
	b := make([]byte, msgSize)
	strIndex:=[]byte(strconv.Itoa(index_str))
	strIndex=append(strIndex, "\n"...)
	i:=1

	binary.PutVarint(b[0:], time.Now().UnixNano())
	copy(b[24:],strIndex)
	nsq.Send(b)
	i++
	time.Sleep(20* time.Second)
	random :=rand.New(rand.NewSource(time.Now().UnixNano()))
        var sleepTime time.Duration
	sleepTime = time.Duration(int64(ExpRnd(rnd_mean, random)))
	time.Sleep(sleepTime * time.Microsecond)
	for {
		if i >= msgNum {
			break
		}
		sleepTime = time.Duration(int64(ExpRnd(rnd_mean, random)))
        	time.Sleep(sleepTime * time.Microsecond)
	
		for j:=0; j< repeat; j++ {
			binary.PutVarint(b, time.Now().UnixNano())
			copy(b[24:], strIndex)
			nsq.Send(b)
			i++
		}
	}
	time.Sleep(5* time.Second)
	nsq.Teardown()
	wg.Done()
}

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	num, _ := strconv.Atoi(os.Args[1])
	//topic, _ := strconv.Atoi(os.Args[2])
	topic := os.Args[2]
	rnd, _:= strconv.Atoi(os.Args[3])
        rnd_mean = float64(rnd)
        repeat, _ = strconv.Atoi(os.Args[4])
	jump, _ :=strconv.Atoi(os.Args[5])
	msgNum := 1000000000
	var wg sync.WaitGroup

	for index:=0; index < num; index++ {
		time.Sleep(3000* time.Microsecond)
		wg.Add(1)
		index_str :=index
		if index%jump == 0 {
			index_str = 0
		}
		go newTest(60001, &wg, index_str, 150, topic, index, msgNum, os.Args[6], os.Args[7])
	}

	wg.Wait()
}
