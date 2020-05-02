// +build rtm

package main

import (
	"encoding/binary"
	"math"
	"math/rand"
	"os"
	"runtime"
	"strconv"
	"time"
	//"fmt"

	"github.com/WU-CPSL/RTM-0.1/nsqio/Test/publisher_batch_poi/mq"
)

func ExpRnd(mean float64, random *rand.Rand) float64 {
	return -mean * math.Log(1-random.Float64())
}

var rnd_mean float64

func newTest(msgSize int, topic string, index int, repeat int, lookupd string, priority string) {
	nsq := mq.NewNsq(msgSize, topic, index, lookupd, priority)

	b := make([]byte, msgSize)
	strIndex:=[]byte(strconv.Itoa(index))
	strIndex=append(strIndex, "\n"...)
	i := 0
	//rnd_mean := float64(100000)
	random :=rand.New(rand.NewSource(time.Now().UnixNano()))
	var sleepTime time.Duration
	for {
		if i == 0 {
			binary.PutVarint(b[0:], time.Now().UnixNano())
			copy(b[24:],strIndex)
			nsq.Send(b)
			i++
			time.Sleep(10 * time.Second)
		}
		sleepTime = time.Duration(int64(ExpRnd(rnd_mean, random)))
		//sleepTime = time.Duration(rnd_mean)
		//if index == 0 {
			//fmt.Println(int(sleepTime))
		//}
		time.Sleep(sleepTime * time.Microsecond)
		for j:=0; j< repeat; j++ {
			binary.PutVarint(b[0:], time.Now().UnixNano())
			copy(b[24:],strIndex)
			nsq.Send(b)
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
	rnd, _:= strconv.Atoi(os.Args[3])
	rnd_mean = float64(rnd)
	repeat, _ := strconv.Atoi(os.Args[4])
	
	for i := 0; i < num; i++ {
		// We create num producers publishing num different topics,
		// To create num producers, all publishing the same topic, replace
		// strconv.Itoa(topic+i) to strconv.Itoa(topic)
		index := i
		if topic > 1 {
			index++
		}
		go newTest(150, strconv.Itoa(topic), index, repeat, os.Args[5], os.Args[6])
		time.Sleep(1000 * time.Microsecond)
	}
	for {
		time.Sleep(50 * time.Second)
	}

}
