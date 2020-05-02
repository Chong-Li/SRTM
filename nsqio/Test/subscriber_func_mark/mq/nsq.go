package mq

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io/ioutil"
	"strconv"
	"time"

	"github.com/WU-CPSL/RTM-0.1/nsqio/go-nsq"
)

type MessageHandler interface {
	ReceiveMessage([]byte) bool
	Setup()
	Increment()
}

type LatencyMessageHandler struct {
	thru  map[string][]int64
	begin map[string][]int64
	index map[string]int
	time  int
	num   int
	label string
}

func (handler *LatencyMessageHandler) ReceiveMessage(message []byte) bool {
	var pid string
	for i, value := range bytes.Split(message[24:29], []byte{'\n'}) {
		if i == 0 {
			pid = string(value)
		}
	}
	send_time, _ := binary.Varint(message[90:108])
	if handler.begin[pid][0] == 0 {
		handler.begin[pid][0] = send_time
	} else if send_time-handler.begin[pid][handler.index[pid]] >= 1000000000 {
		handler.index[pid] += 1
		handler.begin[pid][handler.index[pid]] = send_time
	}
	if bytes.HasPrefix(message[50:], []byte("congest_sent")) {
		congest_time, _ := binary.Varint(message[70:88])
		var i int
		var t int64
		for i, t = range handler.begin[pid] {
			if t > congest_time {
				break
			}
		}
		fmt.Printf("send congest at time %d\n", i)
	}
	if bytes.HasPrefix(message[50:], []byte("decongest_sent")) {
		/*decongest_time, _ := binary.Varint(message[70:88])
		var i int
		var t int64
		for i, t = range handler.begin[pid] {
			if t > decongest_time {
				break
			}
		}*/
		fmt.Printf("send decongest at time %d\n", handler.index[pid])
	}
	handler.thru[pid][handler.index[pid]] += 1

	return false
}

func (handler *LatencyMessageHandler) Setup() {
	handler.thru = make(map[string][]int64)
	handler.begin = make(map[string][]int64)
	handler.index = make(map[string]int)
	for i := 0; i < handler.num; i++ {
		handler.thru[strconv.Itoa(i)] = make([]int64, 300)
		handler.begin[strconv.Itoa(i)] = make([]int64, 300)
		handler.index[strconv.Itoa(i)] = 0
	}
}

func (handler *LatencyMessageHandler) Increment() {
	ticker := time.NewTicker(time.Second)
	for _ = range ticker.C {
		handler.time++
		if handler.time > 100 {
			var results []byte
			for j := 0; j < 100; j++ {
				for i := 0; i < handler.num; i++ {
					results = append(results, strconv.FormatInt(handler.thru[strconv.Itoa(i)][j], 10)...)
					results = append(results, "  "...)
				}
				results = append(results, "\n"...)
			}
			ioutil.WriteFile("func_test"+handler.label, results, 0777)
			break
		}
	}
}

func NewNsq(msgSize int, channeL string, sources int, lookupd string) {
	channel := channeL
	channel += "m#ephemeral"
	topic := channel
	config := nsq.NewConfig()
	config.MaxInFlight = 2000
	config.OutputBufferSize = -1
	sub, _ := nsq.NewConsumer(topic, channel, config)

	var handler MessageHandler
	handler = &LatencyMessageHandler{
		time:  0,
		num:   sources,
		label: channeL,
	}
	handler.Setup()
	go handler.Increment()

	sub.AddHandler(nsq.HandlerFunc(func(message *nsq.Message) error {
		handler.ReceiveMessage(message.Body)
		return nil
	}))

	//sub.ConnectToNSQD("192.168.1.11:4150")
	sub.ConnectToNSQLookupd(lookupd)

}
