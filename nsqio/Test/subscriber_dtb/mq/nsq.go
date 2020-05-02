package mq

import (
	"io/ioutil"
	"strconv"
	"bytes"
	"time"
	"sync"
	"encoding/binary"
	"github.com/WU-CPSL/RTM-0.1/nsqio/go-nsq"
)

type MessageHandler interface {
	ReceiveMessage([]byte) bool
	Setup()
	Increment()
}

type LatencyMessageHandler struct {
	thru	map[string][]int64
	time	int
	num     int
	label   string
	mutex   *sync.Mutex
}


func (handler *LatencyMessageHandler) ReceiveMessage(message []byte) bool {
	var pid string
	for i, value := range bytes.Split(message[24:29], []byte{'\n'}) {
		if i == 0 {
			pid = string(value)
		}
	}
	then,_ := binary.Varint(message[0:18])
	now:= time.Now().UnixNano()
	//handler.thru[pid][handler.time]=handler.thru[pid][handler.time]+1 
	handler.mutex.Lock()
	handler.thru[pid]=append(handler.thru[pid], now-then)
        handler.mutex.Unlock()

	return false
}

func (handler *LatencyMessageHandler) Setup() {
	handler.thru=make(map[string][]int64)
	for i:=0; i<handler.num; i++{
		handler.thru[strconv.Itoa(i)]=make([]int64, 1)
	}
}

func (handler *LatencyMessageHandler) Increment() {
		ticker:=time.NewTicker(time.Second)
		for _= range ticker.C{
			handler.time++
			if handler.time > 80{
				handler.mutex.Lock()
				var results []byte
				for j:=0; j<len(handler.thru[strconv.Itoa(0)]); j++{
					for i:=0; i<handler.num; i++ {
						if j < len(handler.thru[strconv.Itoa(i)]) {
							results=append(results, strconv.FormatInt(handler.thru[strconv.Itoa(i)][j],10)...)
							results=append(results, "  "...)
						} else {
							results=append(results, " 0  "...)
						}
					}
					results=append(results, "\n"...)
				}
				handler.mutex.Unlock()
				ioutil.WriteFile("dtb_test"+handler.label, results, 0777)
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
		time:		0,
		num:		sources,
		label:		channeL,
		mutex:		&sync.Mutex{},
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
