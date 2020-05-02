package mq

import (
	"io/ioutil"
	"strconv"
	"bytes"
	"time"
	"fmt"
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
}


func (handler *LatencyMessageHandler) ReceiveMessage(message []byte) bool {
	var pid string
	if handler.label != "0" {
		return false
	}
	for i, value := range bytes.Split(message[24:29], []byte{'\n'}) {
		if i == 0 {
			pid = string(value)
		}
	}
	//RTM.dtb test only total rate
	pid = "0"
	handler.thru[pid][handler.time]=handler.thru[pid][handler.time]+1 

	return false
}

func (handler *LatencyMessageHandler) Setup() {
	handler.thru=make(map[string][]int64)
	for i:=0; i<handler.num; i++{
		handler.thru[strconv.Itoa(i)]=make([]int64, 300)
	}
}

func (handler *LatencyMessageHandler) Increment() {
		ticker:=time.NewTicker(time.Second)
		for _= range ticker.C{
			handler.time++
			if handler.time > 100 && handler.label == "0" {
				var results []byte
				for j:=0; j<100; j++{
					for i:=0; i<handler.num; i++ {
						results=append(results, strconv.FormatInt(handler.thru[strconv.Itoa(i)][j],10)...)
						results=append(results, "  "...)
					}
					results=append(results, "\n"...)
				}
				ioutil.WriteFile("func_test"+handler.label, results, 0777)
				fmt.Println("func output")
				break
			}
		}
}

func NewNsq(msgSize int, channeL string, sources int, lookupd string) {
	channel := channeL
	channel += "n#ephemeral"
	topic := channel
	config := nsq.NewConfig()
	config.MaxInFlight = 20000000
	config.OutputBufferSize = -1 
	sub, _ := nsq.NewConsumer(topic, channel, config)

	var handler MessageHandler
	handler = &LatencyMessageHandler{
		time:		0,
		num:		sources,
		label:		channeL,
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