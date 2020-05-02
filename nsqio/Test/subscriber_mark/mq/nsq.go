package mq

import (
	"bytes"
	"encoding/binary"
	"io/ioutil"
	"log"
	"strconv"
	"time"

	nsq "github.com/WU-CPSL/RTM-0.1/nsqio/go-nsq"
)

type MessageHandler interface {
	ReceiveMessage([]byte) bool
}

type LatencyMessageHandler struct {
	NumberOfMessages int
	Latencies        []float32
	Results          []byte
	messageCounter   int
	Channel          string
}

func (handler *LatencyMessageHandler) ReceiveMessage(message []byte) bool {
	now := time.Now().UnixNano()
	var then int64
	var ch string
	for i, value := range bytes.Split(message[24:29], []byte{'\n'}) {
		if i == 0 {
			ch = string(value)
		}
	}
	then, _ = binary.Varint(message[0:18])
	enter,_:=binary.Varint(message[40:58])
	exit,_:=binary.Varint(message[60:78])

	if ch != "0" {
		return false
	}

	if then != 0 {
		handler.Latencies = append(handler.Latencies, (float32(now-then))/1000/1000)
		if handler.Channel == "0" {
			x := strconv.FormatInt(enter-then, 10)
			handler.Results = append(handler.Results, x...)
			handler.Results = append(handler.Results, " "...)
			x = strconv.FormatInt(exit-enter, 10)
			handler.Results = append(handler.Results, x...)
			handler.Results = append(handler.Results, " "...)
			x = strconv.FormatInt(now-exit, 10)
			handler.Results = append(handler.Results, x...)
			handler.Results = append(handler.Results, "\n"...)
		}
	}
	handler.messageCounter++
	if handler.messageCounter == 13000 {
		sum := float32(0)
		for index, latency := range handler.Latencies {
			if index > 0 {
				sum += latency
			}

		}
		avgLatency := float32(sum) / float32(len(handler.Latencies)-1)
		log.Printf("Mean latency for %d messages: %f ms\n", handler.NumberOfMessages, avgLatency)
		if handler.Channel == "0" {
			ioutil.WriteFile("latency", handler.Results, 0777)
		}

	}

	return false
}

func NewNsq(numberOfMessages int, channeL string, lookupd string) {
	channel := channeL
	channel += "n#ephemeral"
	topic := channel
	config := nsq.NewConfig()
	config.MaxInFlight = 2000
	config.OutputBufferSize = -1
	config.OutputBufferTimeout = 0
	sub, _ := nsq.NewConsumer(topic, channel, config)
	var handler MessageHandler
	handler = &LatencyMessageHandler{
		NumberOfMessages: numberOfMessages,
		Latencies:        []float32{},
		Channel:          channeL,
	}

	sub.AddHandler(nsq.HandlerFunc(func(message *nsq.Message) error {
		handler.ReceiveMessage(message.Body)
		return nil
	}))

	//sub.ConnectToNSQD("192.168.1.11:4150")
	sub.ConnectToNSQLookupd(lookupd)
}
