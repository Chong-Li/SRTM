package mq

import (
	"bytes"
	"encoding/binary"
	"io/ioutil"
	"log"
	"strconv"
	"time"

	"github.com/WU-CPSL/RTM-0.1/nsqio/go-nsq"
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

	if ch != "0" {
		return false
	}

	if then != 0 {
		handler.Latencies = append(handler.Latencies, (float32(now-then))/1000/1000)
		if handler.Channel == "0" {
			x := strconv.FormatInt(now-then, 10)
			handler.Results = append(handler.Results, x...)
			handler.Results = append(handler.Results, "\n"...)
		}
	}
	handler.messageCounter++
	if handler.messageCounter == 13000 {
		sum := float32(0)
		for _, latency := range handler.Latencies {
			sum += latency

		}
		avgLatency := float32(sum) / float32(len(handler.Latencies))
		log.Printf("Mean latency for %d messages: %f ms\n", handler.NumberOfMessages,
			avgLatency)
		if handler.Channel == "0" {
			ioutil.WriteFile("latency", handler.Results, 0777)
		}

	}

	return false
}

func NewNsq(numberOfMessages int, channeL string, lookupd string, prio string) {
	channel := channeL
	channel += "n#ephemeral"
	topic := channel
	config := nsq.NewConfig()
	config.MaxInFlight = 2000
	if prio == "HIGH" {
		config.OutputBufferSize = -1
	} else {
		config.OutputBufferSize = 8196
	}
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
