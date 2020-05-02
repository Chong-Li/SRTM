package mq

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"strconv"
	"time"

	"github.com/WU-CPSL/RTM-0.1/nsqio/go-nsq"
)

type MessageHandler interface {
	ReceiveMessage([]byte) bool
}

type LatencyMessageHandler struct {
	id               string
	NumberOfMessages int
	latencyCounter   int
	Latencies        []int64
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

	if then != 0 && handler.latencyCounter < 13000 { //}&& handler.latencyCounter%10 == 0 {
		handler.Latencies[handler.latencyCounter] = now - then
		handler.latencyCounter++
		// if handler.latencyCounter%10 == 0 {
		// 	fmt.Println(handler.latencyCounter)
		// }
	}
	handler.messageCounter++
	if handler.messageCounter == 13000 {
		fmt.Println("totally ", handler.messageCounter, " messages received")
		sum := float32(0)
		for _, latency := range handler.Latencies {
			sum += float32(latency) / 1000 / 1000
			x := strconv.FormatInt(latency, 10)
			handler.Results = append(handler.Results, x...)
			handler.Results = append(handler.Results, "\n"...)
		}
		avgLatency := float32(sum) / float32(handler.latencyCounter)
		log.Printf("Mean latency for %d messages: %f ms\n", handler.latencyCounter,
			avgLatency)
		ioutil.WriteFile("latency_"+handler.id, handler.Results, 0777)
	}

	return false
}

func NewNsq(numberOfMessages int, id string, topic string, channel string, lookupd string, intf string, resendTimeout time.Duration, maxHardTimeout time.Duration) {
	topic += "n#ephemeral"
	config := nsq.NewConfig()
	config.MaxInFlight = 2000
	config.OutputBufferSize = -1
	config.ResendTimeout = resendTimeout
	config.MaxHardTimeout = maxHardTimeout
	config.Intf = intf
	sub, _ := nsq.NewConsumer(topic, channel, config)
	latencies := make([]int64, 13000)

	var handler MessageHandler
	handler = &LatencyMessageHandler{
		id:               id,
		latencyCounter:   0,
		NumberOfMessages: numberOfMessages,
		Latencies:        latencies,
		Channel:          channel,
	}

	sub.AddHandler(nsq.HandlerFunc(func(message *nsq.Message) error {
		handler.ReceiveMessage(message.Body)
		return nil
	}))
	sub.SetLogger(log.New(os.Stderr, "", log.LstdFlags), nsq.LogLevelInfo)

	//sub.ConnectToNSQD("192.168.1.11:4150")
	sub.ConnectToNSQLookupd(lookupd)
}
