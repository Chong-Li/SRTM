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
	Latency1         []float32
	Latency2         []float32
	Latency3         []float32
	Latency4         []float32
	prev             int64
	Lens             []int
	Results          []byte
	messageCounter   int
	last             int64
	step             int
	i                int
	Channel          string
}

func (handler *LatencyMessageHandler) ReceiveMessage(message []byte) bool {
	var then int64
	var len0 int
	var len1 int
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
	handler.step++
	if handler.step < 1 {
		return false
	}
	handler.step = 0
	now := time.Now().UnixNano()
	then1, _ := binary.Varint(message[40:58])
	then2, _ := binary.Varint(message[60:78])
	then3, _ := binary.Varint(message[80:98])
	then4, _ := binary.Varint(message[100:118])
	for i, value := range bytes.Split(message[30:], []byte{'\n'}) {
		if i == 0 {
			len0, _ = strconv.Atoi(string(value))
			break
		}
	}
	for i, value := range bytes.Split(message[120:], []byte{'\n'}) {
		if i == 0 {
			len1, _ = strconv.Atoi(string(value))
			break
		}
	}
	if then != 0 {
		handler.Latencies = append(handler.Latencies, (float32(now-then))/1000/1000)
		//		if handler.Channel == "0" {
		x := strconv.FormatInt(now-then, 10)
		handler.Results = append(handler.Results, x...)
		handler.Results = append(handler.Results, " "...)
		x = strconv.FormatInt(then1-handler.last, 10)
		handler.last = then1
		handler.Results = append(handler.Results, x...)
		handler.Results = append(handler.Results, " "...)
		handler.Results = append(handler.Results, strconv.Itoa(len0)...)
		handler.Results = append(handler.Results, " "...)
		x = strconv.FormatInt(then1-then, 10)
		handler.Results = append(handler.Results, x...)
		handler.Results = append(handler.Results, " "...)
		x = strconv.FormatInt(then2-then1, 10)
		handler.Results = append(handler.Results, x...)
		handler.Results = append(handler.Results, " "...)
		x = strconv.FormatInt(then3-then2, 10)
		handler.Results = append(handler.Results, x...)
		handler.Results = append(handler.Results, " "...)
		x = strconv.FormatInt(then4-then3, 10)
		handler.Results = append(handler.Results, x...)
		handler.Results = append(handler.Results, " "...)
		x = strconv.FormatInt(now-then4, 10)
		handler.Results = append(handler.Results, x...)
		handler.Results = append(handler.Results, " "...)
		handler.Results = append(handler.Results, strconv.Itoa(len1)...)
		handler.Results = append(handler.Results, " "...)
		x = strconv.FormatInt(then, 10)
		handler.Results = append(handler.Results, x...)
		handler.Results = append(handler.Results, " "...)
		handler.Results = append(handler.Results, "\n"...)
		//		}
	}
	handler.messageCounter++
	//log.Println(handler.messageCounter)
	//RTM.dtb
	if handler.messageCounter == 25000 {
		sum := float32(0)
		//lenSum := 0
		for _, latency := range handler.Latencies {
			sum += latency
			//lenSum+= handler.Lens[i]
		}
		avgLatency := float32(sum) / float32(len(handler.Latencies))
		//avgLen := lenSum/len(handler.Lens)
		log.Printf("Mean latency for %s messages: %f ms\n", handler.Channel, avgLatency)
		//log.Printf("Mean length is %d\n", avgLen)

		//		if handler.Channel == "0" {
		name := "latency" + handler.Channel + "i" + strconv.Itoa(handler.i)
		ioutil.WriteFile(name, handler.Results, 0777)
		//		}

	}

	return false
}

func NewNsq(numberOfMessages int, channeL string, index int, lookupd string) {
	topic := channeL + "n#ephemeral"
	channel := channeL + "n" + strconv.Itoa(index) + "#ephemeral"
	config := nsq.NewConfig()
	config.MaxInFlight = 20000000
	config.OutputBufferSize = 10000 * 162
	sub, _ := nsq.NewConsumer(topic, channel, config)
	var handler MessageHandler
	handler = &LatencyMessageHandler{
		NumberOfMessages: numberOfMessages,
		Latencies:        []float32{},
		Latency1:         []float32{},
		Latency2:         []float32{},
		Latency3:         []float32{},
		Latency4:         []float32{},
		prev:             0,
		Lens:             []int{},
		last:             0,
		step:             0,
		i:                index,
		Channel:          channeL,
	}

	sub.AddHandler(nsq.HandlerFunc(func(message *nsq.Message) error {
		handler.ReceiveMessage(message.Body)
		return nil
	}))

	//sub.ConnectToNSQD("192.168.1.11:4150")
	sub.ConnectToNSQLookupd(lookupd)
}
