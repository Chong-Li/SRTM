package mq

import (
	"strconv"
	"github.com/WU-CPSL/RTM-0.1/nsqio/go-nsq"
)

type Nsq struct {
	pub       *nsq.Producer
	msgSize   int
	topic     string
	index string
}

func NewNsq(msgSize int, topic_raw string, index int, lookupd string, priority string) *Nsq {
	topic := topic_raw
	topic += "m#ephemeral"	
	//pub, _ := nsq.NewProducer("localhost:4150", nsq.NewConfig())
	pub, _ := nsq.NewProducer("",nsq.NewConfig())
	_ = pub.ConnectToNSQLookupd_v2(lookupd, priority) //priority="HIGH" or "LOW"

	return &Nsq{
		pub:       pub,
		msgSize:   msgSize,
		topic:     topic,
		index: strconv.Itoa(index),
	}
}

func (n *Nsq) Teardown() {
	n.pub.Stop()
}

func (n *Nsq) Send(message []byte) {
	message = append(message, n.index...)
	message = append(message, "\n"...)
	b := make([]byte, n.msgSize-len(message))
	message = append(message, b...)
	n.pub.PublishAsync(n.topic, message, nil)
}
