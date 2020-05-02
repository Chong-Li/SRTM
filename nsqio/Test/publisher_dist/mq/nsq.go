package mq

import (
	//"strconv"
	"github.com/WU-CPSL/RTM-0.1/nsqio/go-nsq"
	//"encoding/binary"
	//"fmt"
)

type Nsq struct {
	pub       *nsq.Producer
	msgCount  int
	msgSize   int
	topic     string
	topic_raw string
}

func NewNsq(msgCount int, msgSize int, topic_raw string, lookupd string, priority string) *Nsq {
	topic := topic_raw
	topic += "n#ephemeral"

	pub, _ := nsq.NewProducer(lookupd, nsq.NewConfig())
	//pub, _ := nsq.NewProducer("",nsq.NewConfig())
	//_ = pub.ConnectToNSQLookupd_v2(lookupd,priority)	


	return &Nsq{
		pub:       pub,
		msgCount:  msgCount,
		msgSize:   msgSize,
		topic:     topic,
		topic_raw: topic_raw,
	}
}

func (n *Nsq) Teardown() {
	n.pub.Stop()
}

func (n *Nsq) Send(message []byte) {
	message = append(message, n.topic_raw...)
	message = append(message, "\n"...)
	b := make([]byte, n.msgSize-len(message))
	message = append(message, b...)
	n.pub.PublishAsync(n.topic, message, nil)
}
