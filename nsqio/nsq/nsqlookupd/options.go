package nsqlookupd

import (
	"log"
	"os"
	"time"
)

type Options struct {
	Verbose bool `flag:"verbose"`

	TCPAddress       string `flag:"tcp-address"`
	HTTPAddress      string `flag:"http-address"`
	BroadcastAddress string `flag:"broadcast-address"`

	InactiveProducerTimeout time.Duration `flag:"inactive-producer-timeout"`
	TombstoneLifetime       time.Duration `flag:"tombstone-lifetime"`

	//yao
	HighPriorityTopicFile string `flag:"high-priority-topic-file"`

	//jarry multicast address management
	MulticastFlag bool   `flag:"multicast_flag"`
	PortMin       int    `flag:"port_range_min"`
	PortMax       int    `flag:"port_range_max"`
	AddrMin       string `flag:"address_range_min"`
	AddrMax       string `flag:"address_range_max"`

	Logger logger
}

func NewOptions() *Options {
	hostname, err := os.Hostname()
	if err != nil {
		log.Fatal(err)
	}

	//RTM
	errorlog, err := os.OpenFile("logfile", os.O_RDWR|os.O_CREATE, 0666)

	return &Options{
		TCPAddress:       "0.0.0.0:4160",
		HTTPAddress:      "0.0.0.0:4161",
		BroadcastAddress: hostname,

		InactiveProducerTimeout: 300 * time.Second,
		TombstoneLifetime:       45 * time.Second,

		MulticastFlag: false,
		AddrMin:       "226.0.0.0",
		AddrMax:       "226.0.4.0",
		PortMin:       60000,
		PortMax:       61000,

		//RTM
		//Logger: log.New(os.Stderr, "[nsqlookupd] ", log.Ldate|log.Ltime|log.Lmicroseconds),
		Logger: log.New(errorlog, "[nsqlookupd] ", log.Ldate|log.Ltime|log.Lmicroseconds),

		HighPriorityTopicFile: "TOPICS",
	}
}
