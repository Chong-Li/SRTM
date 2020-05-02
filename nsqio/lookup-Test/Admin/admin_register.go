package main

import (
  "github.com/WU-CPSL/RTM-0.1/nsqio/go-nsq"
  "os"
)

func main() {
  config := nsq.NewConfig()
  w, _ := nsq.NewProducer("", config)
  _ = w.RegisterHighPriorityTopic("127.0.0.1:4161",os.Args[1])
}
