package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/WU-CPSL/RTM-0.1/nsqio/nsq/internal/version"
	"github.com/WU-CPSL/RTM-0.1/nsqio/nsq/nsqlookupd"
	"github.com/judwhite/go-svc/svc"
	"github.com/mreiferson/go-options"
)

var (
	flagSet = flag.NewFlagSet("nsqlookupd", flag.ExitOnError)

	config      = flagSet.String("config", "", "path to config file")
	showVersion = flagSet.Bool("version", false, "print version string")
	verbose     = flagSet.Bool("verbose", false, "enable verbose logging")

	tcpAddress       = flagSet.String("tcp-address", "0.0.0.0:4160", "<addr>:<port> to listen on for TCP clients")
	httpAddress      = flagSet.String("http-address", "0.0.0.0:4161", "<addr>:<port> to listen on for HTTP clients")
	broadcastAddress = flagSet.String("broadcast-address", "", "address of this lookupd node, (default to the OS hostname)")

	inactiveProducerTimeout = flagSet.Duration("inactive-producer-timeout", 300*time.Second, "duration of time a producer will remain in the active list since its last ping")
	tombstoneLifetime       = flagSet.Duration("tombstone-lifetime", 45*time.Second, "duration of time a producer will remain tombstoned if registration remains")

	//yao
	HighPriorityTopicFile = flagSet.String("high-priority-topic-file", "TOPICS", "High Priority Topic File Name")

	//jarry
	multicastFlag = flagSet.Bool("multicast_flag", false, "enable multicast")
	addrMin       = flagSet.String("address_range_min", "226.0.0.0", "multicast address range lower bound")
	addrMax       = flagSet.String("address_range_max", "226.0.4.0", "multicast address range higher bound")
	portMin       = flagSet.String("port_range_min", "60000", "multicast port range lower bound")
	portMax       = flagSet.String("port_range_max", "61000", "multicast port range higher bound")
)

type program struct {
	nsqlookupd *nsqlookupd.NSQLookupd
}

func main() {
	prg := &program{}
	if err := svc.Run(prg); err != nil {
		log.Fatal(err)
	}
}

func (p *program) Init(env svc.Environment) error {
	if env.IsWindowsService() {
		dir := filepath.Dir(os.Args[0])
		return os.Chdir(dir)
	}
	return nil
}

func (p *program) Start() error {
	flagSet.Parse(os.Args[1:])

	if *showVersion {
		fmt.Println(version.String("nsqlookupd"))
		os.Exit(0)
	}

	var cfg map[string]interface{}
	if *config != "" {
		_, err := toml.DecodeFile(*config, &cfg)
		if err != nil {
			log.Fatalf("ERROR: failed to load config file %s - %s", *config, err.Error())
		}
	}

	opts := nsqlookupd.NewOptions()
	options.Resolve(opts, flagSet, cfg)
	daemon := nsqlookupd.New(opts)

	daemon.Main()
	p.nsqlookupd = daemon
	return nil
}

func (p *program) Stop() error {
	if p.nsqlookupd != nil {
		p.nsqlookupd.Exit()
	}
	return nil
}
