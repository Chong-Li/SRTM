package main

import (
	"bufio"
	"encoding/binary"
	"flag"
	"fmt"
	"net"
	"os"
	"sync"
	"time"
)

const (
	INTERVAL_SIZE = 10000
	LATENCY_SIZE  = 10000
)

//import "strconv"
//import "io/ioutil"

var recordlen int
var ctype string
var ip string
var port string
var stopNum int
var msglen int
var interval int
var cnum int
var oneFile string
var roundFile string

/* A Simple function to verify error */
func CheckErrorExit(errString string, err error) {
	if err != nil {
		fmt.Println(errString+": ", err)
		os.Exit(1)
	}
}

func handleTCP(wg *sync.WaitGroup, id int, recordOrNot bool) {
	var conn net.Conn
	var err error
	conn, err = net.Dial(ctype, ip+":"+port)
	CheckErrorExit("TCP Connection Error", err)
	buf := make([]byte, msglen)
	latencygap := stopNum / LATENCY_SIZE / 2
	intervalgap := stopNum / INTERVAL_SIZE / 2
	oneWayLatencies := make([]int64, LATENCY_SIZE)
	readInterval := make([]int64, INTERVAL_SIZE)
	//time.Sleep(time.Microsecond * time.Duration(interval*rand.Intn(1000)))
	startTime := time.Now().UnixNano()
	currentTime := time.Now().UnixNano()
	lastTime := currentTime
	i := 0
	latencyNum := 0
	readNum := 0
	for i = 0; i < stopNum; i++ {
		n, err := conn.Read(buf)
		lastTime = currentTime
		currentTime = time.Now().UnixNano()
		if err != nil {
			fmt.Println("Read Error:", err)
			conn.Close()
			break
		}
		for n < msglen && n != 0 {
			m, err := conn.Read(buf[n:])
			if err != nil {
				fmt.Println("Read Error:", err)
				conn.Close()
				break
			}
			n += m
		}
		if n == 0 {
			fmt.Println("recieve 0")
		}
		//sentNum, _ := binary.Varint(buf)
		serverSentTime, _ := binary.Varint(buf[8:])
		if i > stopNum/4 && i < stopNum*3/4 && i%latencygap == 0 && latencyNum < LATENCY_SIZE {
			oneWayLatencies[latencyNum] = currentTime - serverSentTime
			latencyNum++
		}
		if i > stopNum/4 && i < stopNum*3/4 && i%intervalgap == 0 && readNum < INTERVAL_SIZE {
			readInterval[readNum] = currentTime - lastTime
			readNum++
		}
	}
	endTime := time.Now().UnixNano()
	fmt.Println(endTime-startTime, " ns passed")
	fmt.Println(i, "packets received")
	writeLines(oneWayLatencies, "latency.log")
	writeLines(readInterval, "interval.log")
	wg.Done()
}

func writeLines(lines []int64, path string) error {
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()

	w := bufio.NewWriter(file)
	for _, line := range lines {
		fmt.Fprintln(w, line)
	}
	return w.Flush()
}

func main() {

	flag.IntVar(&interval, "i", 0, "The interval of sending message in ms")
	flag.IntVar(&msglen, "l", 1000, "The message length")
	flag.IntVar(&cnum, "n", 1, "Number of concurrent client connections")
	flag.StringVar(&ip, "a", "127.0.0.1", "The IP address of remote server")
	flag.StringVar(&port, "p", "8080", "The port number of remote server")
	flag.StringVar(&ctype, "t", "tcp", "The connection type, TCP or UDP or UDT")
	flag.IntVar(&stopNum, "s", 5000000, "Number of message to send before stop")
	flag.IntVar(&recordlen, "rl", 10000, "The number of latency to record")
	flag.StringVar(&oneFile, "of", "client_oneWay", "The file name for one way latency")
	flag.StringVar(&roundFile, "rf", "client_roundtrip", "The file name for round trip latancy")
	flag.Parse()

	var wg sync.WaitGroup
	for i := 1; i <= cnum; i++ {
		wg.Add(1)
		go handleTCP(&wg, i, false)
	}
	wg.Wait()
	//fmt.Println("END Client Program")
}
