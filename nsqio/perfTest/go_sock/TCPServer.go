package main

import "flag"
import "net"
import "fmt"
import "os"
import "time"
import "encoding/binary"
import "bufio"
import "sync"

//import "strconv"

const (
	BUFSIZE       = 2048
	INTERVAL_SIZE = 10000
)

var recordlen int
var ctype string
var ip string
var port string
var stopNum int
var msglen int
var interval int
var cnum int
var sfile string

/* A Simple function to verify error */
func CheckErrorExit(errStr string, err error) {
	if err != nil {
		fmt.Println(errStr+": ", err)
		os.Exit(1)
	}
}

var wg sync.WaitGroup

func main() {

	flag.IntVar(&interval, "i", 0, "The interval of sending message in us")
	flag.IntVar(&msglen, "l", 1000, "The message length")
	flag.IntVar(&cnum, "n", 1, "Number of concurrent client connections")
	//flag.StringVar(&ip, "a", "127.0.0.1", "The IP address of remote server")
	flag.StringVar(&port, "p", "8080", "The port number of remote server")
	flag.StringVar(&ctype, "t", "tcp", "The connection type, TCP or UDP or UDT")
	flag.IntVar(&stopNum, "s", 5000000, "Number of message to send before stop")
	flag.IntVar(&recordlen, "rl", 10000, "The number of latency to record")
	recordOrNot := flag.Bool("record", false, "Indicate whether to record the latency or not")
	flag.StringVar(&sfile, "f", "tcp_server", "The file name for recording the latency")

	flag.Parse()

	ln, err := net.Listen(ctype, ":"+port)
	CheckErrorExit("Listen Error", err)
	for i := 0; i < cnum; i++ {
		conn, err := ln.Accept()
		if err != nil {
			fmt.Println(err)
			continue
		}
		//fmt.Println("Accept from a client ", i)
		wg.Add(1)
		go handleTCP(i, conn, *recordOrNot)
	}
	defer ln.Close()
	wg.Wait()
}

func handleTCP(id int, c net.Conn, recordOrNot bool) {
	sendBuf := make([]byte, msglen)
	intervalgap := stopNum / INTERVAL_SIZE / 2
	readInterval := make([]int64, INTERVAL_SIZE)
	readNum := 0

	beginTime := time.Now().UnixNano()
	rateInterval := 1
	if interval != 0 {
		rateInterval = interval
	}
	rate := time.Microsecond * time.Duration(rateInterval)
	throttle := time.Tick(rate)
	currentTime := time.Now().UnixNano()
	lastTime := currentTime
	for i := 0; i < stopNum; i++ {
		if interval != 0 {
			<-throttle
		}
		binary.PutVarint(sendBuf, int64(i))
		lastTime = currentTime
		currentTime = time.Now().UnixNano()
		binary.PutVarint(sendBuf[8:], currentTime)
		c.Write(sendBuf)
		if i > stopNum/4 && i < stopNum*3/4 && i%intervalgap == 0 && readNum < INTERVAL_SIZE {
			readInterval[readNum] = currentTime - lastTime
			readNum++
		}
	}
	endTime := time.Now().UnixNano()
	fmt.Println(endTime-beginTime, " nanoseconds passed")
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
