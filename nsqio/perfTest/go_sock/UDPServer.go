package main

import "flag"
import "net"
import "fmt"
import "os"
import "time"
import "encoding/binary"
import "bufio"

//import "strconv"
import "sync"

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

func main() {

	flag.IntVar(&interval, "i", 0, "The interval of sending message in us")
	flag.IntVar(&msglen, "l", 1000, "The message length")
	flag.IntVar(&cnum, "n", 1, "Number of concurrent client connections")
	//flag.StringVar(&ip, "a", "127.0.0.1", "The IP address of remote server")
	flag.StringVar(&port, "p", "8080", "The port number of remote server")
	flag.StringVar(&ctype, "t", "udp", "The connection type, TCP or UDP or UDT")
	flag.IntVar(&stopNum, "s", 5000000, "Number of message to send before stop")
	flag.IntVar(&recordlen, "rl", 10000, "The number of latency to record")
	//recordOrNot := flag.Bool("record", false, "Indicate whether to record the latency or not")
	flag.StringVar(&sfile, "f", "tcp_server", "The file name for recording the latency")
	var rnum int
	var wg sync.WaitGroup

	flag.IntVar(&rnum, "rn", 1, "Number of Go Routines to process UDP packets")

	flag.Parse()
	serverAddr, err := net.ResolveUDPAddr("udp", ":"+port)
	CheckErrorExit("Resolve UDP Error", err)

	ln, err := net.ListenUDP(ctype, serverAddr)
	CheckErrorExit("Listen Error", err)
	defer ln.Close()
	for m := 0; m < rnum; m++ {
		wg.Add(1)
		go handleUDP(&wg, ln)
	}
	wg.Wait()
}

func handleUDP(wg *sync.WaitGroup, c *net.UDPConn) {
	buf := make([]byte, BUFSIZE)
	sendBuf := make([]byte, msglen)

	_, addr, err := c.ReadFromUDP(buf)
	if err != nil {
		fmt.Println("ReadFromUDP Error: ", err)
	}
	beginTime := time.Now().UnixNano()
	rateInterval := 1
	if interval != 0 {
		rateInterval = interval
	}
	rate := time.Microsecond * time.Duration(rateInterval)
	throttle := time.Tick(rate)

	intervalgap := stopNum / INTERVAL_SIZE / 2
	readInterval := make([]int64, INTERVAL_SIZE)
	readNum := 0
	currentTime := time.Now().UnixNano()
	lastTime := currentTime

	c.SetWriteBuffer(4 * 1024 * 1024) // setup write buffer to be 40MB
	for i := 0; i < stopNum; i++ {
		if interval != 0 {
			<-throttle
		}
		binary.PutVarint(sendBuf, int64(i))
		lastTime = currentTime
		currentTime = time.Now().UnixNano()
		binary.PutVarint(sendBuf[8:], currentTime)
		c.WriteToUDP(sendBuf, addr)
		//time.Sleep(time.Microsecond*time.Duration(interval))
		if i > stopNum/4 && i < stopNum*3/4 && i%intervalgap == 0 && readNum < INTERVAL_SIZE {
			readInterval[readNum] = currentTime - lastTime
			readNum++
		}
	}
	endTime := time.Now().UnixNano()
	time.Sleep(time.Microsecond * 100)
	binary.PutVarint(sendBuf, int64(-1))
	binary.PutVarint(sendBuf[8:], time.Now().UnixNano())
	c.WriteToUDP(sendBuf, addr)
	writeLines(readInterval, "interval.log")
	wg.Done()
	fmt.Println(endTime-beginTime, " nanoseconds passed")
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
