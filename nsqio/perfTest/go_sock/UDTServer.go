package main
import "github.com/jbenet/go-udtwrapper/udt"
import "flag"
import "net"
import "fmt"
import "os"
import "time"
import "encoding/binary"
import "strconv"
import "bufio"

const (
	BUFSIZE     = 2048
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

	//flag.IntVar(&interval, "i", 5, "The interval of sending message in ms")
  flag.IntVar(&msglen, "l", 1000, "The message length")
  flag.IntVar(&cnum, "n", 2, "Number of concurrent client connections")
  //flag.StringVar(&ip, "a", "127.0.0.1", "The IP address of remote server")
  flag.StringVar(&port, "p", "8080", "The port number of remote server")
  flag.StringVar(&ctype, "t", "udt", "The connection type, TCP or UDP or UDT")
  flag.IntVar(&stopNum, "s", 400, "Number of message to send before stop")
  flag.IntVar(&recordlen, "rl", 200, "The number of latency to record")
	recordOrNot := flag.Bool("record", false, "Indicate whether to record the latency or not")
	flag.StringVar(&sfile, "f", "udt_server", "The file name for recording the latency")

	flag.Parse()
	serverAddr,err := udt.ResolveUDTAddr("udt",":"+port)
  CheckErrorExit("Resolve UDT Error", err)

	ln, err := udt.ListenUDT(ctype, serverAddr)
	CheckErrorExit("Listen Error", err)
	defer ln.Close()
	i := 0
	for {
		conn, err := ln.Accept()
		if err != nil {
			fmt.Println(err)
			continue
		}
		i ++
		fmt.Println("Accept from a client ", i)
		go handleConnection(i, conn, *recordOrNot)
	}

	time.Sleep(10 * time.Second)
	defer ln.Close()
}

func handleConnection(id int, c net.Conn, recordOrNot bool) {
	var oneWayLatencies []int
	if recordOrNot {
		oneWayLatencies = make([]int, recordlen)
	}
	buf := make([]byte, BUFSIZE)
	sendBuf := make([]byte, msglen)
	i := 0
	for {
		//receive message
		_, err := c.Read(buf)
		if err != nil {
			fmt.Println("Read Error:", err)
			c.Close()
			break
		}
		if recordOrNot && stopNum-i <= recordlen {
			currentTime := time.Now().UnixNano()
			sentTime, _ := binary.Varint(buf[8:])
			latency := currentTime - sentTime
			// latency in microseconds
			oneWayLatencies[recordlen-(stopNum-i)] = int(latency)
		}
		pktNum, _ := binary.Varint(buf)
		binary.PutVarint(sendBuf, pktNum)
		binary.PutVarint(sendBuf[8:], time.Now().UnixNano())
		c.Write(sendBuf)
		i ++
	}
	//ioutil.WriteFile("client_server",oneWayLatencies,0777)
	if recordOrNot {
		if err := writeLines(oneWayLatencies, sfile+strconv.Itoa(id)+".log"); err != nil {
			fmt.Println("WRITE FILE ERROR")
		}
	}
	fmt.Println("End Connection ", id, " ", i, " packets sent.")
}

func writeLines(lines []int, path string) error {
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
