package main

import (
	"bufio"
	"encoding/binary"
	"flag"
	"fmt"
	"net"
	"os"
	"strconv"
	"time"

	"golang.org/x/net/ipv4"
)

//import "strconv"

const (
	BUFSIZE       = 10000
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
var maddr string
var intf string

/* A Simple function to verify error */
func CheckErrorExit(errStr string, err error) {
	if err != nil {
		fmt.Println(errStr+": ", err)
		os.Exit(1)
	}
}

/* A Simple function to verify error */
func CheckError(errStr string, err error) {
	if err != nil {
		fmt.Println(errStr+": ", err)
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
	flag.StringVar(&maddr, "m", "224.0.0.1", "Multicast Address")
	flag.StringVar(&intf, "I", "em1", "Name of the interface for multicast")
	flag.Parse()

	en0, err := net.InterfaceByName(intf)
	CheckErrorExit("Interface By Name Error", err)

	//en1, err := net.InterfaceByName("em2")
	//CheckErrorExit("Interface By Name Error", err)

	group := net.ParseIP(maddr)

	//an application listens to an appropriate address with an appropriate service port.
	c, err := net.ListenPacket("udp4", ":"+port)
	CheckErrorExit("Listen Packet Error", err)
	defer c.Close()
	// join group
	p := ipv4.NewPacketConn(c)
	err = p.JoinGroup(en0, &net.UDPAddr{IP: group})
	CheckErrorExit("Join Group Error", err)

	//err = p.JoinGroup(en1, &net.UDPAddr{IP: group})
	//CheckErrorExit("Join Group Error", err)

	// if the application need a dest address in the packet
	err = p.SetControlMessage(ipv4.FlagDst, true)
	CheckErrorExit("Set Control Message Error", err)

	// make a buffer
	b := make([]byte, msglen)
	intPort, err := strconv.Atoi(port)
	dst := &net.UDPAddr{IP: group, Port: intPort}

	//go handleAck(p)
	p.SetTOS(0x0)
	p.SetTTL(4)
	err = p.SetMulticastInterface(en0)
	CheckError("Set Multicast Interface Error", err)

	currentTime := time.Now().UnixNano()
	startTime := time.Now().UnixNano()
	for i := 1; i <= stopNum; i++ {
		binary.PutVarint(b, int64(i))
		currentTime = time.Now().UnixNano()
		binary.PutVarint(b[8:], currentTime)
		//The application can also send both unicast and multicast packets.
		_, err = p.WriteTo(b, nil, dst)
		CheckError("Write To multicast Error", err)
	}
	endTime := time.Now().UnixNano()
	time.Sleep(time.Microsecond * 100)
	binary.PutVarint(b, int64(-1))
	//currentTime = time.Now().UnixNano()
	//binary.PutVarint(sendBuf[8:], currentTime)
	//c.WriteToUDP(sendBuf, addr)
	//The application can also send both unicast and multicast packets.
	_, err = p.WriteTo(b, nil, dst)
	CheckError("Write To multicast Error", err)

	fmt.Println((endTime-startTime)/1000/1000, "ms passed")
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
