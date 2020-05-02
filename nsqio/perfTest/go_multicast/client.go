package main

import (
	"bufio"
	"encoding/binary"
	"flag"
	"fmt"
	"net"
	"os"
	"time"

	"golang.org/x/net/ipv4"
)

const (
	INTERVAL_SIZE = 1000
)

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
var pktLost int
var maddr string
var intf string // name of interface

/* A Simple function to verify error */
func CheckErrorExit(errString string, err error) {
	if err != nil {
		fmt.Println(errString+": ", err)
		os.Exit(1)
	}
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

	flag.IntVar(&interval, "i", 5, "The interval of sending message in ms")
	flag.IntVar(&msglen, "l", 1000, "The message length")
	flag.IntVar(&cnum, "n", 1, "Number of concurrent client connections")
	flag.StringVar(&ip, "a", "127.0.0.1", "The IP address of remote server")
	flag.StringVar(&port, "p", "8080", "The port number of remote server")
	flag.StringVar(&ctype, "t", "udp", "The connection type, TCP or UDP or UDT")
	flag.IntVar(&stopNum, "s", 5000000, "Number of message to send before stop")
	flag.IntVar(&recordlen, "rl", 10000, "The number of latency to record")
	flag.StringVar(&oneFile, "of", "udp_oneway", "The file name for one way latency")
	flag.StringVar(&roundFile, "rf", "udp_roundtrip", "The file name for round trip latancy")
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

	p.SetTOS(0x0)
	p.SetTTL(16)

	startTime := time.Now().UnixNano()
	//lostPkt := 0
	rcvPkt := 0

	//c.SetReadBuffer(4 * 1024 * 1024) // setup the read buffer as 10MB.
	//c.SetWriteBuffer(4 * 1024 * 1024)
	latencygap := stopNum / recordlen / 2
	//intervalgap := stopNum / INTERVAL_SIZE / 2
	oneWayLatencies := make([]int64, recordlen)

	latencyNum := 0
	currentTime := time.Now().UnixNano()

	for i := 1; i <= stopNum; i++ {
		_, cm, _, err := p.ReadFrom(b)
		CheckErrorExit("ReadFrom socket error", err)
		currentTime = time.Now().UnixNano()
		if !cm.Dst.IsMulticast() || !cm.Dst.Equal(group) {
			continue
		}
		sentNum, _ := binary.Varint(b)
		serverSentTime, _ := binary.Varint(b[8:])

		if sentNum == int64(-1) {
			break
		}
		if rcvPkt >= stopNum/4 && rcvPkt%latencygap == 0 && latencyNum < recordlen {
			oneWayLatencies[latencyNum] = currentTime - serverSentTime
			latencyNum++
		}
		rcvPkt++

		//The application can also send both unicast and multicast packets.
		//_, err = p.WriteTo(b, nil, src)
		//CheckErrorExit("Write to socket Error", err)
		// dst := &net.UDPAddr{IP: group, Port: 1024}
		// for _, ifi := range []*net.Interface{en0} {
		// 	if err := p.SetMulticastInterface(ifi); err != nil {
		// 		// error handling
		// 	}
		// 	p.SetMulticastTTL(2)
		// 	if _, err := p.WriteTo(b, nil, dst); err != nil {
		// 		// error handling
		// 	}
		// }
	}
	//wg.Wait()
	endTime := time.Now().UnixNano()
	fmt.Println("END Client Program, ", stopNum-rcvPkt, " packets lost")
	fmt.Println((endTime-startTime)/1000/1000, "ms passed")
	fmt.Println(rcvPkt, "packets received")
	//writeLines(lostPacket, "lostpkt.log")
	writeLines(oneWayLatencies, "latency.log")
}
