package main

//import "github.com/jbenet/go-udtwrapper/udt"
import (
	"bufio"
	"encoding/binary"
	"flag"
	"fmt"
	"math/rand"
	"net"
	"os"
	"strconv"
	"sync"
	"time"

	"golang.org/x/net/ipv4"
)

//import "strconv"
//import "github.com/jbenet/go-udtwrapper/udt"

const (
	BUFSIZE = 2048
)

var recordlen int
var ctype string
var ip string
var port string
var stopNum int
var msglen int
var interval int
var cnum int
var pnum int
var sfile string
var maddr string
var intf string

var chans []chan int64

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

func handleProd() {

	//print("in here")wg

	//time.Sleep(time.Second*1); // sleep for 1 second.
	if interval != 0 {
		time.Sleep(time.Microsecond * time.Duration(rand.Intn(interval)))
	}
	for i := 0; i < stopNum; i++ {
		nanoTime := time.Now().UnixNano()
		//print(nanoTime)
		for j := 0; j < len(chans); j++ {
			chans[j] <- int64(nanoTime)
			//print(j)
		}
		if interval > 0 {
			time.Sleep(time.Microsecond * time.Duration(interval))
		}
	}
	time.Sleep(time.Second * 1)
	wg.Done()
}

var wg sync.WaitGroup

func main() {

	flag.IntVar(&interval, "i", 5000, "The interval of sending message in ms")
	flag.IntVar(&msglen, "l", 1000, "The message length")
	flag.IntVar(&cnum, "n", 2, "Number of concurrent client connections")
	flag.IntVar(&pnum, "pn", 1, "Number of concurrent producors")
	//flag.StringVar(&ip, "a", "127.0.0.1", "The IP address of remote server")
	flag.StringVar(&port, "p", "8080", "The port number of remote server")
	flag.StringVar(&ctype, "c", "tcp", "The connection type, TCP or UDP or UDT")
	flag.IntVar(&stopNum, "s", 400, "Number of message to send before stop")
	flag.IntVar(&recordlen, "rl", 200, "The number of latency to record")
	flag.StringVar(&sfile, "f", "tcp_server", "The file name for recording the latency")
	flag.StringVar(&maddr, "m", "224.0.1.1", "Multicast address")
	flag.StringVar(&intf, "if", "em1", "interface for multicast")
	flag.Parse()
	if ctype != "multicast" {
		chans = make([]chan int64, cnum)
	} else {
		chans = make([]chan int64, 1)
	}
	for t := range chans {
		chans[t] = make(chan int64)
	}
	if ctype == "tcp" {
		ln, err := net.Listen(ctype, ":"+port)
		CheckErrorExit("Listen Error", err)
		for i := 1; i <= cnum; i++ {
			conn, err := ln.Accept()
			if err != nil {
				fmt.Println(err)
				continue
			}
			//fmt.Println("Accept from a client ", i)
			go handleTCP(i, conn, chans[i-1])
		}
		defer ln.Close()
	} else if ctype == "udp" {
		serverAddr, err := net.ResolveUDPAddr("udp", ":"+port)
		CheckErrorExit("Resolve UDP Error", err)

		ln, err := net.ListenUDP(ctype, serverAddr)
		CheckErrorExit("Listen Error", err)
		buf := make([]byte, BUFSIZE)
		for i := 1; i <= cnum; i++ {
			_, addr, err := ln.ReadFromUDP(buf)
			if err != nil {
				fmt.Println("ReadFromUDP Error: ", err)
			}

			//print("get connection")
			wg.Add(1)
			//go handleUDP(ln, chans[i-1])
			go handleUDP(addr.String(), chans[i-1])
		}
		print("connection done\n")
		// }else if ctype == "udt" {
		// 	serverAddr,err := udt.ResolveUDTAddr("udt",":"+port)
		//   CheckErrorExit("Resolve UDT Error", err)
		// 	ln, err := udt.ListenUDT(ctype, serverAddr)
		// 	CheckErrorExit("Listen Error", err)
		// 	defer ln.Close()
		// 	for i:= 1; i <= cnum; i++  {
		// 		conn, err := ln.Accept()
		// 		if err != nil {
		// 			fmt.Println(err)
		// 			continue
		// 		}
		// 		//fmt.Println("Accept from a client ", i)
		// 		wg.Add(1)
		// 		go handleUDT(i, conn, chans[i-1])
		// 	}
		// 	defer ln.Close()
	} else if ctype == "multicast" {
		time.Sleep(time.Second * 10)
		wg.Add(1)
		//go handleUDP(ln, chans[i-1])
		go handleMulticast(chans[0])
	}
	wg.Wait()
	for j := 1; j <= pnum; j++ {
		wg.Add(1)
		//print("start producer")
		go handleProd()
	}
	beginTime := time.Now().UnixNano()
	wg.Wait()
	for j := 0; j < len(chans); j++ {
		close(chans[j])
	}
	endTime := time.Now().UnixNano()
	fmt.Println("Average throughput is ", msglen*8*stopNum*pnum*1000/int(endTime-beginTime), " Mb/s")
	time.Sleep(time.Second * 1) // wait for another second to end process
}

func handleTCP(id int, c net.Conn, chanItem <-chan int64) {
	sendBuf := make([]byte, msglen)
	//time.Sleep(time.Microsecond * time.Duration(interval*rand.Intn(1000)))
	for i := 0; i < stopNum*pnum; i++ {
		send_time, more := <-chanItem
		if !more {
			break
		}
		//print(i)
		//print("get item\n")
		binary.PutVarint(sendBuf, int64(i))
		binary.PutVarint(sendBuf[8:], send_time)
		n, err := c.Write(sendBuf)
		if err != nil || n != msglen {
			fmt.Println("Write Error:", err)
			c.Close()
			break
		}
	}
}

//func handleUDP(addr *net.UDPAddr,  chanItem <-chan int64){
func handleUDP(addr string, chanItem <-chan int64) {
	laddr, _ := net.ResolveUDPAddr("udp", ":0")
	saddr, _ := net.ResolveUDPAddr("udp", addr)
	// CheckErrorExit("Resolve UDP ERROR", err)
	// time.Sleep(time.Millisecond*300)
	c, err := net.DialUDP("udp", laddr, saddr)
	CheckErrorExit("UDP Dial Error", err)

	wg.Done()
	sendBuf := make([]byte, msglen)
	//time.Sleep(time.Microsecond * time.Duration(interval*rand.Intn(1000)))
	for i := 0; i < stopNum*pnum; i++ {
		send_time, more := <-chanItem
		if !more {
			break
		}
		binary.PutVarint(sendBuf, int64(i))
		binary.PutVarint(sendBuf[8:], send_time)
		//n, err := c.Write(sendBuf)
		n, err := c.Write(sendBuf)
		if err != nil || n != msglen {
			fmt.Println("Write Error:", err)
			c.Close()
			break
		}
	}
	time.Sleep(time.Millisecond * 100)
	binary.PutVarint(sendBuf, int64(-1))
	binary.PutVarint(sendBuf[8:], time.Now().UnixNano())
	//n, err := c.Write(sendBuf)
	c.Write(sendBuf)
}

func handleMulticast(chanItem <-chan int64) {
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
	p.SetTTL(2)
	err = p.SetMulticastInterface(en0)
	CheckError("Set Multicast Interface Error", err)

	//currentTime := time.Now().UnixNano()

	wg.Done()
	for i := 0; i < stopNum*pnum; i++ {
		send_time, more := <-chanItem
		if !more {
			break
		}
		binary.PutVarint(b, int64(i))
		//currentTime = time.Now().UnixNano()
		binary.PutVarint(b[8:], send_time)
		//The application can also send both unicast and multicast packets.
		_, err = p.WriteTo(b, nil, dst)
		CheckError("Write To multicast Error", err)
	}
	time.Sleep(time.Millisecond * 100)
	binary.PutVarint(b, int64(-1))
	//binary.PutVarint(b[8:], send_time)
	_, err = p.WriteTo(b, nil, dst)
	CheckError("Write To multicast Error", err)
}

// func handleUDT(id int, c net.Conn, chanItem <-chan int64) {
// 	sendBuf := make([]byte, msglen)
// 	for i := 0; i< stopNum*pnum; i ++ {
// 		//receive message
// 		// _, err := c.Read(buf)
// 		// if err != nil {
// 		// 	fmt.Println("Read Error:", err)
// 		// 	c.Close()
// 		// 	break
// 		// }
// 		// if recordOrNot && stopNum-i <= recordlen {
// 		// 	currentTime := time.Now().UnixNano()
// 		// 	sentTime, _ := binary.Varint(buf[8:])
// 		// 	latency := currentTime - sentTime
// 		// 	// latency in microseconds
// 		// 	oneWayLatencies[recordlen-(stopNum-i)] = int(latency)
// 		// }
// 		//pktNum, _ := binary.Varint(buf)
// 		_, more := <- chanItem
// 		if !more {
// 			break
// 		}
// 		binary.PutVarint(sendBuf, int64(i))
// 		binary.PutVarint(sendBuf[8:], time.Now().UnixNano())
// 		n, err := c.Write(sendBuf)
// 		if err != nil || n != msglen {
//       fmt.Println("Write Error:", err)
//       c.Close()
//       break
//     }
// 		//if interval != 0 {
// 		//	time.Sleep(time.Millisecond * time.Duration(interval))
// 		//}
// 	}
// 	//ioutil.WriteFile("client_server",oneWayLatencies,0777)
// 	// if recordOrNot {
// 	// 	if err := writeLines(oneWayLatencies, sfile+strconv.Itoa(id)+".log"); err != nil {
// 	// 		fmt.Println("WRITE FILE ERROR")
// 	// 	}
// 	// }
// 	//fmt.Println("End Connection ", id, " ", i, " packets sent.")
// }

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
