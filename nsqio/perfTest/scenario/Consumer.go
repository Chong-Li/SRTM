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
	//"math/rand"
	"strconv"
	"sync"
	//  "github.com/jbenet/go-udtwrapper/udt"
)

//import "strconv"
//import "io/ioutil"

var recordlen int
var ctype string
var ip string
var port string
var stopnum int
var msglen int
var interval int
var cnum int
var pnum int
var oneFile string
var roundFile string
var pktLost int
var intf string
var maddr string

var rwMutex sync.RWMutex

/* A Simple function to verify error */
func CheckErrorExit(errString string, err error) {
	if err != nil {
		fmt.Println(errString+": ", err)
		os.Exit(1)
	}
}

func handleTCP(wg *sync.WaitGroup, id int, recordOrNot bool) {
	//time.Sleep(time.Microsecond * time.Duration(interval*rand.Intn(1000)))
	var conn net.Conn
	var err error
	conn, err = net.Dial(ctype, ip+":"+port)
	CheckErrorExit("TCP Connection Error", err)

	//var roundTripLatencies []int
	var oneWayLatencies []int
	if recordOrNot {
		//roundTripLatencies = make([]int, recordlen)
		oneWayLatencies = make([]int, recordlen)
	}

	//buffer := make([]byte, msglen)
	bufferRcv := make([]byte, msglen)
	i := 0
	recordNum := 0
	recordStart := stopnum * pnum / 4
	recordStop := stopnum * pnum * 3 / 4
	gap := stopnum * pnum / recordlen / 2
	//beginTime := time.Now().UnixNano()
	for i = 0; i < stopnum*pnum; i++ {
		//write to the connection
		//currentTime1 := time.Now().UnixNano()
		//binary.PutVarint(buffer, int64(i))
		//binary.PutVarint(buffer[8:], currentTime1)
		//conn.Write(buffer)

		n, err := conn.Read(bufferRcv)
		currentTime2 := time.Now().UnixNano()
		if err != nil {
			fmt.Println("Read Error:", err)
			conn.Close()
			break
		}
		if n == 0 {
			fmt.Println("recieved ", n, " Bytes instead of ", msglen, " Bytes")
		} else if n < msglen {
			m, err := conn.Read(bufferRcv[n:])
			currentTime2 = time.Now().UnixNano()
			if err != nil {
				fmt.Println("Read Error:", err)
				conn.Close()
				break
			}
			if n+m != msglen {
				fmt.Println("recieved ", n+m, " Bytes instead of ", msglen, " Bytes")
			}
		}

		//pktNum, _ := binary.Varint(bufferRcv)
		//if pktNum != int64(i) {
		//  fmt.Println("Packet lost: send ", i, ", recive ", pktNum)
		//}
		//fmt.Println(recordStart, recordStop, recordNum, gap)
		if recordOrNot && (i >= recordStart) && i <= recordStop && recordNum < recordlen && (gap == 0 || (i-recordStart)%gap == 0) {
			serverSentTime, _ := binary.Varint(bufferRcv[8:])
			//append one way latency
			oneWayLatency := currentTime2 - serverSentTime
			oneWayLatencies[recordNum] = int(oneWayLatency)
			recordNum++
			//append round trip latency
			//roundTripLatency := currentTime2 - currentTime1
			//roundTripLatencies[recordlen - stopnum +i] = int(roundTripLatency)
		}
		//time.Sleep(time.Duration(interval*1000) * time.Microsecond)
	}
	//write output to files
	//endTime := time.Now().UnixNano()
	if recordOrNot {
		writeLines(oneWayLatencies, recordNum, oneFile+"_"+strconv.Itoa(id))
		//writeLines(roundTripLatencies, roundFile)
	}
	//fmt.Println("End Client", id)
	wg.Done()
}

// func handleUDT(wg *sync.WaitGroup, id int, recordOrNot bool) {
//   //time.Sleep(time.Microsecond * time.Duration(interval*rand.Intn(1000)))
//   var conn net.Conn
//   var err error
//     // Lets prepare a address at any address at port 10001
//   localAddr, err := udt.ResolveUDTAddr("udt", ":0")
//   CheckErrorExit("Resolve UDT ERROR", err)
//
//   serverAddr, err := udt.ResolveUDTAddr("udt", ip+":"+port)
//   CheckErrorExit("Resolve UDT ERROR", err)
//
//   d := udt.Dialer{LocalAddr: localAddr}
//   conn, err = d.DialUDT("udt", serverAddr)
//   CheckErrorExit("UDT Dial Error", err)
//   defer conn.Close()
//
//   //var roundTripLatencies []int
//   var oneWayLatencies []int
//   if recordOrNot {
//     //roundTripLatencies = make([]int, recordlen)
//     oneWayLatencies = make([]int, recordlen)
//   }
//
//   //buffer := make([]byte, msglen)
//   bufferRcv := make([]byte, msglen)
//   i:= 0
//   recordNum := 0
//   recordStart := stopnum*pnum/4
//   recordStop := stopnum*pnum*3/4
//   gap := stopnum*pnum/recordlen
//
//   for i = 0; i < stopnum*pnum; i++ {
//     //write to the connection
//     //currentTime1 := time.Now().UnixNano()
//     //binary.PutVarint(buffer, int64(i))
//     //binary.PutVarint(buffer[8:], currentTime1)
//     //conn.Write(buffer)
//     n, err := conn.Read(bufferRcv)
//     currentTime2 := time.Now().UnixNano()
//     if n!=msglen {
//       fmt.Println("expecting ", msglen, " Bytes and recieved ", n, " Bytes")
//     }
//     if err != nil  {
//       fmt.Println("Read Error:", err)
//       conn.Close()
//       break
//     }
//     pktNum, _ := binary.Varint(bufferRcv)
//     if pktNum != int64(i) {
//       fmt.Println("Expecting ", i, " recieved ",pktNum)
//     }
//     if pktNum < int64(i) {
//       i --
//       continue
//     }
//     if recordOrNot && (i >= recordStart) && i <= recordStop && recordNum < recordlen && (gap == 0 || (i-recordStart)%gap == 0){
//       serverSentTime, _ := binary.Varint(bufferRcv[8:])
//       //append one way latency
//       oneWayLatency := currentTime2 - serverSentTime
//       oneWayLatencies[recordNum] = int(oneWayLatency)
//       recordNum ++
//       //append round trip latency
//       //roundTripLatency := currentTime2 - currentTime1
//       //roundTripLatencies[recordlen - stopnum +i] = int(roundTripLatency)
//     }
//     //fmt.Println("sleep at ", i)
//     //time.Sleep(time.Millisecond * time.Duration(interval))
//   }
//   //write output to files
//   if recordOrNot {
//     writeLines(oneWayLatencies, recordNum, oneFile+"_"+strconv.Itoa(id))
//     //writeLines(roundTripLatencies, roundFile)
//   }
//   //fmt.Println("End Client", id, " ", stopnum-i , " packets lost")
//   wg.Done()
// }

func handleUDP(wg *sync.WaitGroup, id int, recordOrNot bool) {
	//time.Sleep(time.Microsecond * time.Duration(interval*rand.Intn(1000)))
	var err error
	/* Lets prepare a address at any address at port 10001*/
	localAddr, err := net.ResolveUDPAddr("udp", ":0")
	CheckErrorExit("Resolve UDP ERROR", err)

	serverAddr, err := net.ResolveUDPAddr("udp", ip+":"+port)
	CheckErrorExit("Resolve UDP ERROR", err)

	conn, err := net.ListenUDP("udp", localAddr)

	conn.SetWriteBuffer(4 * 1024 * 1024) // set write buffer side to 10M
	conn.SetReadBuffer(4 * 1024 * 1024)  // set write buffer side to 10M

	//var roundTripLatencies []int
	var oneWayLatencies []int
	if recordOrNot {
		//roundTripLatencies = make([]int, recordlen)
		oneWayLatencies = make([]int, recordlen)
	}

	//buffer := make([]byte, msglen)
	bufferRcv := make([]byte, msglen)
	buff := make([]byte, msglen)
	binary.PutVarint(buff, int64(conn.LocalAddr().(*net.UDPAddr).Port))
	//binary.PutVarint(buff[8:], )
	conn.WriteTo(buff, serverAddr)
	//ln, _ := net.ListenUDP("udp", localAddr)

	i := 0
	recordNum := 0
	recordStart := stopnum * pnum / 4
	recordStop := stopnum * pnum * 3 / 4
	gap := stopnum * pnum / recordlen / 2
	//conn.SetReadDeadline(time.Now().Add(time.Second*1))
	for {
		//write to the connection
		//currentTime1 := time.Now().UnixNano()
		//binary.PutVarint(buffer, int64(i))
		//binary.PutVarint(buffer[8:], currentTime1)
		//conn.Write(buffer)

		//ln.SetReadDeadline(time.Now().Add(time.Second*1))
		//n,_,err := ln.ReadFromUDP(bufferRcv)
		//n,_,err := ln.ReadFromUDP(bufferRcv)
		n, _, err := conn.ReadFromUDP(bufferRcv)
		currentTime2 := time.Now().UnixNano()
		if err != nil {
			fmt.Println("Read Error:", err)
			conn.Close()
			break
		}
		if n != msglen {
			fmt.Println("expecting ", msglen, " Bytes and recieved ", n, " Bytes")
		}
		seqNum, _ := binary.Varint(bufferRcv)
		if seqNum == int64(-1) {
			break
		}
		if recordOrNot && (i >= recordStart) && i <= recordStop && recordNum < recordlen && (gap == 0 || (i-recordStart)%gap == 0) {
			serverSentTime, _ := binary.Varint(bufferRcv[8:])
			//append one way latency
			oneWayLatency := currentTime2 - serverSentTime
			oneWayLatencies[recordNum] = int(oneWayLatency)
			recordNum++
			//append round trip latency
			//roundTripLatency := currentTime2 - currentTime1
			//roundTripLatencies[recordlen - stopnum +i] = int(roundTripLatency)
		}
		i++
		if i >= stopnum*pnum {
			break
		}
		//fmt.Println("sleep at ", i)
		//time.Sleep(time.Millisecond * time.Duration(interval))
	}
	//write output to files
	if recordOrNot {
		writeLines(oneWayLatencies, recordNum, oneFile+"_"+strconv.Itoa(id))
		//writeLines(roundTripLatencies, roundFile)
	}
	if stopnum*pnum-i > 0 {
		rwMutex.Lock()
		pktLost += stopnum*pnum - i - 1
		rwMutex.Unlock()
	}
	wg.Done()
}

func handleMulticast(wg *sync.WaitGroup, id int, recordOrNot bool) {
	//time.Sleep(time.Microsecond * time.Duration(interval*rand.Intn(1000)))
	var err error
	/* Lets prepare a address at any address at port 10001*/
	en0, err := net.InterfaceByName("em1")
	CheckErrorExit("Interface By Name Error", err)

	//en1, err := net.InterfaceByName("em2")
	//CheckErrorExit("Interface By Name Error", err)

	group := net.ParseIP(maddr)

	//an application listens to an appropriate address with an appropriate service port.
	c, err := net.ListenPacket("udp4", "224.0.0.0:"+port)
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

	p.SetTOS(0x0)
	p.SetTTL(2)

	//c.SetWriteBuffer(4 * 1024 * 1024) // set write buffer side to 10M
	//c.SetReadBuffer(4 * 1024 * 1024)  // set write buffer side to 10M

	//var roundTripLatencies []int
	var oneWayLatencies []int
	if recordOrNot {
		//roundTripLatencies = make([]int, recordlen)
		oneWayLatencies = make([]int, recordlen)
	}

	//buffer := make([]byte, msglen)
	bufferRcv := make([]byte, msglen)
	//ln, _ := net.ListenUDP("udp", localAddr)

	i := 0
	recordNum := 0
	recordStart := stopnum * pnum / 4
	recordStop := stopnum * pnum * 3 / 4
	gap := stopnum * pnum / recordlen / 2
	//conn.SetReadDeadline(time.Now().Add(time.Second*1))
	for {
		n, cm, _, err := p.ReadFrom(bufferRcv)
		CheckErrorExit("ReadFrom socket error", err)
		currentTime := time.Now().UnixNano()
		if !cm.Dst.IsMulticast() || !cm.Dst.Equal(group) {
			continue
		}
		if n != msglen {
			fmt.Println("expecting ", msglen, " Bytes and recieved ", n, " Bytes")
		}

		seqNum, _ := binary.Varint(bufferRcv)
		if seqNum == int64(-1) {
			break
		}
		if recordOrNot && (i >= recordStart) && i <= recordStop && recordNum < recordlen && (gap == 0 || (i-recordStart)%gap == 0) {
			serverSentTime, _ := binary.Varint(bufferRcv[8:])
			//append one way latency
			oneWayLatency := currentTime - serverSentTime
			oneWayLatencies[recordNum] = int(oneWayLatency)
			recordNum++
			//append round trip latency
			//roundTripLatency := currentTime2 - currentTime1
			//roundTripLatencies[recordlen - stopnum +i] = int(roundTripLatency)
		}
		i++
		if i >= stopnum*pnum {
			break
		}
		//fmt.Println("sleep at ", i)
		//time.Sleep(time.Millisecond * time.Duration(interval))
	}
	//write output to files
	if recordOrNot {
		writeLines(oneWayLatencies, recordNum, oneFile+"_"+strconv.Itoa(id))
		//writeLines(roundTripLatencies, roundFile)
	}
	if stopnum*pnum-i > 0 {
		rwMutex.Lock()
		pktLost += stopnum*pnum - i - 1
		rwMutex.Unlock()
	}
	wg.Done()
}

func writeLines(lines []int, num int, path string) error {
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()

	w := bufio.NewWriter(file)
	i := 1
	for _, line := range lines {
		fmt.Fprintln(w, line)
		i++
		if i > num {
			break
		}
	}
	return w.Flush()
}

func main() {

	flag.IntVar(&interval, "i", 5, "The interval of sending message in ms")
	flag.IntVar(&msglen, "l", 1000, "The message length")
	flag.IntVar(&cnum, "n", 2, "Number of concurrent client connections")
	flag.IntVar(&pnum, "pn", 1, "Number of concurrent producors")

	flag.StringVar(&ip, "a", "127.0.0.1", "The IP address of remote server")
	flag.StringVar(&port, "p", "8080", "The port number of remote server")
	flag.StringVar(&ctype, "c", "tcp", "The connection type, TCP or UDP or UDT")
	flag.IntVar(&stopnum, "s", 400, "Number of message to send before stop")
	flag.IntVar(&recordlen, "rl", 200, "The number of latency to record")
	flag.StringVar(&oneFile, "of", "client_oneWay", "The file name for one way latency")
	flag.StringVar(&roundFile, "rf", "client_roundtrip", "The file name for round trip latancy")
	flag.StringVar(&maddr, "m", "224.0.1.1", "Multicast address")
	flag.StringVar(&intf, "if", "em1", "interface for multicast")
	flag.Parse()

	var wg sync.WaitGroup
	var recordOrNot bool
	pktLost = 0
	for i := 1; i <= cnum; i++ {
		recordOrNot = false
		if cnum < 32 || i%(cnum/32) == 0 {
			recordOrNot = true
		}
		wg.Add(1)
		if ctype == "tcp" {
			go handleTCP(&wg, i, recordOrNot)
		} else if ctype == "udp" {
			go handleUDP(&wg, i, recordOrNot)
			time.Sleep(time.Microsecond * 5)
			// }else if ctype == "udt" {
			//   go handleUDT(&wg, i, recordOrNot)
		} else if ctype == "multicast" {
			go handleMulticast(&wg, i, recordOrNot)
		}
	}
	wg.Wait()
	if ctype == "udp" {
		fmt.Println("Pket loss is ", pktLost, " loss rate is ", pktLost*100.0/stopnum/pnum, "%")
	}
	//fmt.Println("END Client Program")
}
