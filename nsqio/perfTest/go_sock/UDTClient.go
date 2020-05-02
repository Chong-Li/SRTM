package main
import "github.com/jbenet/go-udtwrapper/udt"
import (
  "flag"
  "net"
  "fmt"
  "os"
  "time"
  "bufio"
  "encoding/binary"
  "math/rand"
  "sync"
)

const (
  BUFSIZE = 2048
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
var pktLost int

/* A Simple function to verify error */
func CheckErrorExit(errString string, err error) {
  if err != nil {
    fmt.Println(errString+": ", err)
    os.Exit(1)
  }
}

//func sendClient(id int )
func handleClient(wg *sync.WaitGroup, id int, recordOrNot bool) {
  time.Sleep(time.Microsecond * time.Duration(interval*rand.Intn(1000)))
  var conn net.Conn
  var err error
    /* Lets prepare a address at any address at port 10001*/
  localAddr, err := udt.ResolveUDTAddr("udt", ":0")
  CheckErrorExit("Resolve UDT ERROR", err)

  serverAddr, err := udt.ResolveUDTAddr("udt", ip+":"+port)
  CheckErrorExit("Resolve UDT ERROR", err)

  d := udt.Dialer{LocalAddr: localAddr}
  conn, err = d.DialUDT("udt", serverAddr)
  CheckErrorExit("UDT Dial Error", err)
  defer conn.Close()

  var roundTripLatencies []int
  var oneWayLatencies []int
  if recordOrNot {
    roundTripLatencies = make([]int, recordlen)
    oneWayLatencies = make([]int, recordlen)
  }

  buffer := make([]byte, msglen)
  bufferRcv := make([]byte, BUFSIZE)
  for i := 0; i < stopNum; i++ {
    //write to the connection
    currentTime1 := time.Now().UnixNano()
    binary.PutVarint(buffer, int64(i))
    binary.PutVarint(buffer[8:], currentTime1)
    conn.Write(buffer)

    n, err := conn.Read(bufferRcv)
    currentTime2 := time.Now().UnixNano()
    if n!=msglen {
      fmt.Println("send ", n, " Bytes and recieved ", msglen, " Bytes")
    }
    if err != nil  {
      fmt.Println("Read Error:", err)
      conn.Close()
      break
    }
    pktNum, _ := binary.Varint(bufferRcv)
    if pktNum != int64(i) {
      fmt.Println("Packet lost: send ", i, ", recive ", pktNum)
    }
    if recordOrNot && (stopNum-i <= recordlen) {
      serverSentTime, _ := binary.Varint(bufferRcv[8:])
      //append one way latency
      oneWayLatency := currentTime2 - serverSentTime
      oneWayLatencies[recordlen - stopNum +i] = int(oneWayLatency)

      //append round trip latency
      roundTripLatency := currentTime2 - currentTime1
      roundTripLatencies[recordlen - stopNum +i] = int(roundTripLatency)
    }
    //fmt.Println("sleep at ", i)
    time.Sleep(time.Millisecond * time.Duration(interval))
  }
  //write output to files
  if recordOrNot {
    writeLines(oneWayLatencies, oneFile)
    writeLines(roundTripLatencies, roundFile)
  }
  //fmt.Println("End Client", id, " ", pktLost, " packets lost")
  wg.Done()
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

func main() {


  flag.IntVar(&interval, "i", 5, "The interval of sending message in ms")
  flag.IntVar(&msglen, "l", 1000, "The message length")
  flag.IntVar(&cnum, "n", 2, "Number of concurrent client connections")
  flag.StringVar(&ip, "a", "127.0.0.1", "The IP address of remote server")
  flag.StringVar(&port, "p", "8080", "The port number of remote server")
  flag.StringVar(&ctype, "t", "udt", "The connection type, TCP or UDP or UDT")
  flag.IntVar(&stopNum, "s", 400, "Number of message to send before stop")
  flag.IntVar(&recordlen, "rl", 200, "The number of latency to record")
  flag.StringVar(&oneFile, "of", "udt_oneway", "The file name for one way latency")
  flag.StringVar(&roundFile, "rf", "udt_roundtrip", "The file name for round trip latancy")
  flag.Parse()

  var wg sync.WaitGroup
  var recordOrNot bool
  recordNum := rand.Intn(cnum)
  for i := 1; i <= cnum; i++ {
    recordOrNot = false
    if i-1 == recordNum {
      recordOrNot = true
    }
    wg.Add(1)
    go handleClient(&wg, i, recordOrNot)
  }
  wg.Wait()
  fmt.Println("END Client Program, ", pktLost, " packets lost")
}
