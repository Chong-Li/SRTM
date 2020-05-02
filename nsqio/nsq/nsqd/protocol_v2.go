// +build rtm

package nsqd

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	//"io/ioutil"
	"math"
	"math/rand"
	"net"
	"strconv"
	"strings"
	"runtime"
	"sort"
	"sync/atomic"
	//"syscall"
	"time"
	"unsafe"
	"reflect"

	"github.com/WU-CPSL/RTM-0.1/nsqio/nsq/internal/protocol"
	"github.com/WU-CPSL/RTM-0.1/nsqio/nsq/internal/version"
	"github.com/WU-CPSL/RTM-0.1/nsqio/rate"
)

//yao
var topicLatencies []byte
var topicLat []int64
var topicMessagesReceived = 0

const maxTimeout = time.Hour

const (
	frameTypeResponse int32 = 0
	frameTypeError    int32 = 1
	frameTypeMessage  int32 = 2
	frameTypeNull     int32 = 3
)

var separatorBytes = []byte(" ")
var pubBytes = []byte("PUB")
var heartbeatBytes = []byte("_heartbeat_")
var okBytes = []byte("OK")

// jarry multicast
var nullBytes = []byte("_null_")

var congestBytes = []byte("CONGEST")
var decongestBytes = []byte("DECONGEST")

type protocolV2 struct {
	ctx *context
}

// jarry the IOLoop that enables Multicast
func (p *protocolV2) IOLoopV2(conn net.Conn, connType int, channel *Channel) error {
	//RTM.dtb, dedicate thread
	//var mask [1024/64]uint64
	//mask[10/64] |= 1 << (10%64)
	//mask[0] =65532
	//_,_,_= syscall.RawSyscall(203, uintptr(0), uintptr(len(mask)*8), uintptr(unsafe.Pointer(&mask)))
	//runtime.LockOSThread()
	var err error
	var line []byte
	var zeroTime time.Time

	clientID := atomic.AddInt64(&p.ctx.nsqd.clientIDSequence, 1)
	client := newClientV2(clientID, conn, p.ctx)
	client.connType = connType

	//RTM
	p.ctx.nsqd.Notify(client)
	// synchronize the startup of messagePump in order
	// to guarantee that it gets a chance to initialize
	// goroutine local state derived from client attributes
	// and avoid a potential race with IDENTIFY (where a client
	// could have changed or disabled said attributes)
	messagePumpStartedChan := make(chan bool)
	go p.messagePump(client, messagePumpStartedChan)
	<-messagePumpStartedChan

	if p.ctx.nsqd.getOpts().Verbose {
		p.ctx.nsqd.logf("PROTOCOL(V2): [%s] start ioloop", client)
	}

	// if the connection is multicast, add client to given channel.
	if connType == ConnTypeMulticast {
		client.SetReadyCount(p.ctx.nsqd.getOpts().MaxRdyCount)

		channel.AddClient(client.ID, client)

		atomic.StoreInt32(&client.State, stateSubscribed)
		client.Channel = channel
		// update message pump
		client.SubEventChan <- channel
	}

	for {
		if client.HeartbeatInterval > 0 && connType != ConnTypeMulticast {
			client.SetReadDeadline(time.Now().Add(client.HeartbeatInterval * 2))
		} else {
			client.SetReadDeadline(zeroTime)
		}

		// ReadSlice does not allocate new space for the data each request
		// ie. the returned slice is only valid until the next call to it
		//var buf [4]byte
		n, err := client.Reader.Discard(4) // discard addr size uint32
		if n != 4 || err != nil {
			err = fmt.Errorf("fail to read addr length - %s", err)
			break
		}
		addrline, err := client.Reader.ReadSlice('\n')
		if err != nil {
			if err == io.EOF {
				err = nil
			} else {
				err = fmt.Errorf("failed to read address - %s", err)
			}
			fmt.Println("exit due to readslice return ")
			break
		}
		srcAddr := strings.TrimSpace(string(addrline[:len(addrline)-1]))

		line, err = client.Reader.ReadSlice('\n')
		if err != nil {
			if err == io.EOF {
				err = nil
			} else {
				err = fmt.Errorf("failed to read command - %s", err)
			}
			fmt.Println("exit due to readslice return ")
			break
		}

		// trim the '\n'
		line = line[:len(line)-1]
		// optionally trim the '\r'
		if len(line) > 0 && line[len(line)-1] == '\r' {
			line = line[:len(line)-1]
		}
		var response []byte
		if bytes.HasPrefix(line, pubBytes) {
                        //response, err = p.PUB(client, line[4:])
                } else {
		params := bytes.Split(line, separatorBytes)

		if bytes.Equal(params[0], []byte("NACK")) { // need src addr for NACK
			params = append(params, []byte(srcAddr))
		}

		if p.ctx.nsqd.getOpts().Verbose {
			p.ctx.nsqd.logf("PROTOCOL(V2): [%s] %s", client, params)
		}

		response, err = p.Exec(client, params)
		}
		if err != nil {
			ctx := ""
			if parentErr := err.(protocol.ChildErr).Parent(); parentErr != nil {
				ctx = " - " + parentErr.Error()
			}
			p.ctx.nsqd.logf("ERROR: [%s] - %s%s", client, err, ctx)

			sendErr := p.Send(client, frameTypeError, []byte(err.Error()))
			if sendErr != nil {
				p.ctx.nsqd.logf("ERROR: [%s] - %s%s", client, sendErr, ctx)
				break
			}

			// errors of type FatalClientErr should forceably close the connection
			if _, ok := err.(*protocol.FatalClientErr); ok {
				break
			}
			continue
		}

		if response != nil {
			err = p.Send(client, frameTypeResponse, response)
			if err != nil {
				err = fmt.Errorf("failed to send response - %s", err)
				break
			}
		}
	}

	p.ctx.nsqd.logf("PROTOCOL(V2): [%s] exiting ioloop", client)
	conn.Close()
	close(client.ExitChan)

	//RTM
	client.exitFlag = 1
	p.ctx.nsqd.Notify(client)

	if client.Channel != nil {
		client.Channel.RemoveClient(client.ID)
	}

	return err
}
//var curTT int64

func (p *protocolV2) IOLoop(conn net.Conn) error {
	var err error
	var line []byte
	var zeroTime time.Time

	clientID := atomic.AddInt64(&p.ctx.nsqd.clientIDSequence, 1)
	client := newClientV2(clientID, conn, p.ctx)

	//RTM
	p.ctx.nsqd.Notify(client)

	// synchronize the startup of messagePump in order
	// to guarantee that it gets a chance to initialize
	// goroutine local state derived from client attributes
	// and avoid a potential race with IDENTIFY (where a client
	// could have changed or disabled said attributes)
	messagePumpStartedChan := make(chan bool)
	go p.messagePump(client, messagePumpStartedChan)
	<-messagePumpStartedChan

	for {
		if client.HeartbeatInterval > 0 {
			client.SetReadDeadline(time.Now().Add(client.HeartbeatInterval * 2))
		} else {
			client.SetReadDeadline(zeroTime)
		}

		// ReadSlice does not allocate new space for the data each request
		// ie. the returned slice is only valid until the next call to it
		/*line, err = client.Reader.ReadSlice('\n')
		if err != nil {
			if err == io.EOF {
				err = nil
			} else {
				err = fmt.Errorf("failed to read command - %s", err)
			}
			break
		}*/
		line, client.curTT, err = client.TReader.ReadSliceMsg('\n')
		if err != nil {
                        if err == io.EOF {
                                err = nil
                        } else {
                                err = fmt.Errorf("failed to read command - %s", err)
                        }
                        break
                }
		// trim the '\n'
		line = line[:len(line)-1]
		//fmt.Printf("come one pub %v %v\n", string(line), curTT)

		// optionally trim the '\r'
		if len(line) > 0 && line[len(line)-1] == '\r' {
			line = line[:len(line)-1]
		}
		var response []byte
		if bytes.HasPrefix(line, pubBytes) {
			response, err = p.PUB(client, line[4:])
		} else {
			params := bytes.Split(line, separatorBytes)

			if p.ctx.nsqd.getOpts().Verbose {
				p.ctx.nsqd.logf("PROTOCOL(V2): [%s] %s", client, params)
			}

			response, err = p.Exec(client, params)
		}
		if err != nil {
			ctx := ""
			if parentErr := err.(protocol.ChildErr).Parent(); parentErr != nil {
				ctx = " - " + parentErr.Error()
			}
			p.ctx.nsqd.logf("ERROR: [%s] - %s%s", client, err, ctx)

			sendErr := p.Send(client, frameTypeError, []byte(err.Error()))
			if sendErr != nil {
				p.ctx.nsqd.logf("ERROR: [%s] - %s%s", client, sendErr, ctx)
				break
			}

			// errors of type FatalClientErr should forceably close the connection
			if _, ok := err.(*protocol.FatalClientErr); ok {
				break
			}
			continue
		}

		if response != nil {
			err = p.Send(client, frameTypeResponse, response)
			if err != nil {
				err = fmt.Errorf("failed to send response - %s", err)
				break
			}
		}
	}

	p.ctx.nsqd.logf("PROTOCOL(V2): [%s] exiting ioloop", client)
	conn.Close()
	close(client.ExitChan)

	//RTM
	client.exitFlag = 1
	p.ctx.nsqd.Notify(client)

	if client.Channel != nil {
		client.Channel.RemoveClient(client.ID)
	}

	return err
}

func (p *protocolV2) SendMessage(client *clientV2, msg *Message, buf *bytes.Buffer) error {
	if p.ctx.nsqd.getOpts().Verbose {
		p.ctx.nsqd.logf("PROTOCOL(V2): [%s] Writing Message %d", client, msg.SeqID)
	}

	buf.Reset()
	//_, err := msg.WriteTo(buf)
	_, err := msg.WriteToV2(buf, client.buf)
	
	if err != nil {
		return err
	}
	err = p.Send(client, frameTypeMessage, buf.Bytes())
	if err != nil {
		return err
	}

	return nil
}

func (p *protocolV2) SendNullMessage(client *clientV2, msg *Message, buf *bytes.Buffer) error {
	if p.ctx.nsqd.getOpts().Verbose {
		p.ctx.nsqd.logf("PROTOCOL(V2): [%s] Send Null Message %d", client, msg.SeqID)
	}

	buf.Reset()
	_, err := msg.WriteTo(buf)
	if err != nil {
		return err
	}

	err = p.Send(client, frameTypeNull, buf.Bytes())
	if err != nil {
		return err
	}

	return nil
}

func (p *protocolV2) Send(client *clientV2, frameType int32, data []byte) error {
	client.writeLock.Lock()
	defer client.writeLock.Unlock()
	var err error
	var zeroTime time.Time
	if client.HeartbeatInterval > 0 {
		client.SetWriteDeadline(time.Now().Add(client.HeartbeatInterval))
	} else {
		client.SetWriteDeadline(zeroTime)
	}

	if client.connType == ConnTypeMulticast {
		_, err = protocol.SendAddr(client.Writer, "")
		if err != nil {
			return err
		}
	}
	//_, err = protocol.SendFramedResponse(client.Writer, frameType, data)
	_, err = protocol.SendFramedResponseV2(client.Writer, frameType, data, client.beBuf)

	if err != nil {
		return err
	}

	// jarry Flush for any message
	//RTM.dtb, this should be enabled
	if frameType != frameTypeMessage {
		err = client.Flush()
	}
	return err
}

func (p *protocolV2) Exec(client *clientV2, params [][]byte) ([]byte, error) {
	if bytes.Equal(params[0], []byte("IDENTIFY")) {
		return p.IDENTIFY(client, params)
	}
	err := enforceTLSPolicy(client, p, params[0])
	if err != nil {
		return nil, err
	}
	switch {
	case bytes.Equal(params[0], []byte("FIN")):
		return p.FIN(client, params)
	case bytes.Equal(params[0], []byte("RDY")):
		return p.RDY(client, params)
	case bytes.Equal(params[0], []byte("REQ")):
		return p.REQ(client, params)
	//case bytes.Equal(params[0], []byte("PUB")):
		//return p.PUB(client, params)
	//RTM.dtb
	case bytes.Equal(params[0], []byte("TIME")):
		p.s1Delay(client, params[1:])
		return nil,nil
	case bytes.Equal(params[0], []byte("MPUB")):
		return p.MPUB(client, params)
	case bytes.Equal(params[0], []byte("DPUB")):
		return p.DPUB(client, params)
	case bytes.Equal(params[0], []byte("NOP")):
		return p.NOP(client, params)
	case bytes.Equal(params[0], []byte("TOUCH")):
		return p.TOUCH(client, params)
	case bytes.Equal(params[0], []byte("SUB")):
		return p.SUB(client, params)
	case bytes.Equal(params[0], []byte("CLS")):
		return p.CLS(client, params)
	case bytes.Equal(params[0], []byte("AUTH")):
		return p.AUTH(client, params)
	// for reliablility
	case bytes.Equal(params[0], []byte("NACK")):
		return p.NACK(client, params)
	}
	return nil, protocol.NewFatalClientErr(nil, "E_INVALID", fmt.Sprintf("invalid command %s", params[0]))
}

func (p *protocolV2) messagePump(client *clientV2, startedChan chan bool) {
	//RTM.dtb, dedicate thread
	//var mask [1024/64]uint64
	//mask[10/64] |= 1 << (10%64)
	//mask[0]=65532
	//_,_,_= syscall.RawSyscall(203, uintptr(0), uintptr(len(mask)*8), uintptr(unsafe.Pointer(&mask)))
	//runtime.LockOSThread()
	var err error
	//var buf bytes.Buffer
	//RTM.dtb
	buf := &client.txBuf
	var clientMsgChan chan *Message
	//RTM.dtb
	var memoryMsgChan chan *Message
	/*var tickerCPU <-chan time.Time
	load := &syscall.Rusage{}
	syscall.Getrusage(syscall.RUSAGE_THREAD, load)
	last_cpu := int(load.Utime.Nano() + load.Stime.Nano())
	last_time := time.Now().UnixNano()
	cpu := 0
	this_time := time.Now().UnixNano()*/
	//bufNum := 0
	//lastChanLen := 128
	//var sendLatency []byte
	//sendLen := 0
	//first := true
	var subChannel *Channel
	// NOTE: `flusherChan` is used to bound message latency for
	// the pathological case of a channel on a low volume topic
	// with >1 clients having >1 RDY counts
	var flusherChan <-chan time.Time
	var sampleRate int32

	if p.ctx.nsqd.getOpts().Verbose {
		p.ctx.nsqd.logf("PROTOCOL(V2): [%s] start messagePump", client)
	}

	subEventChan := client.SubEventChan
	identifyEventChan := client.IdentifyEventChan
	outputBufferTicker := time.NewTicker(client.OutputBufferTimeout)
	heartbeatTicker := time.NewTicker(client.HeartbeatInterval)
	heartbeatChan := heartbeatTicker.C
	msgTimeout := client.MsgTimeout

	//jarry multicast
	if client.connType == ConnTypeMulticast {
		msgTimeout = client.ctx.nsqd.getOpts().MsgSafeTimeout
	}
	atomic.StoreUint64(&client.SeqID, 0)
	multicastFlag := client.connType == ConnTypeMulticast
	lastSendTimeout := client.ctx.nsqd.getOpts().LastSendTimeout
	maxNullSend := client.ctx.nsqd.getOpts().MaxNullSend
	var lastSendChan <-chan time.Time
	var lastSendTimer *time.Timer
	nullSentCount := int64(0)
	var resendMsgChan chan *Message
	if !multicastFlag {
		lastSendTimeout = 0
		maxNullSend = 0
	}

	// v2 opportunistically buffers data to clients to reduce write system calls
	// we force flush in two cases:
	//    1. when the client is not ready to receive messages
	//    2. we're buffered and the channel has nothing left to send us
	//       (ie. we would block in this loop anyway)
	//
	flushed := true

	// signal to the goroutine that started the messagePump
	// that we've started up
	close(startedChan)
	resendFlag := false
	for {

		if subChannel == nil || !client.IsReadyForMessages() {
			// the client is not ready to receive messages...
			clientMsgChan = nil
			memoryMsgChan = nil
			flusherChan = nil
			// force flush
			client.writeLock.Lock()
			err = client.Flush()
			client.writeLock.Unlock()
			if err != nil {
				goto exit
			}
			flushed = true
			resendFlag = false
		} else if flushed {
			// last iteration we flushed...
			// do not select on the flusher ticker channel
			clientMsgChan = subChannel.clientMsgChan
			memoryMsgChan = subChannel.memoryMsgChan
			flusherChan = nil
			resendFlag = true
		} else {
			// we're buffered (if there isn't any more data we should flush)...
			// select on the flusher ticker channel, too
			clientMsgChan = subChannel.clientMsgChan
			memoryMsgChan = subChannel.memoryMsgChan
			flusherChan = outputBufferTicker.C
			resendFlag = true
		}

		// jarry: multicast
		if multicastFlag && resendFlag {
			if nullSentCount >= maxNullSend {
				resendMsgChan = subChannel.resendMsgChan
				lastSendChan = nil
			} else {
				resendMsgChan = subChannel.resendMsgChan
				if lastSendTimer != nil {
					lastSendChan = lastSendTimer.C
				} else {
					lastSendChan = nil
				}
			}
		} else {
			lastSendChan = nil
			resendMsgChan = nil
		}

		select {
		/*
		* RTM:
		* A topic just left congestion mode. We should send a decongestion signal (for
		* this topic) back to the producer. Once the producer receives a decongestion
		* signal, it should resume sending the msgs of the corresponding topic.
		 */
		case topicName := <-client.CongestChan:
			bytes := congestBytes
			bytes = append(bytes, topicName...)
			err := p.Send(client, frameTypeResponse, bytes)
			if err != nil {
				err = fmt.Errorf("failed to send congestion signal - %s", err)
			}
		case topicName := <-client.DeCongestChan:
			bytes := decongestBytes
			bytes = append(bytes, topicName...)
			err := p.Send(client, frameTypeResponse, bytes)
			if err != nil {
				err = fmt.Errorf("failed to send decongestion signal - %s", err)
			}
		case address := <-client.MigrateChan:
                        bytes := []byte("MIGRATE")
                        bytes = append(bytes, address...)
                        err := p.Send(client, frameTypeResponse, bytes)
                        if err != nil {
                                err = fmt.Errorf("failed to send migrate signal - %s", err)
                        }
		case topicName := <-client.profileChan:
                        bytes := []byte("PROFILE")
                        bytes = append(bytes, topicName...)
                        _ = p.Send(client, frameTypeResponse, bytes)

		case <-flusherChan:
			// if this case wins, we're either starved
			// or we won the race between other channels...
			// in either case, force flush
			client.writeLock.Lock()
			err = client.Flush()
			client.writeLock.Unlock()
			if err != nil {
				goto exit
			}
			flushed = true
		case <-client.ReadyStateChan:
		case subChannel = <-subEventChan:
			// you can't SUB anymore
			subEventChan = nil
			//RTM.dtb, dedicate thread
			//TCPConn := client.Conn.(*net.TCPConn)
			//TCPConn.SetWriteBuffer(4096000)
			//runtime.LockOSThread()
			//defer runtime.UnlockOSThread()
			//tickerCPU = time.Tick(1000 * time.Millisecond)
			//var mask2 [1024/64]uint64
			//mask2[10/64] |= 1 << (10%64)
			//mask2[0]=0
			//_,_,_= syscall.RawSyscall(203, uintptr(0), uintptr(len(mask2)*8), uintptr(unsafe.Pointer(&mask2)))
		/*case <-tickerCPU:
		syscall.Getrusage(syscall.RUSAGE_THREAD, load)
		cpu = int(load.Utime.Nano() + load.Stime.Nano())
		this_time = time.Now().UnixNano()
		fmt.Println((float64(cpu-last_cpu) / float64(this_time-last_time)) / 16.0)
		last_cpu = cpu
		last_time = this_time*/
		case identifyData := <-identifyEventChan:
			// you can't IDENTIFY anymore
			identifyEventChan = nil

			outputBufferTicker.Stop()
			if identifyData.OutputBufferTimeout > 0 {
				outputBufferTicker = time.NewTicker(identifyData.OutputBufferTimeout)
			}

			heartbeatTicker.Stop()
			heartbeatChan = nil
			if identifyData.HeartbeatInterval > 0 {
				heartbeatTicker = time.NewTicker(identifyData.HeartbeatInterval)
				heartbeatChan = heartbeatTicker.C
			}

			if identifyData.SampleRate > 0 {
				sampleRate = identifyData.SampleRate
			}

			msgTimeout = identifyData.MsgTimeout
		case <-heartbeatChan:
			err = p.Send(client, frameTypeResponse, heartbeatBytes)
			if err != nil {
				goto exit
			}
		//RTM.dtb
		case <-clientMsgChan:
		case msg, ok := <-memoryMsgChan:
			//RTM.dtb
			//before := time.Now().UnixNano()
			//RTM.dtbt
			//binary.PutVarint(msg.Body[180:], time.Now().UnixNano())
			if !ok {
				goto exit
			}

			if sampleRate > 0 && rand.Int31n(100) > sampleRate {
				continue
			}
			atomic.AddUint64(&client.SeqID, 1)
			msg.SeqID = atomic.LoadUint64(&client.SeqID)

			subChannel.StartInFlightTimeout(msg, client.ID, msgTimeout)
			client.SendingMessage()
			//RTM
			//copy(msg.Body[150:], []byte("congest_sent"))
			//RTM.dtb test only
			//X := time.Now().UnixNano()
			//if bytes.HasPrefix(msg.Body[24:], []byte{'0'}) {
				//binary.PutVarint(msg.Body[80:], X)
				/*var lenByte []byte
				if first == true {
					lenByte = []byte(strconv.Itoa(len(memoryMsgChan) + 1))
					first = false
				} else {
					//lenByte = []byte("0")
					lenByte = []byte(strconv.Itoa(len(memoryMsgChan) + 1))
				}
				lenByte = append(lenByte, "\n"...)*/
				/*	file,_:=client.Conn.(*net.TCPConn).File()
					fd:=file.Fd()
					var size int
					_,_,_=syscall.Syscall(syscall.SYS_IOCTL,uintptr(fd), uintptr(syscall.TIOCOUTQ),uintptr(unsafe.Pointer(&size)))
					lenByte = append(lenByte, strconv.Itoa(size)...)*/
				//lenByte = append(lenByte, "\n"...)
				//copy(msg.Body[130:], lenByte)
			//}
			//RTM.dtb
			/*if client.subTopic.startRec {
				client.departTimes[client.departNums]=X
				client.departNums++
			}*/
			//err = p.SendMessage(client, msg, &buf)
			err = p.SendMessage(client, msg, buf)
			if err != nil {
				goto exit
			}

			flushed = false

			//after := time.Now().UnixNano()
			//sendLatency = append(sendLatency, strconv.FormatInt(after-before, 10)...)
			//sendLatency = append(sendLatency, " "...)
			//RTM.dtb2
			if len(memoryMsgChan) == 0 {
				client.writeLock.Lock()
				err = client.Flush()
				client.writeLock.Unlock()
				if err != nil {
					goto exit
				}
				flushed = true
				//first = true
				//sendLen = 0
			}

			// jarry: multicast, enable last send timer
			if multicastFlag && lastSendTimeout > 0 {
				if lastSendTimer == nil {
					lastSendTimer = time.NewTimer(lastSendTimeout)
				} else {
					if !lastSendTimer.Stop() {
						<-lastSendTimer.C
					}
					lastSendTimer.Reset(lastSendTimeout)
				}
				nullSentCount = 0
			}
			/*after2 := time.Now().UnixNano()
			sendLatency = append(sendLatency, strconv.FormatInt(after2-before, 10)...)
			if flushed == true{
				sendLatency = append(sendLatency, " "...)
				sendLatency = append(sendLatency, strconv.FormatInt(after2-after, 10)...)
			} else {
				sendLatency = append(sendLatency, " "...)
				sendLatency = append(sendLatency, strconv.FormatInt(0, 10)...)
			}
			sendLatency = append(sendLatency, "\n"...)
			sendLen++
			if sendLen == 50000 {
				ioutil.WriteFile("flushTest", sendLatency, 0777)
			}*/
		// resend messagePump
		case msg, ok := <-resendMsgChan:
			if !ok {
				goto exit
			}
			err = subChannel.ResendMessage(msg.clientID, msg.ID, msgTimeout)
			if err != nil {
				err = protocol.NewClientErr(err, "E_RESEND_FAILED",
					fmt.Sprintf("Resend %s failed %s", msg.ID, err.Error()))
				goto exit
			}
			client.ResendingMessage()
			if p.ctx.nsqd.getOpts().Verbose {
				p.ctx.nsqd.logf("PROTOCOL(V2): [%s] Resend Client Message %d", client, msg.SeqID)
			}
			err = p.SendMessage(client, msg, buf)
			if err != nil {
				goto exit
			}
			flushed = false

		// jarry: multicast for sending null
		case <-lastSendChan:
			nullMsg := NewNullMessage(atomic.LoadUint64(&client.SeqID))
			nullSentCount++
			lastSendTimer.Reset(lastSendTimeout)
			err = p.SendNullMessage(client, nullMsg, buf)
			if err != nil {
				goto exit
			}
			flushed = false
		case <-client.ExitChan:
			goto exit
		}
		if multicastFlag && client.Channel != nil {
			client.Channel.processInFlightQueue(time.Now().UnixNano())
		}
	}

exit:
	p.ctx.nsqd.logf("PROTOCOL(V2): [%s] exiting messagePump", client)
	heartbeatTicker.Stop()
	outputBufferTicker.Stop()
	if lastSendTimer != nil && !lastSendTimer.Stop() {
		<-lastSendTimer.C
	}
	if err != nil {
		p.ctx.nsqd.logf("PROTOCOL(V2): [%s] messagePump error - %s", client, err)
	}
}

func (p *protocolV2) IDENTIFY(client *clientV2, params [][]byte) ([]byte, error) {
	var err error

	if atomic.LoadInt32(&client.State) != stateInit {
		return nil, protocol.NewFatalClientErr(nil, "E_INVALID", "cannot IDENTIFY in current state")
	}

	//bodyLen, err := readLen(client.Reader, client.lenSlice)
	bodyLen, err := readLenMsg(client.TReader, client.lenSlice)
	if err != nil {
		return nil, protocol.NewFatalClientErr(err, "E_BAD_BODY", "IDENTIFY failed to read body size")
	}

	if int64(bodyLen) > p.ctx.nsqd.getOpts().MaxBodySize {
		return nil, protocol.NewFatalClientErr(nil, "E_BAD_BODY",
			fmt.Sprintf("IDENTIFY body too big %d > %d", bodyLen, p.ctx.nsqd.getOpts().MaxBodySize))
	}

	if bodyLen <= 0 {
		return nil, protocol.NewFatalClientErr(nil, "E_BAD_BODY",
			fmt.Sprintf("IDENTIFY invalid body size %d", bodyLen))
	}

	body := make([]byte, bodyLen)
	//_, err = io.ReadFull(client.Reader, body)
	_, err = client.TReader.ReadFull(body)
	if err != nil {
		return nil, protocol.NewFatalClientErr(err, "E_BAD_BODY", "IDENTIFY failed to read body")
	}

	// body is a json structure with producer information
	var identifyData identifyDataV2
	err = json.Unmarshal(body, &identifyData)
	if err != nil {
		return nil, protocol.NewFatalClientErr(err, "E_BAD_BODY", "IDENTIFY failed to decode JSON body")
	}

	if p.ctx.nsqd.getOpts().Verbose {
		p.ctx.nsqd.logf("PROTOCOL(V2): [%s] %+v", client, identifyData)
	}

	err = client.Identify(identifyData)
	if err != nil {
		return nil, protocol.NewFatalClientErr(err, "E_BAD_BODY", "IDENTIFY "+err.Error())
	}

	// bail out early if we're not negotiating features
	if !identifyData.FeatureNegotiation {
		return okBytes, nil
	}

	tlsv1 := p.ctx.nsqd.tlsConfig != nil && identifyData.TLSv1
	deflate := p.ctx.nsqd.getOpts().DeflateEnabled && identifyData.Deflate
	deflateLevel := 0
	if deflate {
		if identifyData.DeflateLevel <= 0 {
			deflateLevel = 6
		}
		deflateLevel = int(math.Min(float64(deflateLevel), float64(p.ctx.nsqd.getOpts().MaxDeflateLevel)))
	}
	snappy := p.ctx.nsqd.getOpts().SnappyEnabled && identifyData.Snappy

	if deflate && snappy {
		return nil, protocol.NewFatalClientErr(nil, "E_IDENTIFY_FAILED", "cannot enable both deflate and snappy compression")
	}

	resp, err := json.Marshal(struct {
		MaxRdyCount         int64  `json:"max_rdy_count"`
		Version             string `json:"version"`
		MaxMsgTimeout       int64  `json:"max_msg_timeout"`
		MsgTimeout          int64  `json:"msg_timeout"`
		TLSv1               bool   `json:"tls_v1"`
		Deflate             bool   `json:"deflate"`
		DeflateLevel        int    `json:"deflate_level"`
		MaxDeflateLevel     int    `json:"max_deflate_level"`
		Snappy              bool   `json:"snappy"`
		SampleRate          int32  `json:"sample_rate"`
		AuthRequired        bool   `json:"auth_required"`
		OutputBufferSize    int    `json:"output_buffer_size"`
		OutputBufferTimeout int64  `json:"output_buffer_timeout"`
	}{
		MaxRdyCount:         p.ctx.nsqd.getOpts().MaxRdyCount,
		Version:             version.Binary,
		MaxMsgTimeout:       int64(p.ctx.nsqd.getOpts().MaxMsgTimeout / time.Millisecond),
		MsgTimeout:          int64(client.MsgTimeout / time.Millisecond),
		TLSv1:               tlsv1,
		Deflate:             deflate,
		DeflateLevel:        deflateLevel,
		MaxDeflateLevel:     p.ctx.nsqd.getOpts().MaxDeflateLevel,
		Snappy:              snappy,
		SampleRate:          client.SampleRate,
		AuthRequired:        p.ctx.nsqd.IsAuthEnabled(),
		OutputBufferSize:    client.OutputBufferSize,
		OutputBufferTimeout: int64(client.OutputBufferTimeout / time.Millisecond),
	})
	if err != nil {
		return nil, protocol.NewFatalClientErr(err, "E_IDENTIFY_FAILED", "IDENTIFY failed "+err.Error())
	}

	err = p.Send(client, frameTypeResponse, resp)
	if err != nil {
		return nil, protocol.NewFatalClientErr(err, "E_IDENTIFY_FAILED", "IDENTIFY failed "+err.Error())
	}

	if tlsv1 {
		p.ctx.nsqd.logf("PROTOCOL(V2): [%s] upgrading connection to TLS", client)
		err = client.UpgradeTLS()
		if err != nil {
			return nil, protocol.NewFatalClientErr(err, "E_IDENTIFY_FAILED", "IDENTIFY failed "+err.Error())
		}

		err = p.Send(client, frameTypeResponse, okBytes)
		if err != nil {
			return nil, protocol.NewFatalClientErr(err, "E_IDENTIFY_FAILED", "IDENTIFY failed "+err.Error())
		}
	}

	if snappy {
		p.ctx.nsqd.logf("PROTOCOL(V2): [%s] upgrading connection to snappy", client)
		err = client.UpgradeSnappy()
		if err != nil {
			return nil, protocol.NewFatalClientErr(err, "E_IDENTIFY_FAILED", "IDENTIFY failed "+err.Error())
		}

		err = p.Send(client, frameTypeResponse, okBytes)
		if err != nil {
			return nil, protocol.NewFatalClientErr(err, "E_IDENTIFY_FAILED", "IDENTIFY failed "+err.Error())
		}
	}

	if deflate {
		p.ctx.nsqd.logf("PROTOCOL(V2): [%s] upgrading connection to deflate", client)
		err = client.UpgradeDeflate(deflateLevel)
		if err != nil {
			return nil, protocol.NewFatalClientErr(err, "E_IDENTIFY_FAILED", "IDENTIFY failed "+err.Error())
		}

		err = p.Send(client, frameTypeResponse, okBytes)
		if err != nil {
			return nil, protocol.NewFatalClientErr(err, "E_IDENTIFY_FAILED", "IDENTIFY failed "+err.Error())
		}
	}

	return nil, nil
}

func (p *protocolV2) AUTH(client *clientV2, params [][]byte) ([]byte, error) {
	if atomic.LoadInt32(&client.State) != stateInit {
		return nil, protocol.NewFatalClientErr(nil, "E_INVALID", "cannot AUTH in current state")
	}

	if len(params) != 1 {
		return nil, protocol.NewFatalClientErr(nil, "E_INVALID", "AUTH invalid number of parameters")
	}

	//bodyLen, err := readLen(client.Reader, client.lenSlice)
	bodyLen, err := readLenMsg(client.TReader, client.lenSlice)
	if err != nil {
		return nil, protocol.NewFatalClientErr(err, "E_BAD_BODY", "AUTH failed to read body size")
	}

	if int64(bodyLen) > p.ctx.nsqd.getOpts().MaxBodySize {
		return nil, protocol.NewFatalClientErr(nil, "E_BAD_BODY",
			fmt.Sprintf("AUTH body too big %d > %d", bodyLen, p.ctx.nsqd.getOpts().MaxBodySize))
	}

	if bodyLen <= 0 {
		return nil, protocol.NewFatalClientErr(nil, "E_BAD_BODY",
			fmt.Sprintf("AUTH invalid body size %d", bodyLen))
	}

	body := make([]byte, bodyLen)
	//_, err = io.ReadFull(client.Reader, body)
	_, err = client.TReader.ReadFull(body)
	if err != nil {
		return nil, protocol.NewFatalClientErr(err, "E_BAD_BODY", "AUTH failed to read body")
	}

	if client.HasAuthorizations() {
		return nil, protocol.NewFatalClientErr(nil, "E_INVALID", "AUTH Already set")
	}

	if !client.ctx.nsqd.IsAuthEnabled() {
		return nil, protocol.NewFatalClientErr(err, "E_AUTH_DISABLED", "AUTH Disabled")
	}

	if err := client.Auth(string(body)); err != nil {
		// we don't want to leak errors contacting the auth server to untrusted clients
		p.ctx.nsqd.logf("PROTOCOL(V2): [%s] Auth Failed %s", client, err)
		return nil, protocol.NewFatalClientErr(err, "E_AUTH_FAILED", "AUTH failed")
	}

	if !client.HasAuthorizations() {
		return nil, protocol.NewFatalClientErr(nil, "E_UNAUTHORIZED", "AUTH No authorizations found")
	}

	resp, err := json.Marshal(struct {
		Identity        string `json:"identity"`
		IdentityURL     string `json:"identity_url"`
		PermissionCount int    `json:"permission_count"`
	}{
		Identity:        client.AuthState.Identity,
		IdentityURL:     client.AuthState.IdentityURL,
		PermissionCount: len(client.AuthState.Authorizations),
	})
	if err != nil {
		return nil, protocol.NewFatalClientErr(err, "E_AUTH_ERROR", "AUTH error "+err.Error())
	}

	err = p.Send(client, frameTypeResponse, resp)
	if err != nil {
		return nil, protocol.NewFatalClientErr(err, "E_AUTH_ERROR", "AUTH error "+err.Error())
	}

	return nil, nil

}

func (p *protocolV2) CheckAuth(client *clientV2, cmd, topicName, channelName string) error {
	// if auth is enabled, the client must have authorized already
	// compare topic/channel against cached authorization data (refetching if expired)
	if client.ctx.nsqd.IsAuthEnabled() {
		if !client.HasAuthorizations() {
			return protocol.NewFatalClientErr(nil, "E_AUTH_FIRST",
				fmt.Sprintf("AUTH required before %s", cmd))
		}
		ok, err := client.IsAuthorized(topicName, channelName)
		if err != nil {
			// we don't want to leak errors contacting the auth server to untrusted clients
			p.ctx.nsqd.logf("PROTOCOL(V2): [%s] Auth Failed %s", client, err)
			return protocol.NewFatalClientErr(nil, "E_AUTH_FAILED", "AUTH failed")
		}
		if !ok {
			return protocol.NewFatalClientErr(nil, "E_UNAUTHORIZED",
				fmt.Sprintf("AUTH failed for %s on %q %q", cmd, topicName, channelName))
		}
	}
	return nil
}

// hanlding multicast reliability
func (p *protocolV2) NACK(client *clientV2, params [][]byte) ([]byte, error) {
	state := atomic.LoadInt32(&client.State)
	if state != stateSubscribed && state != stateClosing {
		return nil, protocol.NewFatalClientErr(nil, "E_INVALID", "cannot FIN in current state")
	}

	if client.connType != ConnTypeMulticast {
		return nil, protocol.NewFatalClientErr(nil, "E_INVALID", "Receive NACK from a non-multicast client")
	}

	if len(params) < 3 {
		return nil, protocol.NewFatalClientErr(nil, "E_INVALID", "NACK insufficient number of params")
	}
	seqID, err := strconv.Atoi(string(params[1]))
	if p.ctx.nsqd.getOpts().Verbose {
		p.ctx.nsqd.logf("PROTOCOL(V2): [%s] Recieve NACK Message of %d", client, seqID)
	}
	if err != nil {
		return nil, protocol.NewFatalClientErr(nil, "E_INVALID", err.Error())
	}

	ipAddr := string(params[2])
	msg, resend := client.Channel.resendOrNot(uint64(seqID), ipAddr) // need to resend or not
	if msg != nil && resend {
		client.Channel.resendMsgChan <- msg
	}
	if p.ctx.nsqd.getOpts().Verbose {
		if msg == nil && !resend {
			p.ctx.nsqd.logf("PROTOCOL(V2): [%s] Message %d is not resend, not In-Flight message", client, seqID)
		}
	}
	return nil, nil
}

func (p *protocolV2) SUB(client *clientV2, params [][]byte) ([]byte, error) {
	if atomic.LoadInt32(&client.State) != stateInit {
		return nil, protocol.NewFatalClientErr(nil, "E_INVALID", "cannot SUB in current state")
	}

	if client.HeartbeatInterval <= 0 {
		return nil, protocol.NewFatalClientErr(nil, "E_INVALID", "cannot SUB with heartbeats disabled")
	}

	if len(params) < 3 {
		return nil, protocol.NewFatalClientErr(nil, "E_INVALID", "SUB insufficient number of parameters")
	}

	topicName := string(params[1])
	if !protocol.IsValidTopicName(topicName) {
		return nil, protocol.NewFatalClientErr(nil, "E_BAD_TOPIC",
			fmt.Sprintf("SUB topic name %q is not valid", topicName))
	}

	channelName := string(params[2])
	if !protocol.IsValidChannelName(channelName) {
		return nil, protocol.NewFatalClientErr(nil, "E_BAD_CHANNEL",
			fmt.Sprintf("SUB channel name %q is not valid", channelName))
	}

	if err := p.CheckAuth(client, "SUB", topicName, channelName); err != nil {
		return nil, err
	}

	topic := p.ctx.nsqd.GetTopic(topicName)
	channel := topic.GetChannel(channelName)
	channel.AddClient(client.ID, client)
	//RTM.dtb
	topic.subClients[client.ID]=client
	client.subTopic = topic

	atomic.StoreInt32(&client.State, stateSubscribed)
	client.Channel = channel
	// update message pump
	client.SubEventChan <- channel

	return okBytes, nil
}

func (p *protocolV2) RDY(client *clientV2, params [][]byte) ([]byte, error) {
	state := atomic.LoadInt32(&client.State)

	if state == stateClosing {
		// just ignore ready changes on a closing channel
		p.ctx.nsqd.logf(
			"PROTOCOL(V2): [%s] ignoring RDY after CLS in state ClientStateV2Closing",
			client)
		return nil, nil
	}

	if state != stateSubscribed {
		return nil, protocol.NewFatalClientErr(nil, "E_INVALID", "cannot RDY in current state")
	}

	count := int64(1)
	if len(params) > 1 {
		b10, err := protocol.ByteToBase10(params[1])
		if err != nil {
			return nil, protocol.NewFatalClientErr(err, "E_INVALID",
				fmt.Sprintf("RDY could not parse count %s", params[1]))
		}
		count = int64(b10)
	}

	if count < 0 || count > p.ctx.nsqd.getOpts().MaxRdyCount {
		// this needs to be a fatal error otherwise clients would have
		// inconsistent state
		return nil, protocol.NewFatalClientErr(nil, "E_INVALID",
			fmt.Sprintf("RDY count %d out of range 0-%d", count, p.ctx.nsqd.getOpts().MaxRdyCount))
	}

	client.SetReadyCount(count)

	return nil, nil
}

func (p *protocolV2) FIN(client *clientV2, params [][]byte) ([]byte, error) {
	state := atomic.LoadInt32(&client.State)
	if state != stateSubscribed && state != stateClosing {
		return nil, protocol.NewFatalClientErr(nil, "E_INVALID", "cannot FIN in current state")
	}

	if len(params) < 2 {
		return nil, protocol.NewFatalClientErr(nil, "E_INVALID", "FIN insufficient number of params")
	}

	id, err := getMessageID(params[1])
	if err != nil {
		return nil, protocol.NewFatalClientErr(nil, "E_INVALID", err.Error())
	}

	err = client.Channel.FinishMessage(client.ID, *id)
	if err != nil {
		return nil, protocol.NewClientErr(err, "E_FIN_FAILED",
			fmt.Sprintf("FIN %s failed %s", *id, err.Error()))
	}

	client.FinishedMessage()

	return nil, nil
}

func (p *protocolV2) REQ(client *clientV2, params [][]byte) ([]byte, error) {

	state := atomic.LoadInt32(&client.State)
	if state != stateSubscribed && state != stateClosing {
		return nil, protocol.NewFatalClientErr(nil, "E_INVALID", "cannot REQ in current state")
	}

	if len(params) < 3 {
		return nil, protocol.NewFatalClientErr(nil, "E_INVALID", "REQ insufficient number of params")
	}

	id, err := getMessageID(params[1])
	if err != nil {
		return nil, protocol.NewFatalClientErr(nil, "E_INVALID", err.Error())
	}

	timeoutMs, err := protocol.ByteToBase10(params[2])
	if err != nil {
		return nil, protocol.NewFatalClientErr(err, "E_INVALID",
			fmt.Sprintf("REQ could not parse timeout %s", params[2]))
	}
	timeoutDuration := time.Duration(timeoutMs) * time.Millisecond

	if timeoutDuration < 0 || timeoutDuration > p.ctx.nsqd.getOpts().MaxReqTimeout {
		return nil, protocol.NewFatalClientErr(nil, "E_INVALID",
			fmt.Sprintf("REQ timeout %d out of range 0-%d", timeoutDuration, p.ctx.nsqd.getOpts().MaxReqTimeout))
	}

	err = client.Channel.RequeueMessage(client.ID, *id, timeoutDuration)
	if err != nil {
		return nil, protocol.NewClientErr(err, "E_REQ_FAILED",
			fmt.Sprintf("REQ %s failed %s", *id, err.Error()))
	}

	client.RequeuedMessage()

	return nil, nil
}

func (p *protocolV2) CLS(client *clientV2, params [][]byte) ([]byte, error) {
	if atomic.LoadInt32(&client.State) != stateSubscribed {
		return nil, protocol.NewFatalClientErr(nil, "E_INVALID", "cannot CLS in current state")
	}

	client.StartClose()

	return []byte("CLOSE_WAIT"), nil
}

func (p *protocolV2) NOP(client *clientV2, params [][]byte) ([]byte, error) {
	return nil, nil
}

func (p *protocolV2) s1Delay(client *clientV2, params[][]byte) {
	for i:=0; client.s1Index < client.sNum && i < len(params); client.s1Index++ {
		client.s1Times[client.s1Index],_ = strconv.ParseInt(string(params[i]), 10, 64)
		i++
	}
	//fmt.Printf("~~~~~~~~~~receive %v msgs\n", client.s1Index)
}
func ByteSlice2String(bs []byte) string {
	return *(*string)(unsafe.Pointer(&bs))
}
func Bytes2String(bytes []byte) (s string) {
	str := (*reflect.StringHeader)(unsafe.Pointer(&s))
	str.Data = uintptr(unsafe.Pointer(&bytes[0]))
	str.Len = len(bytes)

	return s
}
func (p *protocolV2) PUB(client *clientV2, params []byte) ([]byte, error) {
	var err error

	if len(params) < 2 {
		return nil, protocol.NewFatalClientErr(nil, "E_INVALID", "PUB insufficient number of parameters")
	}

	//topicName := string(params[1])
	topicName := client.topicName
	if len(topicName) <= 0 {
		topicName = string(bytes.TrimSuffix(params, separatorBytes))
		client.topicName = topicName
	}
	if !protocol.IsValidTopicName(topicName) {
		return nil, protocol.NewFatalClientErr(nil, "E_BAD_TOPIC",
			fmt.Sprintf("PUB topic name %q is not valid", topicName))
	}

	//bodyLen, err := readLen(client.Reader, client.lenSlice)
	bodyLen, err := readLenMsg(client.TReader, client.lenSlice)
	if err != nil {
		return nil, protocol.NewFatalClientErr(err, "E_BAD_MESSAGE", "PUB failed to read message body size")
	}

	if bodyLen <= 0 {
		return nil, protocol.NewFatalClientErr(nil, "E_BAD_MESSAGE",
			fmt.Sprintf("PUB invalid message body size %d", bodyLen))
	}

	if int64(bodyLen) > p.ctx.nsqd.getOpts().MaxMsgSize {
		return nil, protocol.NewFatalClientErr(nil, "E_BAD_MESSAGE",
			fmt.Sprintf("PUB message too big %d > %d", bodyLen, p.ctx.nsqd.getOpts().MaxMsgSize))
	}

	//messageBody := make([]byte, bodyLen)
	messageBody := client.msgBuffer[client.msgBufferIndex]
	//_, err = io.ReadFull(client.Reader, messageBody)
	_, err = client.TReader.ReadFull(messageBody)
	if err != nil {
		return nil, protocol.NewFatalClientErr(err, "E_BAD_MESSAGE", "PUB failed to read message body")
	}

	if err := p.CheckAuth(client, "PUB", topicName, ""); err != nil {
		return nil, err
	}


	topic := p.ctx.nsqd.GetTopic(topicName)
	if topic.bucketRate <= 0 {
		topic.Lock()
		if topic.bucketSize > 0 {
			topic.Unlock()
			goto noset
		}
		for i, value := range bytes.Split(messageBody[80:99], []byte{'x'}) {
                        if i == 1 {
                                rx, _ := strconv.Atoi(string(value))
				topic.bucketRate = float64(rx)
                        }
                        if i == 2 {
                                topic.bucketSize, _ = strconv.Atoi(string(value))
				break
                        }
                }
		topic.bucket = rate.NewLimiter(rate.Limit(topic.bucketRate), topic.bucketSize)
		if topic.bucketSize/6 > 10 {
                	topic.quantum = 10
        	} else {
                	topic.quantum = topic.bucketSize/6
			if topic.quantum < 1 {
				topic.quantum = 1
			}
        	}
		//topic.quantum = 1
		topic.Tmpq=make([]*Message, topic.quantum)
		topic.Tmpindex = 0
		topic.Unlock()
		go func() {
                        time.Sleep(30*time.Second)
                        fmt.Printf("New topic %v %v %v\n", topicName, topic.bucketRate, topic.bucketSize)
			if strings.HasPrefix(topic.name, "p") || strings.HasPrefix(topic.name, "tb") || strings.HasPrefix(topic.name, "e") {
				return
			}
			if len(topic.pubClients) == 0{
				return
			}
			runtime.GC()
			time.Sleep(3*time.Second)
			for _, p := range topic.pubClients {
                              p.profile = true
                              p.s1Index = 0
                              p.s2Index = 0
                              p.s3Index = 0
                              p.profileChan <- topic.name
                        }
			fmt.Printf("start measure\n")
			time.Sleep(70*time.Second)
			wait := true
			for wait {
				wait = false
                        	for _,p:= range topic.pubClients {
					if p.s1Index < p.sNum {
						time.Sleep(5*time.Second)
						wait=true
						break
					}
                                	//p.profile = false
                        	}
			}
			topic.ctx.nsqd.topicChan <- topic
			return
			/******************************/
                                                keys := make([]int, len(topic.pubClients))
                                                kIndex := 0
                                                for k, _ := range topic.pubClients {
                                                        keys[kIndex]=k
                                                        kIndex++
                                                }
                                                sort.Slice(keys, func(i, j int) bool {return keys[i] < keys[j]})
                                                delays1:=[]int64{}
                                                begin:=(topic.pubClients[keys[0]].sNum/6)
                                                end:=(topic.pubClients[keys[0]].sNum/6)*5
                                                for _, k:= range keys {
                                                        c := topic.pubClients[k]
							if c.s1Index < end {
								fmt.Printf("!!!!!!! not enough samples %v\n", c.s1Index)
							}
                                                        for i:=begin; i< end; i++ {
                                                                delays1 = append(delays1, c.s1Times[i])
                                                        }
                                                }
						delays2:=[]int64{}
                                                for _, k:= range keys {
                                                        c := topic.pubClients[k]
                                                        for i:=begin; i< end; i++ {
                                                                delays2 = append(delays2, c.s2Times[i])
                                                        }
                                                }

                                                delays3 :=[]int64{}
                                                for _, k:= range keys {
                                                        c := topic.pubClients[k]
                                                        for i:=begin; i< end; i++ {
                                                                delays3 = append(delays3, c.s3Times[i])
                                                        }
                                                }

                                                s := make([]int64, len(delays1))
                                                for i:=0; i<len(s); i++ {
                                                        s[i]= delays1[i]/2+ delays2[i]+delays3[i]/2
                                                }
                                                sort.Slice(s, func(i,j int) bool {return s[i]<s[j]})
                                                fmt.Printf("total=%v\n", len(s))
                                                fmt.Printf("50=%v, 95=%v, 99=%v\n", s[int(float64(len(s))*0.5)], s[int(float64(len(s))*0.95)], s[int(float64(len(s))*0.99)])

                }()
	}
noset:
	//msg := NewMessage(<-p.ctx.nsqd.idChan, messageBody)
	msg := client.msgStr[client.msgBufferIndex]
	client.msgBufferIndex = (client.msgBufferIndex+1)%client.msgBufferLen
	msg.ID = <-p.ctx.nsqd.idChan
	msg.Body = messageBody
	msg.Timestamp = time.Now().UnixNano()
	//if strings.HasPrefix(topic.name, "0") || strings.HasPrefix(topic.name, "x") {
		//pubTime1, _ := binary.Varint(msg.Body[0:18])
		//msg.Ktimestamp = time.Unix(0, pubTime1) //Pub Timestamping
		//msg.Ktimestamp = time.Now()
		//msg.Timestamp = msg.Ktimestamp.UnixNano()
		msg.Ktimestamp = time.Unix(0, client.curTT)
	//} else {
		//msg.Ktimestamp = time.Unix(0, client.curTT)
	//}
	//RTM.dtb
	msg.senderID = client.ID
	msg.pubClient = client

	/*
	* RTM:
	* We put msgs to the corresponding MsgQ[topic], based on their topic. Then we
	* try to wake up the messagePump in Topic, except the Topic is in congestion mode.
	 */
	/*err = topic.PutMessage(msg)
	if err != nil {
		return nil, protocol.NewFatalClientErr(err, "E_PUB_FAILED", "PUB failed "+err.Error())
	}*/
	topic.clientLock.RLock()
	check := topic.clients[client.ID]
	topic.clientLock.RUnlock()
	if check == nil {
		//client.MsgQ[topicName] = make(chan *Message, p.ctx.nsqd.getOpts().MemQueueSize)
		topic.clientLock.Lock()
		topic.clients[client.ID] = client
		topic.clientLock.Unlock()
		for i, value := range bytes.Split(msg.Body[30:40], []byte{'\n'}) {
			if i == 0 {
				client.pubID = string(value)
				break
			}
		}
		ID,_:=strconv.Atoi(client.pubID)
		for i, value := range bytes.Split(msg.Body[130:140], []byte{'\n'}){
                        if i == 0 {
                                 client.sNum, _ = strconv.Atoi(string(value))
                                 break
                        }
                }
		if strings.HasPrefix(topicName, "p") {
			client.CongestChan <- topicName
		}
		if !strings.HasPrefix(topicName, "tb"){
			client.arrNums = client.sNum+1
		}

		if bytes.HasPrefix(msg.Body[24:], []byte{'0'}) {
			topic.clientLock.Lock()
			topic.pubClients[ID]=client
			topic.clientLock.Unlock()
			client.s1Times=make([]int64, client.sNum)
			client.s1Index= client.sNum + 1
			client.s2Times=make([]int64, client.sNum)
			client.s2Index= client.sNum + 1
			client.s3Times=make([]int64, client.sNum)
			client.s3Index= client.sNum + 1
		}
		topic.clientLock.Lock()
		topic.allPubClients[ID]=client
		topic.clientLock.Unlock()
	}


	//RTM.dtb test only
	curTime := time.Now()
	X := curTime.UnixNano()
	if bytes.HasPrefix(msg.Body[24:], []byte{'0'}) {
		//lenByte := []byte(strconv.Itoa(len(topic.memMsgChan)))
		//lenByte = append(lenByte, "\n"...)
		//copy(msg.Body[30:], lenByte)
		//binary.PutVarint(msg.Body[40:], time.Now().UnixNano())
		binary.PutVarint(msg.Body[40:], X)
	}
	if client.profile && client.arrNums < client.sNum{
		//client.arrTimes[client.arrNums]=curTime
		client.arrTimes[client.arrNums]=msg.Ktimestamp
		//thenTT, _:= binary.Varint(msg.Body[0:18])
		//client.arrTimes[client.arrNums] = time.Unix(0, thenTT)
		client.arrNums++
	}

	topic.memMsgChan <- msg
	atomic.AddUint64(&topic.messageCount, 1)

	topic.congestLock.RLock()
	if !topic.congestion {
		select {
		case topic.pullChan <- 1:
		default:
		}
	}
	topic.congestLock.RUnlock()

	/*
	* RTM:
	* if we find the current operation makes the queue length exceed its high-watermark (the
	* current configuration is 0.6*the capacity of the queue), we send a congestion signal back to
	* the producer. Once producer receives this congestion signal, it should stop sending msgs of
	* the corresponding topic.
	 */
	if !topic.congest &&
		len(topic.memMsgChan) > int(float64(cap(topic.memMsgChan))*0.6) {
		//copy(msg.Body[50:], []byte("congest_sent"))
		//binary.PutVarint(msg.Body[70:], time.Now().UnixNano())
		topic.congest = true
		fmt.Printf("~~~~~~~~~~!!!!!! Congest: %v %v\n", topic.bucket.Limit(), topic.bucket.Burst())
		topic.clientLock.RLock()
		for _, v := range topic.clients {
			select {
			case v.CongestChan <- topicName:
			default:
			}
		}
		topic.clientLock.RUnlock()
	
		//RTM: trigger topic to consume pending msgs
		select {
                	case topic.pullChan <- 1:
                	default:
                }
	}
	/*if bytes.HasPrefix(msg.Body[24:], []byte{'0'}) {
		msg.Timestamp = time.Now().UnixNano()
	}*/
	return okBytes, nil
}

func (p *protocolV2) MPUB(client *clientV2, params [][]byte) ([]byte, error) {
	var err error

	if len(params) < 2 {
		return nil, protocol.NewFatalClientErr(nil, "E_INVALID", "MPUB insufficient number of parameters")
	}

	topicName := string(params[1])
	if !protocol.IsValidTopicName(topicName) {
		return nil, protocol.NewFatalClientErr(nil, "E_BAD_TOPIC",
			fmt.Sprintf("E_BAD_TOPIC MPUB topic name %q is not valid", topicName))
	}

	bodyLen, err := readLen(client.Reader, client.lenSlice)
	if err != nil {
		return nil, protocol.NewFatalClientErr(err, "E_BAD_BODY", "MPUB failed to read body size")
	}

	if bodyLen <= 0 {
		return nil, protocol.NewFatalClientErr(nil, "E_BAD_BODY",
			fmt.Sprintf("MPUB invalid body size %d", bodyLen))
	}

	if int64(bodyLen) > p.ctx.nsqd.getOpts().MaxBodySize {
		return nil, protocol.NewFatalClientErr(nil, "E_BAD_BODY",
			fmt.Sprintf("MPUB body too big %d > %d", bodyLen, p.ctx.nsqd.getOpts().MaxBodySize))
	}

	messages, err := readMPUB(client.Reader, client.lenSlice, p.ctx.nsqd.idChan,
		p.ctx.nsqd.getOpts().MaxMsgSize)
	if err != nil {
		return nil, err
	}

	if err := p.CheckAuth(client, "MPUB", topicName, ""); err != nil {
		return nil, err
	}

	topic := p.ctx.nsqd.GetTopic(topicName)

	// if we've made it this far we've validated all the input,
	// the only possible error is that the topic is exiting during
	// this next call (and no messages will be queued in that case)
	err = topic.PutMessages(messages)
	if err != nil {
		return nil, protocol.NewFatalClientErr(err, "E_MPUB_FAILED", "MPUB failed "+err.Error())
	}

	return okBytes, nil
}

func (p *protocolV2) DPUB(client *clientV2, params [][]byte) ([]byte, error) {
	var err error

	if len(params) < 3 {
		return nil, protocol.NewFatalClientErr(nil, "E_INVALID", "DPUB insufficient number of parameters")
	}

	topicName := string(params[1])
	if !protocol.IsValidTopicName(topicName) {
		return nil, protocol.NewFatalClientErr(nil, "E_BAD_TOPIC",
			fmt.Sprintf("DPUB topic name %q is not valid", topicName))
	}

	timeoutMs, err := protocol.ByteToBase10(params[2])
	if err != nil {
		return nil, protocol.NewFatalClientErr(err, "E_INVALID",
			fmt.Sprintf("DPUB could not parse timeout %s", params[2]))
	}
	timeoutDuration := time.Duration(timeoutMs) * time.Millisecond

	if timeoutDuration < 0 || timeoutDuration > p.ctx.nsqd.getOpts().MaxReqTimeout {
		return nil, protocol.NewFatalClientErr(nil, "E_INVALID",
			fmt.Sprintf("DPUB timeout %d out of range 0-%d",
				timeoutMs, p.ctx.nsqd.getOpts().MaxReqTimeout/time.Millisecond))
	}

	bodyLen, err := readLen(client.Reader, client.lenSlice)
	if err != nil {
		return nil, protocol.NewFatalClientErr(err, "E_BAD_MESSAGE", "DPUB failed to read message body size")
	}

	if bodyLen <= 0 {
		return nil, protocol.NewFatalClientErr(nil, "E_BAD_MESSAGE",
			fmt.Sprintf("DPUB invalid message body size %d", bodyLen))
	}

	if int64(bodyLen) > p.ctx.nsqd.getOpts().MaxMsgSize {
		return nil, protocol.NewFatalClientErr(nil, "E_BAD_MESSAGE",
			fmt.Sprintf("DPUB message too big %d > %d", bodyLen, p.ctx.nsqd.getOpts().MaxMsgSize))
	}

	messageBody := make([]byte, bodyLen)
	_, err = io.ReadFull(client.Reader, messageBody)
	if err != nil {
		return nil, protocol.NewFatalClientErr(err, "E_BAD_MESSAGE", "DPUB failed to read message body")
	}

	if err := p.CheckAuth(client, "DPUB", topicName, ""); err != nil {
		return nil, err
	}

	topic := p.ctx.nsqd.GetTopic(topicName)
	msg := NewMessage(<-p.ctx.nsqd.idChan, messageBody)
	msg.deferred = timeoutDuration
	err = topic.PutMessage(msg)
	if err != nil {
		return nil, protocol.NewFatalClientErr(err, "E_DPUB_FAILED", "DPUB failed "+err.Error())
	}

	return okBytes, nil
}

func (p *protocolV2) TOUCH(client *clientV2, params [][]byte) ([]byte, error) {
	state := atomic.LoadInt32(&client.State)
	if state != stateSubscribed && state != stateClosing {
		return nil, protocol.NewFatalClientErr(nil, "E_INVALID", "cannot TOUCH in current state")
	}

	if len(params) < 2 {
		return nil, protocol.NewFatalClientErr(nil, "E_INVALID", "TOUCH insufficient number of params")
	}

	id, err := getMessageID(params[1])
	if err != nil {
		return nil, protocol.NewFatalClientErr(nil, "E_INVALID", err.Error())
	}

	client.writeLock.RLock()
	msgTimeout := client.MsgTimeout
	client.writeLock.RUnlock()
	err = client.Channel.TouchMessage(client.ID, *id, msgTimeout)
	if err != nil {
		return nil, protocol.NewClientErr(err, "E_TOUCH_FAILED",
			fmt.Sprintf("TOUCH %s failed %s", *id, err.Error()))
	}

	return nil, nil
}

func readMPUB(r io.Reader, tmp []byte, idChan chan MessageID, maxMessageSize int64) ([]*Message, error) {
	numMessages, err := readLen(r, tmp)
	if err != nil {
		return nil, protocol.NewFatalClientErr(err, "E_BAD_BODY", "MPUB failed to read message count")
	}

	if numMessages <= 0 {
		return nil, protocol.NewFatalClientErr(err, "E_BAD_BODY",
			fmt.Sprintf("MPUB invalid message count %d", numMessages))
	}

	messages := make([]*Message, 0, numMessages)
	for i := int32(0); i < numMessages; i++ {
		messageSize, err := readLen(r, tmp)
		if err != nil {
			return nil, protocol.NewFatalClientErr(err, "E_BAD_MESSAGE",
				fmt.Sprintf("MPUB failed to read message(%d) body size", i))
		}

		if messageSize <= 0 {
			return nil, protocol.NewFatalClientErr(nil, "E_BAD_MESSAGE",
				fmt.Sprintf("MPUB invalid message(%d) body size %d", i, messageSize))
		}

		if int64(messageSize) > maxMessageSize {
			return nil, protocol.NewFatalClientErr(nil, "E_BAD_MESSAGE",
				fmt.Sprintf("MPUB message too big %d > %d", messageSize, maxMessageSize))
		}

		msgBody := make([]byte, messageSize)
		_, err = io.ReadFull(r, msgBody)
		if err != nil {
			return nil, protocol.NewFatalClientErr(err, "E_BAD_MESSAGE", "MPUB failed to read message body")
		}

		messages = append(messages, NewMessage(<-idChan, msgBody))
	}

	return messages, nil
}

// validate and cast the bytes on the wire to a message ID
func getMessageID(p []byte) (*MessageID, error) {
	if len(p) != MsgIDLength {
		return nil, errors.New("Invalid Message ID")
	}
	return (*MessageID)(unsafe.Pointer(&p[0])), nil
}

func readLen(r io.Reader, tmp []byte) (int32, error) {
	_, err := io.ReadFull(r, tmp)
	if err != nil {
		return 0, err
	}
	return int32(binary.BigEndian.Uint32(tmp)), nil
}

func readLenMsg(r *Reader, tmp []byte) (int32, error) {
        _, err := r.ReadFull(tmp)
        if err != nil {
                return 0, err
        }
        return int32(binary.BigEndian.Uint32(tmp)), nil
}

func enforceTLSPolicy(client *clientV2, p *protocolV2, command []byte) error {
	if p.ctx.nsqd.getOpts().TLSRequired != TLSNotRequired && atomic.LoadInt32(&client.TLS) != 1 {
		return protocol.NewFatalClientErr(nil, "E_INVALID",
			fmt.Sprintf("cannot %s in current state (TLS required)", command))
	}
	return nil
}
