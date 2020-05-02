package nsqd

import (
	"bytes"
	"container/heap"
	"encoding/binary"
	"errors"
	"io/ioutil"
	"math"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	//"runtime"

	"github.com/WU-CPSL/RTM-0.1/nsqio/nsq/internal/pqueue"
	"github.com/WU-CPSL/RTM-0.1/nsqio/nsq/internal/quantile"
)

type Consumer interface {
	UnPause()
	Pause()
	Close() error
	TimedOutMessage()
	Stats() ClientStats
	Empty()
}

// Channel represents the concrete type for a NSQ channel (and also
// implements the Queue interface)
//
// There can be multiple channels per topic, each with there own unique set
// of subscribers (clients).
//
// Channels maintain all client and message metadata, orchestrating in-flight
// messages, timeouts, requeuing, etc.
type Channel struct {
	// 64bit atomic vars need to be first for proper alignment on 32bit platforms
	requeueCount uint64
	messageCount uint64
	timeoutCount uint64

	sync.RWMutex

	topicName string
	name      string
	ctx       *context

	backend BackendQueue

	memoryMsgChan chan *Message
	clientMsgChan chan *Message
	exitChan      chan int
	exitFlag      int32
	exitMutex     sync.RWMutex

	// state tracking
	clients        map[int64]Consumer
	paused         int32
	ephemeral      bool
	deleteCallback func(*Channel)
	deleter        sync.Once

	// Stats tracking
	e2eProcessingLatencyStream *quantile.Quantile

	// TODO: these can be DRYd up
	deferredMessages map[MessageID]*pqueue.Item
	deferredPQ       pqueue.PriorityQueue
	deferredMutex    sync.Mutex
	inFlightMessages map[MessageID]*Message
	inFlightPQ       inFlightPqueue
	inFlightMutex    sync.Mutex

	// multicast
	idMap          map[uint64]*Message
	resendMessages map[MessageID][]string
	resendMsgChan  chan *Message

	// stat counters
	bufferedCount int32

	//RTM.dtb
	hRTTs  []int64
	hIndex   int
	e2es   []int64
	eIndex   int
	Client   *clientV2
}

// NewChannel creates a new instance of the Channel type and returns a pointer
func NewChannel(topicName string, channelName string, ctx *context,
	deleteCallback func(*Channel)) *Channel {

	c := &Channel{
		topicName:      topicName,
		name:           channelName,
		memoryMsgChan:  make(chan *Message, ctx.nsqd.getOpts().MemQueueSize),
		clientMsgChan:  make(chan *Message),
		hRTTs:          make([]int64, 3),
		hIndex:         0,
		e2es:          make([]int64, 3),
                eIndex:         0,
		exitChan:       make(chan int),
		clients:        make(map[int64]Consumer),
		resendMsgChan:  make(chan *Message),
		deleteCallback: deleteCallback,
		ctx:            ctx,
	}
	if len(ctx.nsqd.getOpts().E2EProcessingLatencyPercentiles) > 0 {
		c.e2eProcessingLatencyStream = quantile.New(
			ctx.nsqd.getOpts().E2EProcessingLatencyWindowTime,
			ctx.nsqd.getOpts().E2EProcessingLatencyPercentiles,
		)
	}

	c.initPQ()

	if strings.HasSuffix(channelName, "#ephemeral") {
		c.ephemeral = true
		c.backend = newDummyBackendQueue()
	} else {
		// backend names, for uniqueness, automatically include the topic...
		backendName := getBackendName(topicName, channelName)
		c.backend = newDiskQueue(backendName,
			ctx.nsqd.getOpts().DataPath,
			ctx.nsqd.getOpts().MaxBytesPerFile,
			int32(minValidMsgLength),
			int32(ctx.nsqd.getOpts().MaxMsgSize)+minValidMsgLength,
			ctx.nsqd.getOpts().SyncEvery,
			ctx.nsqd.getOpts().SyncTimeout,
			ctx.nsqd.getOpts().Logger)
	}
	//RTM.dtb
	//go c.messagePump()

	c.ctx.nsqd.Notify(c)
	
	/*go func(){
		time.Sleep(3*time.Second)
		runtime.GC()
	}()*/

	return c
}

func (c *Channel) initPQ() {
	pqSize := int(math.Max(1, float64(c.ctx.nsqd.getOpts().MemQueueSize)/10))

	c.inFlightMessages = make(map[MessageID]*Message)
	c.deferredMessages = make(map[MessageID]*pqueue.Item)
	c.idMap = make(map[uint64]*Message)
	c.resendMessages = make(map[MessageID][]string)

	c.inFlightMutex.Lock()
	c.inFlightPQ = newInFlightPqueue(pqSize)
	c.inFlightMutex.Unlock()

	c.deferredMutex.Lock()
	c.deferredPQ = pqueue.New(pqSize)
	c.deferredMutex.Unlock()
}

// Exiting returns a boolean indicating if this channel is closed/exiting
func (c *Channel) Exiting() bool {
	return atomic.LoadInt32(&c.exitFlag) == 1
}

// Delete empties the channel and closes
func (c *Channel) Delete() error {
	return c.exit(true)
}

// Close cleanly closes the Channel
func (c *Channel) Close() error {
	return c.exit(false)
}

func (c *Channel) exit(deleted bool) error {
	c.exitMutex.Lock()
	defer c.exitMutex.Unlock()

	if !atomic.CompareAndSwapInt32(&c.exitFlag, 0, 1) {
		return errors.New("exiting")
	}

	if deleted {
		c.ctx.nsqd.logf("CHANNEL(%s): deleting", c.name)

		// since we are explicitly deleting a channel (not just at system exit time)
		// de-register this from the lookupd
		c.ctx.nsqd.Notify(c)
	} else {
		c.ctx.nsqd.logf("CHANNEL(%s): closing", c.name)
	}

	// this forceably closes client connections
	c.RLock()
	for _, client := range c.clients {
		client.Close()
	}
	c.RUnlock()

	close(c.exitChan)

	if deleted {
		// empty the queue (deletes the backend files, too)
		c.Empty()
		return c.backend.Delete()
	}

	// write anything leftover to disk
	c.flush()
	return c.backend.Close()
}

func (c *Channel) Empty() error {
	c.Lock()
	defer c.Unlock()

	c.initPQ()
	for _, client := range c.clients {
		client.Empty()
	}
	clientMsgChan := c.clientMsgChan
	for {
		select {
		case _, ok := <-clientMsgChan:
			if !ok {
				// c.clientMsgChan may be closed while in this loop
				// so just remove it from the select so we can make progress
				clientMsgChan = nil
			}
		case <-c.memoryMsgChan:
		default:
			goto finish
		}
	}

finish:
	return c.backend.Empty()
}

// flush persists all the messages in internal memory buffers to the backend
// it does not drain inflight/deferred because it is only called in Close()
func (c *Channel) flush() error {
	var msgBuf bytes.Buffer

	// messagePump is responsible for closing the channel it writes to
	// this will read until it's closed (exited)
	for msg := range c.clientMsgChan {
		c.ctx.nsqd.logf("CHANNEL(%s): recovered buffered message from clientMsgChan", c.name)
		writeMessageToBackend(&msgBuf, msg, c.backend)
	}

	if len(c.memoryMsgChan) > 0 || len(c.inFlightMessages) > 0 || len(c.deferredMessages) > 0 {
		c.ctx.nsqd.logf("CHANNEL(%s): flushing %d memory %d in-flight %d deferred messages to backend",
			c.name, len(c.memoryMsgChan), len(c.inFlightMessages), len(c.deferredMessages))
	}

	for {
		select {
		case msg := <-c.memoryMsgChan:
			err := writeMessageToBackend(&msgBuf, msg, c.backend)
			if err != nil {
				c.ctx.nsqd.logf("ERROR: failed to write message to backend - %s", err)
			}
		default:
			goto finish
		}
	}

finish:
	for _, msg := range c.inFlightMessages {
		err := writeMessageToBackend(&msgBuf, msg, c.backend)
		if err != nil {
			c.ctx.nsqd.logf("ERROR: failed to write message to backend - %s", err)
		}
	}

	for _, item := range c.deferredMessages {
		msg := item.Value.(*Message)
		err := writeMessageToBackend(&msgBuf, msg, c.backend)
		if err != nil {
			c.ctx.nsqd.logf("ERROR: failed to write message to backend - %s", err)
		}
	}

	return nil
}

func (c *Channel) Depth() int64 {
	return int64(len(c.memoryMsgChan)) + c.backend.Depth() + int64(atomic.LoadInt32(&c.bufferedCount))
}

func (c *Channel) Pause() error {
	return c.doPause(true)
}

func (c *Channel) UnPause() error {
	return c.doPause(false)
}

func (c *Channel) doPause(pause bool) error {
	if pause {
		atomic.StoreInt32(&c.paused, 1)
	} else {
		atomic.StoreInt32(&c.paused, 0)
	}

	c.RLock()
	for _, client := range c.clients {
		if pause {
			client.Pause()
		} else {
			client.UnPause()
		}
	}
	c.RUnlock()
	return nil
}

func (c *Channel) IsPaused() bool {
	return atomic.LoadInt32(&c.paused) == 1
}

// PutMessage writes a Message to the queue
func (c *Channel) PutMessage(m *Message) error {
	c.RLock()
	defer c.RUnlock()
	if atomic.LoadInt32(&c.exitFlag) == 1 {
		return errors.New("exiting")
	}
	err := c.put(m)
	if err != nil {
		return err
	}

	atomic.AddUint64(&c.messageCount, 1)
	return nil
}

func (c *Channel) put(m *Message) error {
	select {
	case c.memoryMsgChan <- m:
	default:
		b := bufferPoolGet()
		err := writeMessageToBackend(b, m, c.backend)
		bufferPoolPut(b)
		c.ctx.nsqd.SetHealth(err)
		if err != nil {
			c.ctx.nsqd.logf("CHANNEL(%s) ERROR: failed to write message to backend - %s",
				c.name, err)
			return err
		}
	}
	return nil
}

// TouchMessage resets the timeout for an in-flight message
func (c *Channel) TouchMessage(clientID int64, id MessageID, clientMsgTimeout time.Duration) error {
	msg, err := c.popInFlightMessage(clientID, id)
	if err != nil {
		return err
	}
	c.removeFromInFlightPQ(msg)

	newTimeout := time.Now().Add(clientMsgTimeout)
	if newTimeout.Sub(msg.deliveryTS) >=
		c.ctx.nsqd.getOpts().MaxMsgTimeout {
		// we would have gone over, set to the max
		newTimeout = msg.deliveryTS.Add(c.ctx.nsqd.getOpts().MaxMsgTimeout)
	}

	msg.pri = newTimeout.UnixNano()
	err = c.pushInFlightMessage(msg)
	if err != nil {
		return err
	}
	c.addToInFlightPQ(msg)
	return nil
}

// ResendMessage resets the timeout for an in-flight message
func (c *Channel) ResendMessage(clientID int64, id MessageID, clientMsgTimeout time.Duration) error {
	msg, err := c.popInFlightMessage(clientID, id)
	hardTimeout := c.ctx.nsqd.getOpts().MaxHardTimeout
	if err != nil {
		return err
	}
	c.removeFromInFlightPQ(msg)
	now := time.Now()
	if now.Sub(msg.deliveryTS) >= hardTimeout {
		// pass hard timeout then do not push again.
		return nil
	}
	msg.lastSendTS = now
	newTimeout := now.Add(clientMsgTimeout)
	if newTimeout.Sub(msg.deliveryTS) >= hardTimeout {
		// we would have gone over, set to the max
		newTimeout = msg.deliveryTS.Add(c.ctx.nsqd.getOpts().MaxMsgTimeout)
	}

	msg.pri = newTimeout.UnixNano()
	err = c.pushInFlightMessage(msg)
	if err != nil {
		return err
	}
	c.addToInFlightPQ(msg)
	return nil
}

// FinishMessage successfully discards an in-flight message
func (c *Channel) FinishMessage(clientID int64, id MessageID) error {
	msg, err := c.popInFlightMessage(clientID, id)
	if err != nil {
		return err
	}
	//c.removeFromInFlightPQ(msg)
	if c.e2eProcessingLatencyStream != nil {
		c.e2eProcessingLatencyStream.Insert(msg.Timestamp)
	}
	return nil
}

// RequeueMessage requeues a message based on `time.Duration`, ie:
//
// `timeoutMs` == 0 - requeue a message immediately
// `timeoutMs`  > 0 - asynchronously wait for the specified timeout
//     and requeue a message (aka "deferred requeue")
//
func (c *Channel) RequeueMessage(clientID int64, id MessageID, timeout time.Duration) error {
	// remove from inflight first
	msg, err := c.popInFlightMessage(clientID, id)
	if err != nil {
		return err
	}
	c.removeFromInFlightPQ(msg)

	if timeout == 0 {
		c.exitMutex.RLock()
		err := c.doRequeue(msg)
		c.exitMutex.RUnlock()
		return err
	}

	// deferred requeue
	return c.StartDeferredTimeout(msg, timeout)
}

// AddClient adds a client to the Channel's client list
func (c *Channel) AddClient(clientID int64, client Consumer) {
	c.Lock()
	defer c.Unlock()

	_, ok := c.clients[clientID]
	if ok {
		return
	}
	c.clients[clientID] = client
}

// RemoveClient removes a client from the Channel's client list
func (c *Channel) RemoveClient(clientID int64) {
	c.Lock()
	defer c.Unlock()

	_, ok := c.clients[clientID]
	if !ok {
		return
	}
	delete(c.clients, clientID)

	if len(c.clients) == 0 && c.ephemeral == true {
		go c.deleter.Do(func() { c.deleteCallback(c) })
	}
}

func (c *Channel) StartInFlightTimeout(msg *Message, clientID int64, timeout time.Duration) error {
	now := time.Now()
	msg.clientID = clientID
	msg.deliveryTS = now
	msg.lastSendTS = now
	msg.pri = now.Add(timeout).UnixNano()
	err := c.pushInFlightMessage(msg)
	if err != nil {
		return err
	}
	//c.addToInFlightPQ(msg)
	//RTM.dtb
        msg.sendTime = now.UnixNano()
	/*if bytes.HasPrefix(msg.Body[24:], []byte{'0'}) && c.eIndex < 30000 {
                c.e2es[c.eIndex]= time.Now().UnixNano()-msg.Timestamp
                c.eIndex++
        }*/
	if msg.pubClient.profile && bytes.HasPrefix(msg.Body[24:], []byte{'0'}) && msg.pubClient.s2Index < msg.pubClient.sNum  {
                msg.pubClient.s2Times[msg.pubClient.s2Index]= time.Now().UnixNano()-msg.Timestamp
                msg.pubClient.s2Index++
        }

	return nil
}

func (c *Channel) StartDeferredTimeout(msg *Message, timeout time.Duration) error {
	absTs := time.Now().Add(timeout).UnixNano()
	item := &pqueue.Item{Value: msg, Priority: absTs}
	err := c.pushDeferredMessage(item)
	if err != nil {
		return err
	}
	c.addToDeferredPQ(item)
	return nil
}

// doRequeue performs the low level operations to requeue a message
//
// Callers of this method need to ensure that a simultaneous exit will not occur
func (c *Channel) doRequeue(m *Message) error {
	err := c.put(m)
	if err != nil {
		return err
	}
	atomic.AddUint64(&c.requeueCount, 1)
	return nil
}

// resendMessage checks whether this message received a nack before.
// return false if do not resend the message
func (c *Channel) resendOrNot(seqID uint64, addr string) (*Message, bool) {
	c.inFlightMutex.Lock()
	defer c.inFlightMutex.Unlock()

	msg, ok := c.idMap[seqID]
	if !ok { // not in inflight
		return nil, false
	}
	m, ok := c.resendMessages[msg.ID]
	if !ok { // first time received for messageID
		c.resendMessages[msg.ID] = []string{addr}
		return msg, true
	}
	resend := false
	for _, v := range m {
		if addr == v {
			resend = true
			break
		}
	}

	if resend { // resend message
		c.resendMessages[msg.ID] = []string{addr}
	} else { // append nack addresses
		c.resendMessages[msg.ID] = append(c.resendMessages[msg.ID], addr)
	}

	return msg, resend
}

// pushInFlightMessage atomically adds a message to the in-flight dictionary
func (c *Channel) pushInFlightMessage(msg *Message) error {
	c.inFlightMutex.Lock()
	defer c.inFlightMutex.Unlock()
	_, ok := c.inFlightMessages[msg.ID]
	if ok {
		return errors.New("ID already in flight")
	}
	c.inFlightMessages[msg.ID] = msg
	c.idMap[msg.SeqID] = msg
	/*if bytes.HasPrefix(msg.Body[24:], []byte{'0'}) && c.eIndex < 30000 {
                c.e2es[c.eIndex]= time.Now().UnixNano()-msg.Timestamp
                c.eIndex++
        }*/
	return nil
}

// popInFlightMessage atomically removes a message from the in-flight dictionary
func (c *Channel) popInFlightMessage(clientID int64, id MessageID) (*Message, error) {
	c.inFlightMutex.Lock()
	defer c.inFlightMutex.Unlock()
	msg, ok := c.inFlightMessages[id]
	if !ok {
		return nil, errors.New("ID not in flight")
	}
	if msg.clientID != clientID {
		return nil, errors.New("client does not own message")
	}
	/*if bytes.HasPrefix(msg.Body[24:], []byte{'0'}) && c.hIndex < 30000 {
		c.hRTTs[c.hIndex]= time.Now().UnixNano()-msg.sendTime
		c.hIndex++
	}*/
	if msg.pubClient.profile && bytes.HasPrefix(msg.Body[24:], []byte{'0'}) && msg.pubClient.s3Index < msg.pubClient.sNum {
                msg.pubClient.s3Times[msg.pubClient.s3Index]= time.Now().UnixNano()-msg.sendTime
                msg.pubClient.s3Index++
        }
	delete(c.idMap, msg.SeqID)
	delete(c.inFlightMessages, id)

	return msg, nil
}

func (c *Channel) popResendMessages(id MessageID) {
	// jarry multicast remove resend information
	c.inFlightMutex.Lock()
	defer c.inFlightMutex.Unlock()
	_, ok := c.resendMessages[id]
	if ok {
		delete(c.resendMessages, id)
	}
}

func (c *Channel) addToInFlightPQ(msg *Message) {
	c.inFlightMutex.Lock()
	c.inFlightPQ.Push(msg)
	c.inFlightMutex.Unlock()
}

func (c *Channel) removeFromInFlightPQ(msg *Message) {
	c.inFlightMutex.Lock()
	if msg.index == -1 {
		// this item has already been popped off the pqueue
		c.inFlightMutex.Unlock()
		return
	}
	c.inFlightPQ.Remove(msg.index)
	c.inFlightMutex.Unlock()
}

func (c *Channel) pushDeferredMessage(item *pqueue.Item) error {
	c.deferredMutex.Lock()
	// TODO: these map lookups are costly
	id := item.Value.(*Message).ID
	_, ok := c.deferredMessages[id]
	if ok {
		c.deferredMutex.Unlock()
		return errors.New("ID already deferred")
	}
	c.deferredMessages[id] = item
	c.deferredMutex.Unlock()
	return nil
}

func (c *Channel) popDeferredMessage(id MessageID) (*pqueue.Item, error) {
	c.deferredMutex.Lock()
	// TODO: these map lookups are costly
	item, ok := c.deferredMessages[id]
	if !ok {
		c.deferredMutex.Unlock()
		return nil, errors.New("ID not deferred")
	}
	delete(c.deferredMessages, id)
	c.deferredMutex.Unlock()
	return item, nil
}

func (c *Channel) addToDeferredPQ(item *pqueue.Item) {
	c.deferredMutex.Lock()
	heap.Push(&c.deferredPQ, item)
	c.deferredMutex.Unlock()
}

// messagePump reads messages from either memory or backend and sends
// messages to clients over a go chan
func (c *Channel) messagePump() {

	var msg *Message
	var buf []byte
	var err error

	//yao
	var latencies []byte
	var lat []int64
	var channelLength []int64
	messagesReceived := 0

	for {
		// do an extra check for closed exit before we select on all the memory/backend/exitChan
		// this solves the case where we are closed and something else is draining clientMsgChan into
		// backend. we don't want to reverse that
		if atomic.LoadInt32(&c.exitFlag) == 1 {
			goto exit
		}
		select {
		case msg = <-c.memoryMsgChan:
			//yao
			if c.name == "0#ephemeral" {
				sentTime, _ := binary.Varint(msg.Body)
				now := time.Now().UnixNano()

				lat = append(lat, (now-sentTime)/1000, 10)
				channelLength = append(channelLength, int64(len(c.memoryMsgChan)))
				messagesReceived++
				//write to file
				if messagesReceived == 10000 {
					sum := int64(0)
					totalLength := int64(0)
					for _, thisLatency := range lat {
						sum += thisLatency
					}
					for _, thisLength := range channelLength {
						totalLength += thisLength
					}

					averageLatency := int64(sum) / int64(len(lat))
					averageLength := int64(totalLength) / int64(len(channelLength))
					x := strconv.FormatInt(averageLatency, 10)
					latencies = append(latencies, x...)
					latencies = append(latencies, "\n"...)
					x = strconv.FormatInt(averageLength, 10)
					latencies = append(latencies, x...)
					latencies = append(latencies, "\n"...)
					ioutil.WriteFile(("NSQ_OUTPUT/pump_to_client_queue"), latencies, 0777)
				}
			}
		case buf = <-c.backend.ReadChan():
			msg, err = decodeMessage(buf)
			if err != nil {
				c.ctx.nsqd.logf("ERROR: failed to decode message - %s", err)
				continue
			}
		case <-c.exitChan:
			goto exit
		}

		msg.Attempts++
		atomic.StoreInt32(&c.bufferedCount, 1)
		c.clientMsgChan <- msg
		atomic.StoreInt32(&c.bufferedCount, 0)
		// the client will call back to mark as in-flight w/ its info
	}

exit:
	c.ctx.nsqd.logf("CHANNEL(%s): closing ... messagePump", c.name)
	close(c.clientMsgChan)
}

func (c *Channel) processDeferredQueue(t int64) bool {
	c.exitMutex.RLock()
	defer c.exitMutex.RUnlock()

	if c.Exiting() {
		return false
	}

	dirty := false
	for {
		c.deferredMutex.Lock()
		item, _ := c.deferredPQ.PeekAndShift(t)
		c.deferredMutex.Unlock()

		if item == nil {
			goto exit
		}
		dirty = true

		msg := item.Value.(*Message)
		_, err := c.popDeferredMessage(msg.ID)
		if err != nil {
			goto exit
		}
		c.doRequeue(msg)
	}

exit:
	return dirty
}

func (c *Channel) processInFlightQueue(t int64) bool {

	c.exitMutex.RLock()
	defer c.exitMutex.RUnlock()

	if c.Exiting() {
		return false
	}

	dirty := false
	for {
		c.inFlightMutex.Lock()
		msg, _ := c.inFlightPQ.PeekAndShift(t)
		c.inFlightMutex.Unlock()

		if msg == nil {
			goto exit
		}
		dirty = true

		_, err := c.popInFlightMessage(msg.clientID, msg.ID)
		if err != nil {
			goto exit
		}

		atomic.AddUint64(&c.timeoutCount, 1)
		c.RLock()
		client, ok := c.clients[msg.clientID]
		c.RUnlock()
		if ok {
			client.TimedOutMessage()
		}
		if client.(*clientV2).connType == ConnTypeMulticast {
			// remove after timeout
			c.popResendMessages(msg.ID)
			c.removeFromInFlightPQ(msg)
		} else {
			// requeue after timeout
			c.doRequeue(msg)
		}
	}

exit:
	return dirty
}