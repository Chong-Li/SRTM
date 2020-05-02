package nsq

import (
	"bufio"
	"bytes"
	"compress/flate"
	"crypto/tls"
	//"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/WU-CPSL/RTM-0.1/nsqio/nsq/transport"
	"github.com/mreiferson/go-snappystream"
)

// jarry add connection type
const (
	ConnTypeDefault   = 0
	ConnTypeTCP       = 0
	ConnTypeMulticast = 1
	//RTM.dtb
	ConnTypeMTCP = 2
)

// jarry add close flags
const (
	CloseFlagUninit     = -1
	CloseFlagConnected  = 0
	CloseFlagCloseRead  = 1
	CloseFlagCloseWrite = 2
	CloseFlagClosed     = 4
)

// IdentifyResponse represents the metadata
// returned from an IDENTIFY command to nsqd
type IdentifyResponse struct {
	MaxRdyCount  int64 `json:"max_rdy_count"`
	TLSv1        bool  `json:"tls_v1"`
	Deflate      bool  `json:"deflate"`
	Snappy       bool  `json:"snappy"`
	AuthRequired bool  `json:"auth_required"`
}

// AuthResponse represents the metadata
// returned from an AUTH command to nsqd
type AuthResponse struct {
	Identity        string `json:"identity"`
	IdentityUrl     string `json:"identity_url"`
	PermissionCount int64  `json:"permission_count"`
}

type msgResponse struct {
	msg     *Message
	cmd     *Command
	success bool
	backoff bool
	nack    bool
}

type mapKey struct {
	Addr  string
	SeqID uint64
}

type mapValue struct {
	firstTS time.Time
	lastTS  time.Time
}

// Conn represents a connection to nsqd
//
// Conn exposes a set of callbacks for the
// various events that occur on a connection
// jarry
// change the connection struct and reader and writer
type Conn struct {
	// 64bit atomic vars need to be first for proper alignment on 32bit platforms
	messagesInFlight int64
	maxRdyCount      int64
	rdyCount         int64
	lastRdyCount     int64
	lastMsgTimestamp int64

	mtx sync.Mutex

	config *Config

	//conn    *net.TCPConn
	connType  int
	conn      net.Conn
	tlsConn   *tls.Conn
	addr      string
	closeFlag int32

	delegate ConnDelegate

	logger   logger
	logLvl   LogLevel
	logFmt   string
	logGuard sync.RWMutex

	r io.Reader
	w io.Writer
	// r *bufio.Reader
	// w *bufio.Writer

	cmdChan         chan *Command
	msgResponseChan chan *msgResponse
	exitChan        chan int
	drainReady      chan int

	stopper sync.Once
	wg      sync.WaitGroup

	readLoopRunning int32

	//multicast
	lastSeqID      map[string]uint64
	lostListSync   sync.Mutex
	lostMsgList    map[mapKey]*mapValue
	lostInFlight   int64
	lostCount      int64
	resendTimeout  time.Duration
	maxHardTimeout time.Duration
}

// jarry add connection type and close flag
// NewConn returns a new Conn instance
func NewConn(addr string, connType int, config *Config, delegate ConnDelegate) *Conn {
	if !config.initialized {
		panic("Config must be created with NewConfig()")
	}
	return &Conn{
		addr:      addr,
		connType:  connType,
		closeFlag: CloseFlagUninit,

		config:   config,
		delegate: delegate,

		maxRdyCount:      2500,
		lastMsgTimestamp: time.Now().UnixNano(),

		cmdChan:         make(chan *Command),
		msgResponseChan: make(chan *msgResponse),
		exitChan:        make(chan int),
		drainReady:      make(chan int),

		lastSeqID:   make(map[string]uint64),
		lostMsgList: make(map[mapKey]*mapValue),

		resendTimeout:  config.ResendTimeout,
		maxHardTimeout: config.MaxHardTimeout,
	}
}

func (c *Conn) CloseRead() error {
	atomic.AddInt32(&c.closeFlag, CloseFlagCloseRead)
	if c.connType == ConnTypeTCP || c.connType == ConnTypeMTCP {
		return c.conn.(*net.TCPConn).CloseRead()
	} else if c.connType == ConnTypeMulticast {
		if c.closeFlag == CloseFlagClosed {
			return c.conn.Close()
		}
	}
	return nil
}

func (c *Conn) CloseWrite() error {
	atomic.AddInt32(&c.closeFlag, CloseFlagCloseWrite)
	if c.connType == ConnTypeTCP || c.connType == ConnTypeMTCP {
		return c.conn.(*net.TCPConn).CloseWrite()
	} else if c.connType == ConnTypeMulticast {
		if c.closeFlag == CloseFlagClosed {
			return c.conn.Close()
		}
	}
	return nil
}

// SetLogger assigns the logger to use as well as a level.
//
// The format parameter is expected to be a printf compatible string with
// a single %s argument.  This is useful if you want to provide additional
// context to the log messages that the connection will print, the default
// is '(%s)'.
//
// The logger parameter is an interface that requires the following
// method to be implemented (such as the the stdlib log.Logger):
//
//    Output(calldepth int, s string)
//
func (c *Conn) SetLogger(l logger, lvl LogLevel, format string) {
	c.logGuard.Lock()
	defer c.logGuard.Unlock()

	c.logger = l
	c.logLvl = lvl
	c.logFmt = format
	if c.logFmt == "" {
		c.logFmt = "(%s)"
	}
}

func (c *Conn) getLogger() (logger, LogLevel, string) {
	c.logGuard.RLock()
	defer c.logGuard.RUnlock()

	return c.logger, c.logLvl, c.logFmt
}

// Connect dials and bootstraps the nsqd connection
// (including IDENTIFY) and returns the IdentifyResponse
func (c *Conn) Connect() (*IdentifyResponse, error) {
	if c.connType == ConnTypeTCP || c.connType == ConnTypeMTCP {
		dialer := &net.Dialer{
			LocalAddr: c.config.LocalAddr,
			Timeout:   c.config.DialTimeout,
		}

		conn, err := dialer.Dial("tcp", c.addr)
		if err != nil {
			return nil, err
		}
		// c.conn = conn.(*net.TCPConn)
		c.r = conn
		c.w = conn
		c.conn = conn
		c.closeFlag = CloseFlagConnected
	} else if c.connType == ConnTypeMulticast {
		uaddr, _ := net.ResolveUDPAddr("udp", c.addr)
		conn, err := transport.NewMTCConn(c.config.Intf, uaddr, uaddr, transport.MTCTypeDefault)
		if err != nil {
			return nil, err
		}
		c.r = conn
		c.w = conn
		c.conn = conn
		c.closeFlag = CloseFlagConnected
		c.log(LogLevelDebug, "[%s] create connection", c)
	}

	//c.r = bufio.NewReaderSize(conn, c.config.DefaultBufferSize)
	//c.w = bufio.NewWriterSize(conn, c.config.DefaultBufferSize)
	resp := &IdentifyResponse{}
	if c.connType == ConnTypeTCP || c.connType == ConnTypeMTCP {
		_, err := c.Write(MagicV2)
		if err != nil {
			c.Close()
			return nil, fmt.Errorf("[%s] failed to write magic - %s", c.addr, err)
		}

		resp, err = c.identify()
		if err != nil {
			return nil, err
		}

		if resp != nil && resp.AuthRequired {
			if c.config.AuthSecret == "" {
				c.log(LogLevelError, "Auth Required")
				return nil, errors.New("Auth Required")
			}
			err := c.auth(c.config.AuthSecret)
			if err != nil {
				c.log(LogLevelError, "Auth Failed %s", err)
				return nil, err
			}
		}
	} else {
		c.r = bufio.NewReaderSize(c.conn, c.config.DefaultBufferSize)
		c.w = bufio.NewWriterSize(c.conn, c.config.DefaultBufferSize)
	}

	c.wg.Add(2)
	atomic.StoreInt32(&c.readLoopRunning, 1)
	go c.readLoop()
	go c.writeLoop()
	return resp, nil
}

// Close idempotently initiates connection close
func (c *Conn) Close() error {
	atomic.StoreInt32(&c.closeFlag, 1)
	if c.conn != nil && atomic.LoadInt64(&c.messagesInFlight) == 0 && atomic.LoadInt64(&c.lostInFlight) == 0 {
		return c.CloseRead()
	}
	return nil
}

// IsClosing indicates whether or not the
// connection is currently in the processing of
// gracefully closing
func (c *Conn) IsClosing() bool {
	return atomic.LoadInt32(&c.closeFlag) == 1
}

// RDY returns the current RDY count
func (c *Conn) RDY() int64 {
	return atomic.LoadInt64(&c.rdyCount)
}

// LastRDY returns the previously set RDY count
func (c *Conn) LastRDY() int64 {
	return atomic.LoadInt64(&c.lastRdyCount)
}

// SetRDY stores the specified RDY count
func (c *Conn) SetRDY(rdy int64) {
	atomic.StoreInt64(&c.rdyCount, rdy)
	atomic.StoreInt64(&c.lastRdyCount, rdy)
}

// MaxRDY returns the nsqd negotiated maximum
// RDY count that it will accept for this connection
func (c *Conn) MaxRDY() int64 {
	return c.maxRdyCount
}

// LastMessageTime returns a time.Time representing
// the time at which the last message was received
func (c *Conn) LastMessageTime() time.Time {
	return time.Unix(0, atomic.LoadInt64(&c.lastMsgTimestamp))
}

// RemoteAddr returns the configured destination nsqd address
func (c *Conn) RemoteAddr() net.Addr {
	return c.conn.RemoteAddr()
}

// String returns the fully-qualified address
func (c *Conn) String() string {
	return c.addr+strconv.Itoa(c.connType) //RTM.dtb
}

// Read performs a deadlined read on the underlying TCP connection
func (c *Conn) Read(p []byte) (int, error) {
	c.conn.SetReadDeadline(time.Now().Add(c.config.ReadTimeout))
	return c.r.Read(p)
}

// Write performs a deadlined write on the underlying TCP connection
func (c *Conn) Write(p []byte) (int, error) {
	c.conn.SetWriteDeadline(time.Now().Add(c.config.WriteTimeout))
	return c.w.Write(p)
}

// WriteCommand is a goroutine safe method to write a Command
// to this connection, and flush.
func (c *Conn) WriteCommand(cmd *Command) error {
	c.mtx.Lock()
	/*var buf [4]byte
	var err error
	if c.connType == ConnTypeMulticast {
		if cmd.addr == "" {
			binary.BigEndian.PutUint32(buf[:], uint32(0))
			_, err = c.Write(buf[:])
			if err != nil {
				goto exit
			}
		} else {
			binary.BigEndian.PutUint32(buf[:], uint32(len(cmd.addr)+1))
			_, err = c.Write(buf[:])
			if err != nil {
				goto exit
			}

			_, err = c.Write([]byte(cmd.addr + "\n"))
			if err != nil {
				goto exit
			}
		}
	}*/
	_, err := cmd.WriteTo(c)
	if err != nil {
		goto exit
	}
	err = c.Flush()

exit:
	c.mtx.Unlock()
	if err != nil {
		c.log(LogLevelError, "IO error - %s", err)
		c.delegate.OnIOError(c, err)
	}
	return err
}

type flusher interface {
	Flush() error
}

// Flush writes all buffered data to the underlying TCP connection
func (c *Conn) Flush() error {
	if f, ok := c.w.(flusher); ok {
		return f.Flush()
	}
	return nil
}

func (c *Conn) identify() (*IdentifyResponse, error) {
	ci := make(map[string]interface{})
	ci["client_id"] = c.config.ClientID
	ci["hostname"] = c.config.Hostname
	ci["user_agent"] = c.config.UserAgent
	ci["short_id"] = c.config.ClientID // deprecated
	ci["long_id"] = c.config.Hostname  // deprecated
	ci["tls_v1"] = c.config.TlsV1
	ci["deflate"] = c.config.Deflate
	ci["deflate_level"] = c.config.DeflateLevel
	ci["snappy"] = c.config.Snappy
	ci["feature_negotiation"] = true
	if c.config.HeartbeatInterval == -1 {
		ci["heartbeat_interval"] = -1
	} else {
		ci["heartbeat_interval"] = int64(c.config.HeartbeatInterval / time.Millisecond)
	}
	ci["sample_rate"] = c.config.SampleRate
	ci["output_buffer_size"] = c.config.OutputBufferSize
	if c.config.OutputBufferTimeout == -1 {
		ci["output_buffer_timeout"] = -1
	} else {
		ci["output_buffer_timeout"] = int64(c.config.OutputBufferTimeout / time.Millisecond)
	}
	ci["msg_timeout"] = int64(c.config.MsgTimeout / time.Millisecond)
	cmd, err := Identify(ci)
	if err != nil {
		return nil, ErrIdentify{err.Error()}
	}

	err = c.WriteCommand(cmd)
	if err != nil {
		return nil, ErrIdentify{err.Error()}
	}

	frameType, data, err := ReadUnpackedResponse(c)
	if err != nil {
		return nil, ErrIdentify{err.Error()}
	}

	if frameType == FrameTypeError {
		return nil, ErrIdentify{string(data)}
	}

	// check to see if the server was able to respond w/ capabilities
	// i.e. it was a JSON response
	if data[0] != '{' {
		return nil, nil
	}

	resp := &IdentifyResponse{}
	err = json.Unmarshal(data, resp)
	if err != nil {
		return nil, ErrIdentify{err.Error()}
	}

	c.log(LogLevelDebug, "IDENTIFY response: %+v", resp)

	c.maxRdyCount = resp.MaxRdyCount

	if resp.TLSv1 {
		c.log(LogLevelInfo, "upgrading to TLS")
		err := c.upgradeTLS(c.config.TlsConfig)
		if err != nil {
			return nil, ErrIdentify{err.Error()}
		}
	}

	if resp.Deflate {
		c.log(LogLevelInfo, "upgrading to Deflate")
		err := c.upgradeDeflate(c.config.DeflateLevel)
		if err != nil {
			return nil, ErrIdentify{err.Error()}
		}
	}

	if resp.Snappy {
		c.log(LogLevelInfo, "upgrading to Snappy")
		err := c.upgradeSnappy()
		if err != nil {
			return nil, ErrIdentify{err.Error()}
		}
	}

	// now that connection is bootstrapped, enable read buffering
	// (and write buffering if it's not already capable of Flush())
	// c.r = bufio.NewReader(c.r)
	// if _, ok := c.w.(flusher); !ok {
	// 	c.w = bufio.NewWriter(c.w)
	// }

	return resp, nil
}

func (c *Conn) upgradeTLS(tlsConf *tls.Config) error {
	// create a local copy of the config to set ServerName for this connection
	var conf tls.Config
	if tlsConf != nil {
		conf = *tlsConf
	}
	host, _, err := net.SplitHostPort(c.addr)
	if err != nil {
		return err
	}
	conf.ServerName = host

	c.tlsConn = tls.Client(c.conn, &conf)
	err = c.tlsConn.Handshake()
	if err != nil {
		return err
	}
	c.r = c.tlsConn
	c.w = c.tlsConn
	frameType, data, err := ReadUnpackedResponse(c)
	if err != nil {
		return err
	}
	if frameType != FrameTypeResponse || !bytes.Equal(data, []byte("OK")) {
		return errors.New("invalid response from TLS upgrade")
	}
	return nil
}

func (c *Conn) upgradeDeflate(level int) error {
	conn := net.Conn(c.conn)
	if c.tlsConn != nil {
		conn = c.tlsConn
	}
	fw, _ := flate.NewWriter(conn, level)
	c.r = flate.NewReader(conn)
	c.w = fw
	frameType, data, err := ReadUnpackedResponse(c)
	if err != nil {
		return err
	}
	if frameType != FrameTypeResponse || !bytes.Equal(data, []byte("OK")) {
		return errors.New("invalid response from Deflate upgrade")
	}
	return nil
}

func (c *Conn) upgradeSnappy() error {
	conn := net.Conn(c.conn)
	if c.tlsConn != nil {
		conn = c.tlsConn
	}
	c.r = snappystream.NewReader(conn, snappystream.SkipVerifyChecksum)
	c.w = snappystream.NewWriter(conn)
	frameType, data, err := ReadUnpackedResponse(c)
	if err != nil {
		return err
	}
	if frameType != FrameTypeResponse || !bytes.Equal(data, []byte("OK")) {
		return errors.New("invalid response from Snappy upgrade")
	}
	return nil
}

func (c *Conn) auth(secret string) error {
	cmd, err := Auth(secret)
	if err != nil {
		return err
	}

	err = c.WriteCommand(cmd)
	if err != nil {
		return err
	}

	frameType, data, err := ReadUnpackedResponse(c)
	if err != nil {
		return err
	}

	if frameType == FrameTypeError {
		return errors.New("Error authenticating " + string(data))
	}

	resp := &AuthResponse{}
	err = json.Unmarshal(data, resp)
	if err != nil {
		return err
	}

	c.log(LogLevelInfo, "Auth accepted. Identity: %q %s Permissions: %d",
		resp.Identity, resp.IdentityUrl, resp.PermissionCount)

	return nil
}

func (c *Conn) readLoop() {
	var addr net.IP
	var err error
	c.log(LogLevelInfo, "start readLoop")
	delegate := &connMessageDelegate{c}
	for {
		if atomic.LoadInt32(&c.closeFlag) == 1 {
			goto exit
		}
		if c.connType == ConnTypeMulticast {
			addr, err = ReadAddr(c)
			if err != nil {
				c.log(LogLevelError, "IO error Read Addr - %s", err)
			}
		}
		frameType, data, err := ReadUnpackedResponse(c)
		c.log(LogLevelDebug, "Get message in readLoop, frameType %d, data %s", frameType, data)
		if err != nil {
			if !strings.Contains(err.Error(), "use of closed network connection") {
				c.log(LogLevelError, "IO error - %s", err)
				c.delegate.OnIOError(c, err)
			}
			goto exit
		}

		if frameType == FrameTypeResponse && bytes.Equal(data, []byte("_heartbeat_")) {
			c.log(LogLevelDebug, "heartbeat received")
			c.delegate.OnHeartbeat(c)
			if c.connType != ConnTypeMulticast {
				err = c.WriteCommand(Nop())
				if err != nil {
					c.log(LogLevelError, "IO error - %s", err)
					c.delegate.OnIOError(c, err)
					goto exit
				}
			}
			continue
		}

		switch frameType {
		case FrameTypeResponse:
			c.delegate.OnResponse(c, data)
		case FrameTypeMessage:
			msg, err := DecodeMessage(data)
			if err != nil {
				c.log(LogLevelError, "IO error - %s", err)
				c.delegate.OnIOError(c, err)
				goto exit
			}
			msg.Delegate = delegate
			if c.connType == ConnTypeMulticast {
				msg.NSQDAddress = addr.String()
				msg.DisableAutoResponse()
			} else {
				msg.NSQDAddress = c.String()
			}
			atomic.AddInt64(&c.rdyCount, -1)
			atomic.StoreInt64(&c.lastMsgTimestamp, time.Now().UnixNano())
			if c.connType == ConnTypeDefault || c.connType == ConnTypeMTCP {
				// do care about inflight for tcp
				atomic.AddInt64(&c.messagesInFlight, 1)
				c.delegate.OnMessage(c, msg)
			} else if c.ReliableRecv(msg, FrameTypeMessage) {
				c.delegate.OnMessage(c, msg)
			}

		case FrameTypeNull:
			msg, err := DecodeMessage(data)
			if err != nil {
				c.log(LogLevelError, "IO error - %s", err)
				c.delegate.OnIOError(c, err)
				goto exit
			}

			if c.connType == ConnTypeMulticast {
				msg.NSQDAddress = addr.String()
				msg.DisableAutoResponse()
			} else {
				msg.NSQDAddress = c.String()
			}

			if c.connType == ConnTypeMulticast {
				msg.DisableAutoResponse()
				c.ReliableRecv(msg, FrameTypeNull)
			}
			msg.Delegate = delegate
			c.delegate.OnMessageNull(c, msg)
		case FrameTypeError:
			c.log(LogLevelError, "protocol error - %s", data)
			c.delegate.OnError(c, data)
		default:
			c.log(LogLevelError, "IO error - %s", err)
			c.delegate.OnIOError(c, fmt.Errorf("unknown frame type %d", frameType))
		}
	}

exit:
	atomic.StoreInt32(&c.readLoopRunning, 0)
	// start the connection close
	messagesInFlight := atomic.LoadInt64(&c.messagesInFlight)
	if messagesInFlight == 0 {
		// if we exited readLoop with no messages in flight
		// we need to explicitly trigger the close because
		// writeLoop won't
		c.close()
	} else {
		c.log(LogLevelWarning, "delaying close, %d outstanding messages", messagesInFlight)
	}
	c.wg.Done()
	c.log(LogLevelInfo, "readLoop exiting")
}

func (c *Conn) writeLoop() {
	c.log(LogLevelInfo, "start writeLoop")
	var resendTiker *time.Ticker
	var resendChan <-chan time.Time

	if c.connType == ConnTypeMulticast {
		resendTiker = time.NewTicker(c.resendTimeout)
		resendChan = resendTiker.C
	}
	for {
		select {
		case <-resendChan:
			c.processResendTimeout()
		case <-c.exitChan:
			c.log(LogLevelInfo, "breaking out of writeLoop")
			// Indicate drainReady because we will not pull any more off msgResponseChan
			close(c.drainReady)
			goto exit
		case cmd := <-c.cmdChan:
			err := c.WriteCommand(cmd)
			if err != nil {
				c.log(LogLevelError, "error sending command %s - %s", cmd, err)
				c.close()
				continue
			}
		case resp := <-c.msgResponseChan:
			// Decrement this here so it is correct even if we can't respond to nsqd
			msgsInFlight := atomic.AddInt64(&c.messagesInFlight, -1)

			if resp.success {
				c.log(LogLevelDebug, "FIN %s", resp.msg.ID)
				c.delegate.OnMessageFinished(c, resp.msg)
				c.delegate.OnResume(c)
			} else {
				c.log(LogLevelDebug, "REQ %s", resp.msg.ID)
				c.delegate.OnMessageRequeued(c, resp.msg)
				if resp.backoff {
					c.delegate.OnBackoff(c)
				} else {
					c.delegate.OnContinue(c)
				}
			}

			err := c.WriteCommand(resp.cmd)
			if err != nil {
				c.log(LogLevelError, "error sending command %s - %s", resp.cmd, err)
				c.close()
				continue
			}

			if msgsInFlight == 0 &&
				atomic.LoadInt32(&c.closeFlag) == 1 {
				c.close()
				continue
			}
		}
	}

exit:
	c.wg.Done()
	if resendTiker != nil {
		resendTiker.Stop()
	}
	c.log(LogLevelInfo, "writeLoop exiting")
}

func (c *Conn) close() {
	// a "clean" connection close is orchestrated as follows:
	//
	//     1. CLOSE cmd sent to nsqd
	//     2. CLOSE_WAIT response received from nsqd
	//     3. set c.closeFlag
	//     4. readLoop() exits
	//         a. if messages-in-flight > 0 delay close()
	//             i. writeLoop() continues receiving on c.msgResponseChan chan
	//                 x. when messages-in-flight == 0 call close()
	//         b. else call close() immediately
	//     5. c.exitChan close
	//         a. writeLoop() exits
	//             i. c.drainReady close
	//     6a. launch cleanup() goroutine (we're racing with intraprocess
	//        routed messages, see comments below)
	//         a. wait on c.drainReady
	//         b. loop and receive on c.msgResponseChan chan
	//            until messages-in-flight == 0
	//            i. ensure that readLoop has exited
	//     6b. launch waitForCleanup() goroutine
	//         b. wait on waitgroup (covers readLoop() and writeLoop()
	//            and cleanup goroutine)
	//         c. underlying TCP connection close
	//         d. trigger Delegate OnClose()
	//
	c.stopper.Do(func() {
		c.log(LogLevelInfo, "beginning close")
		close(c.exitChan)
		c.CloseRead()

		c.wg.Add(1)
		go c.cleanup()

		go c.waitForCleanup()
	})
}

func (c *Conn) cleanup() {
	<-c.drainReady
	ticker := time.NewTicker(100 * time.Millisecond)
	lastWarning := time.Now()
	// writeLoop has exited, drain any remaining in flight messages
	for {
		// we're racing with readLoop which potentially has a message
		// for handling so infinitely loop until messagesInFlight == 0
		// and readLoop has exited
		var msgsInFlight int64
		select {
		case <-c.msgResponseChan:
			msgsInFlight = atomic.AddInt64(&c.messagesInFlight, -1)
		case <-ticker.C:
			msgsInFlight = atomic.LoadInt64(&c.messagesInFlight)
		}
		if msgsInFlight > 0 {
			if time.Now().Sub(lastWarning) > time.Second {
				c.log(LogLevelWarning, "draining... waiting for %d messages in flight", msgsInFlight)
				lastWarning = time.Now()
			}
			continue
		}
		// until the readLoop has exited we cannot be sure that there
		// still won't be a race
		if atomic.LoadInt32(&c.readLoopRunning) == 1 {
			if time.Now().Sub(lastWarning) > time.Second {
				c.log(LogLevelWarning, "draining... readLoop still running")
				lastWarning = time.Now()
			}
			continue
		}
		goto exit
	}

exit:
	ticker.Stop()
	c.wg.Done()
	c.log(LogLevelInfo, "finished draining, cleanup exiting")
}

func (c *Conn) waitForCleanup() {
	// this blocks until readLoop and writeLoop
	// (and cleanup goroutine above) have exited
	c.wg.Wait()
	c.CloseWrite()
	c.log(LogLevelInfo, "clean close complete")
	c.delegate.OnClose(c)
}

func (c *Conn) onMessageFinish(m *Message) {
	if c.connType == ConnTypeTCP || c.connType == ConnTypeMTCP {
		c.msgResponseChan <- &msgResponse{msg: m, cmd: Finish(m.ID), success: true}
	}
}

func (c *Conn) onMessageRequeue(m *Message, delay time.Duration, backoff bool) {
	if delay == -1 {
		// linear delay
		delay = c.config.DefaultRequeueDelay * time.Duration(m.Attempts)
		// bound the requeueDelay to configured max
		if delay > c.config.MaxRequeueDelay {
			delay = c.config.MaxRequeueDelay
		}
	}
	c.msgResponseChan <- &msgResponse{msg: m, cmd: Requeue(m.ID, delay), success: false, backoff: backoff}
}

// Reliablerecv evoke NACK, return bool indicating
// whether return the message to upper layer or not
func (c *Conn) ReliableRecv(m *Message, frameType int32) bool {
	sendFlag := true
	c.lostListSync.Lock()
	defer c.lostListSync.Unlock()
	lastSeq, ok := c.lastSeqID[m.NSQDAddress]
	if !ok {
		c.lastSeqID[m.NSQDAddress] = m.SeqID
		return sendFlag
	}
	expID := lastSeq + 1
	now := time.Now()
	endID := m.SeqID
	if frameType == FrameTypeNull {
		endID = m.SeqID + 1 // null type
	}
	if m.SeqID >= expID { // out-of-order message
		for i := expID; i < endID; i++ {
			_, ok := c.lostMsgList[mapKey{m.NSQDAddress, i}]
			if !ok {
				cmd := Nack(i)
				cmd.addr = m.NSQDAddress
				// c.log(LogLevelDebug, "Packet Loss from %s for message %d", m.NSQDAddress, m.SeqID)
				c.log(LogLevelDebug, "Packet Loss from %s for message %d", m.NSQDAddress, m.SeqID)
				c.WriteCommand(cmd)
				c.lostMsgList[mapKey{m.NSQDAddress, i}] = &mapValue{now, now}
				atomic.AddInt64(&c.lostInFlight, 1)
				atomic.AddInt64(&c.lostCount, 1)
				c.delegate.OnMessageNacked(c, m)
			}
		}
		c.lastSeqID[m.NSQDAddress] = m.SeqID
	} else { // duplicated or resend message
		_, ok := c.lostMsgList[mapKey{m.NSQDAddress, m.SeqID}]
		if ok && frameType == FrameTypeMessage { // resend message received
			delete(c.lostMsgList, mapKey{m.NSQDAddress, m.SeqID})
			atomic.AddInt64(&c.lostInFlight, -1)
		} else { // duplicated message of NULL
			sendFlag = false
		}
	}
	return sendFlag
}

func (c *Conn) processResendTimeout() {
	c.lostListSync.Lock()
	now := time.Now()
	for k, v := range c.lostMsgList {
		if now.Sub(v.firstTS) >= c.maxHardTimeout {
			// c.log(LogLevelDebug, "Remove Resend List %s for message %d", k.Addr, k.SeqID)
			c.log(LogLevelDebug, "Remove Resend List %s for message %d", k.Addr, k.SeqID)
			delete(c.lostMsgList, k)
			atomic.AddInt64(&c.lostInFlight, -1)
			continue
		}
		if now.Sub(v.lastTS) >= c.resendTimeout {
			//c.log(LogLevelDebug, "Resend NACK to %s for message %d", k.Addr, k.SeqID)
			c.log(LogLevelDebug, "Resend NACK to %s for message %d", k.Addr, k.SeqID)
			cmd := Nack(k.SeqID)
			cmd.addr = k.Addr
			c.WriteCommand(cmd)
			c.lostMsgList[k].lastTS = now
		}
	}
	c.lostListSync.Unlock()
}

func (c *Conn) onMessageTouch(m *Message) {
	select {
	case c.cmdChan <- Touch(m.ID):
	case <-c.exitChan:
	}
}

func (c *Conn) log(lvl LogLevel, line string, args ...interface{}) {
	logger, logLvl, logFmt := c.getLogger()

	if logger == nil {
		return
	}

	if logLvl > lvl {
		return
	}

	logger.Output(2, fmt.Sprintf("%-4s %s %s", lvl,
		fmt.Sprintf(logFmt, c.String()),
		fmt.Sprintf(line, args...)))
}
