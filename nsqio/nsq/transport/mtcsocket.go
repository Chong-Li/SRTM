package transport

import (
	"encoding/binary"
	"fmt"
	"net"
	"sync"
	"syscall"
	"time"

	"golang.org/x/net/ipv4"
)

const defaultReadBufferSize = 1048576  // 1M
const defaultWriteBufferSize = 1048576 // 1M

const (
	NotClosed   = 0
	ReadClosed  = 1
	WriteClosed = 2
)

const maxAddrStringLength = 39
const reservedLength = maxAddrStringLength + 5

const (
	MTCTypeDefault      = 3
	MTCTypeReadOnly     = 1
	MTCTypeWriteOnly    = 2
	MTCTypeReadAndWrite = 3
	MTCTypeDaemon       = MTCTypeWriteOnly
	MTCTypeConsumer     = MTCTypeReadOnly
)

type MTCConn struct {
	// ListenPacket and PacketConn for multicast
	conn  net.PacketConn
	pconn *ipv4.PacketConn

	uconn *net.UDPConn

	intf *net.Interface
	src  *net.UDPAddr
	mdst *net.UDPAddr

	writeLock sync.RWMutex
	readLock  sync.RWMutex
	mtcType   int
	//rbuf *RingBuffer
	//wbuf *RingBuffer

	// reliable
	//SndSeq uint64
	//RcvSeq uint64
	count int
}

type MTCHeader struct {
	length   uint32
	mtcORunc uint32
	sequence uint64
}

func NewMTCConn(intf string, addr *net.UDPAddr, maddr *net.UDPAddr, mtcType int) (conn *MTCConn, err error) {
	// get interface
	en0, err := net.InterfaceByName(intf)
	if err != nil {
		return nil, err
	}
	//an application listens to an appropriate address with an appropriate service port.
	c, err := net.ListenPacket("udp4", addr.String())
	if err != nil {
		return nil, err
	}
	if !addr.IP.IsMulticast() {
		setReuseAddress(c)
	}

	// join group
	p := ipv4.NewPacketConn(c)
	err = p.JoinGroup(en0, maddr)
	if err != nil {
		return nil, err
	}

	// if the application need a dest address in the packet
	err = p.SetControlMessage(ipv4.FlagDst, true)
	if err != nil {
		return nil, err
	}
	err = p.SetControlMessage(ipv4.FlagSrc, true)
	if err != nil {
		return nil, err
	}

	p.SetTOS(0x0)
	p.SetTTL(2)
	err = p.SetMulticastInterface(en0)
	if err != nil {
		return nil, err
	}

	// return MTConn
	conn = &MTCConn{
		conn:    c,
		pconn:   p,
		intf:    en0,
		src:     addr,
		mdst:    maddr,
		mtcType: mtcType,
		//rbuf: NewRingBuffer(defaultReadBufferSize),
		//wbuf: NewRingBuffer(defaultWriteBufferSize),
	}

	//go handleRead(conn)
	//go handleWrite(conn)
	return conn, err
}

func setReuseAddress(conn net.PacketConn) {
	file, _ := conn.(*net.UDPConn).File()
	syscall.SetsockoptInt(int(file.Fd()), syscall.SOL_SOCKET, syscall.SO_REUSEADDR, 1)
}

// GetIPAddr returen the ip address of given interface
func GetIPAddr(intf string) string {
	en0, err := net.InterfaceByName(intf)
	if err != nil {
		return ""
	}
	addrs, err := en0.Addrs()
	for _, address := range addrs {
		// check the address type and if it is not a loopback the display it
		if ipnet, ok := address.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			if ipnet.IP.To4() != nil {
				return ipnet.IP.String()
			}
		}
	}
	return ""
}

// the implementation of Conn interface

func (c *MTCConn) ok() bool { return c != nil && c.pconn != nil && c.conn != nil }

func (c *MTCConn) Read(b []byte) (n int, err error) {
	c.readLock.Lock()
	defer c.readLock.Unlock()
	if !c.ok() {
		return 0, syscall.EINVAL
	}
	n, cm, _, err := c.pconn.ReadFrom(b[reservedLength:])

	if err != nil || n == 0 {
		return n, err
	}
	if cm.Src.IsLoopback() { //cm.Src.Equal(c.src.IP) ||
		return 0, nil
	}
	srcIP := cm.Src.String()
	binary.BigEndian.PutUint32(b[:4], uint32(reservedLength-4))
	pstr := fmt.Sprintf("%%-%ds\n", maxAddrStringLength)
	s := fmt.Sprintf(pstr, srcIP)
	copy(b[4:reservedLength], []byte(s))
	return n + reservedLength, err
}

func (c *MTCConn) Write(b []byte) (n int, err error) {
	c.writeLock.Lock()
	defer c.writeLock.Unlock()
	if !c.ok() {
		return 0, syscall.EINVAL
	}
	var dst net.UDPAddr
	addrLen := binary.BigEndian.Uint32(b[:4])
	if addrLen != 0 {
		addrIP := net.ParseIP(string(b[4 : 4+addrLen-1]))
		dst.IP = addrIP
		dst.Port = c.mdst.Port
		n, err = c.pconn.WriteTo(b[4+addrLen:], nil, &dst)
		return n + 4 + int(addrLen), err
	} else {
		n, err = c.pconn.WriteTo(b[4:], nil, c.mdst)
		return n + 4, err
	}
}

func (c *MTCConn) Close() error {
	if !c.ok() {
		return syscall.EINVAL
	}
	c.pconn.LeaveGroup(c.intf, &net.UDPAddr{IP: c.mdst.IP})
	return c.conn.Close()
}

func (c *MTCConn) LocalAddr() net.Addr {
	if !c.ok() {
		return nil
	}
	return &net.UDPAddr{IP: net.ParseIP("0.0.0.0"), Port: 0}
}

func (c *MTCConn) RemoteAddr() net.Addr {
	if !c.ok() {
		return nil
	}
	return c.mdst
}

func (c *MTCConn) SetDeadline(t time.Time) error {
	if !c.ok() {
		return syscall.EINVAL
	}
	return c.conn.SetDeadline(t)
}

func (c *MTCConn) SetReadDeadline(t time.Time) error {
	if !c.ok() {
		return syscall.EINVAL
	}
	return c.conn.SetReadDeadline(t)
}

func (c *MTCConn) SetWriteDeadline(t time.Time) error {
	if !c.ok() {
		return syscall.EINVAL
	}
	return c.conn.SetWriteDeadline(t)
}
