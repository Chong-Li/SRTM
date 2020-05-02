package nsqlookupd

import (
	"net"
	"strconv"
	"sync"
)

// MTCAddrStruct ...
type MTCAddrStruct struct {
	Addr  *net.UDPAddr
	Count int
}

// MTCAddrManager ...
type MTCAddrManager struct {
	sync.RWMutex
	AddrMap  *UsageMap
	PortMap  *UsageMap
	AddrList map[string]*MTCAddrStruct
}

// NewMTCAddrManager ...
func NewMTCAddrManager(opts *Options) *MTCAddrManager {
	addrmax := opts.AddrMax
	addrmin := opts.AddrMin
	portmax := opts.PortMax
	portmin := opts.PortMin

	addrMap := NewAddrMap(addrmin, addrmax)
	portMap := NewPortMap(portmin, portmax)
	return &MTCAddrManager{
		AddrMap:  addrMap,
		PortMap:  portMap,
		AddrList: make(map[string]*MTCAddrStruct),
	}
}

// RegisterMTCAddr ...
// Get A new Multicast Address and add it to the list
// Call for topic/channel register
func (m *MTCAddrManager) RegisterMTCAddr(topic string) (addr *net.UDPAddr, err int) {
	m.Lock()
	if m.HasMTCAddr(topic) {
		addr = m.AddrList[topic].Addr
	} else {
		ip := m.AddrMap.GetNew()
		if ip == -1 {
			m.AddrMap.RemoveUsage(ip)
			goto exit
		}
		port := m.PortMap.GetNew()
		if port == -1 {
			m.AddrMap.RemoveUsage(port)
			goto exit
		}
		IPAddr := InttoIP4(ip)
		addr = m.AddMTCAddr(topic, IPAddr, port)
	}
	m.AddrList[topic].Count++
	m.Unlock()
	return addr, 0
exit:
	m.Unlock()
	return nil, -1
}

// UnregisterMTCAddr ...
func (m *MTCAddrManager) UnregisterMTCAddr(topic string) {
	m.Lock()
	if m.HasMTCAddr(topic) {
		m.AddrList[topic].Count--
		if m.AddrList[topic].Count == 0 {
			IPAddr := m.AddrList[topic].Addr.IP.String()
			port := m.AddrList[topic].Addr.Port
			ip := IP4toInt(IPAddr)
			m.AddrMap.RemoveUsage(ip)
			m.PortMap.RemoveUsage(port)
			delete(m.AddrList, topic)
		}
	}
	m.Unlock()
}

// GetMTCAddr ...
func (m *MTCAddrManager) GetMTCAddr(topic string) (addr *net.UDPAddr, err int) {
	if m.HasMTCAddr(topic) {
		return m.AddrList[topic].Addr, 0
	}
	return nil, -1
}

// AddMTCAddr ...
func (m *MTCAddrManager) AddMTCAddr(topic string, addr string, port int) *net.UDPAddr {
	udpAddr, _ := net.ResolveUDPAddr("udp", addr+":"+strconv.Itoa(port))
	m.AddrList[topic] = &MTCAddrStruct{udpAddr, 0}
	return udpAddr
}

// RemoveMTCAddr ...
func (m *MTCAddrManager) RemoveMTCAddr(topic string) (addr *net.UDPAddr) {
	addr = m.AddrList[topic].Addr
	delete(m.AddrList, topic)
	return addr
}

// HasMTCAddr ...
func (m *MTCAddrManager) HasMTCAddr(topic string) bool {
	_, ok := m.AddrList[topic]
	return ok
}
