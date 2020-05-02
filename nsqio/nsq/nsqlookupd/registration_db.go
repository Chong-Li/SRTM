package nsqlookupd

import (
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
	"container/list"

	"github.com/emirpasic/gods/trees/redblacktree"

)

type bEntry struct {
	index int
	burst int
}

type Timeline struct {
	pubID     int
	rate      float64
	scanIndex int
	time      []bEntry
}
type TimeStamp struct {
	t time.Time
	nano int64
}
type CorGroup struct {
	num int
	index int
	tmp_index int
	tmp_index2 int
	pubs []int
	assigns []int
}

type RegistrationDB struct {
	sync.RWMutex
	registrationMap map[Registration]Producers
	topicDist       map[string][]*Producer
	topicCands      map[string][]*Producer
	matrix          map[string][]*Timeline
	preDist         map[string][]*Producer
	topicCount      map[string]int
	TCtotal         int
	burstDist       map[string][]*Producer
	topicCount2     map[string]int
	totalCPU        map[string]float64
	totalRate       map[string]float64
	times           map[string][][]int64
	tp_times        map[string][][]int64
	latency         map[string][]int64
	tb_times        map[string][][]TimeStamp
	TPName         string
	TPRate         int
	perRate        int
	TPBurst        int
	groups         map[int]*CorGroup
	keys           []int
	random          *rand.Rand
	brokerList      map[string][]*Producer
	initBroker      *Producer
	mmDist          map[int]*Producer
	rates           map[int]int
	oriBroker       map[int]int
	curBIndex       int
	profileTopic    chan int
	profileChan     chan int
	TBChan          chan int64
	kValue          int
	lastClient      *ClientV1
	curK            int
	curK2           int
	realK           int
	lastTail        int64
}

type Registration struct {
	Category string
	Key      string
	SubKey   string
}
type Registrations []Registration

type RateUpdate struct {
	rate     int
	burst    int
	updated  bool
	newrate  float64
	newburst float64
	sendback bool
}
type timeInfo struct {
        stamp int64
        //inter int64
        delay int64
}

type PeerInfo struct {
	lastUpdate       int64
	id               string
	RemoteAddress    string `json:"remote_address"`
	TCPPort          int    `json:"tcp_port"`
	Hostname         string `json:"hostname"`
	ConnType         int    `json:"connection_type"`
	BroadcastAddress string `json:"broadcast_address"`
	HTTPPort         int    `json:"http_port"`
	Version          string `json:"version"`
	//yao
	//add daemon_priority into the struct
	DaemonPriority string `json:"daemon_priority"`

	//RTM
	TopicCount    int
	ChannelCount  int
	ConnCount     int
	FConnCount    int
	PrevConnCount int
	QLen          int
	CPU           float64
	CPUVar        float64
	oldCPU        float64
	oldVar        float64
	newCPU        float64
	newVar        float64
	risk          float64
	rCap          float64
	oldRisk       float64
	newRisk       float64
	curRisk       float64
	curConn       float64
	curCPU        float64
	curVar        float64
	time          []bEntry
	rates         map[string]*RateUpdate
	bursts        map[string]float64
	totalRate     int
	tpRate        int
	client        *ClientV1
	BackMsg       [][]byte
	tp_times      []int64
	tp_times2     []int64
	info          []timeInfo
	info2         []timeInfo
	tokenRate     float64
	over          int64
	over2         int64
	msgCount      int64
        tp_list       *list.List
	tree          *redblacktree.Tree
	pubs          []int
	keys          []int
	groups         map[int]*CorGroup
	rcap          int
	rate          int
	profileChan   chan int
	backup        *PeerInfo
	tRate         float64
	balRate        int
	lastLatency    int64
	curLatency     int64
	inProfile      int
	curProfile     int
        prevProfile    int
	sync.RWMutex
}

// jarry: some connection types
// const (
// 	ConnTypeDefault   = 0
// 	ConnTypeTCP       = 0
// 	ConnTypeMulticast = 1
// )

// jarry: connection related information
// type ConnInfo struct {
// 	connType int
// 	addr     string
// 	intf     string
// }

type Producer struct {
	peerInfo *PeerInfo
	//connInfo     *ConnInfo
	tombstoned   bool
	tombstonedAt time.Time
}

type Producers []*Producer

func (p *Producer) String() string {
	return fmt.Sprintf("%s [%d, %d]", p.peerInfo.BroadcastAddress, p.peerInfo.TCPPort, p.peerInfo.HTTPPort)
}

func (p *Producer) Tombstone() {
	p.tombstoned = true
	p.tombstonedAt = time.Now()
}

func (p *Producer) IsTombstoned(lifetime time.Duration) bool {
	return p.tombstoned && time.Now().Sub(p.tombstonedAt) < lifetime
}

func NewRegistrationDB() *RegistrationDB {
	return &RegistrationDB{
		registrationMap: make(map[Registration]Producers),
		topicDist:       make(map[string][]*Producer),
		topicCands:      make(map[string][]*Producer),
		matrix:          make(map[string][]*Timeline),
		preDist:         make(map[string][]*Producer),
		burstDist:       make(map[string][]*Producer),
		topicCount:      make(map[string]int),
		TCtotal:         -1, //RTM.dtb
		topicCount2:     make(map[string]int),
		totalCPU:        make(map[string]float64),
		totalRate:       make(map[string]float64),
		times:           make(map[string][][]int64),
		tp_times:        make(map[string][][]int64),
		tb_times:        make(map[string][][]TimeStamp),
		latency:          make(map[string][]int64),
		random:          rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

// add a registration key
func (r *RegistrationDB) AddRegistration(k Registration) {
	r.Lock()
	defer r.Unlock()
	_, ok := r.registrationMap[k]
	if !ok {
		r.registrationMap[k] = Producers{}
	}
}

// add a producer to a registration
func (r *RegistrationDB) AddProducer(k Registration, p *Producer) bool {
	r.Lock()
	defer r.Unlock()
	producers := r.registrationMap[k]
	found := false
	for _, producer := range producers {
		if producer.peerInfo.id == p.peerInfo.id {
			found = true
		}
	}
	if found == false {
		r.registrationMap[k] = append(producers, p)
	}
	return !found
}

// remove a producer from a registration
func (r *RegistrationDB) RemoveProducer(k Registration, id string) (bool, int) {
	r.Lock()
	defer r.Unlock()
	producers, ok := r.registrationMap[k]
	if !ok {
		return false, 0
	}
	removed := false
	cleaned := Producers{}
	for _, producer := range producers {
		if producer.peerInfo.id != id {
			cleaned = append(cleaned, producer)
		} else {
			removed = true
		}
	}
	// Note: this leaves keys in the DB even if they have empty lists
	r.registrationMap[k] = cleaned
	return removed, len(cleaned)
}

// remove a Registration and all it's producers
func (r *RegistrationDB) RemoveRegistration(k Registration) {
	r.Lock()
	defer r.Unlock()
	delete(r.registrationMap, k)
}

func (r *RegistrationDB) needFilter(key string, subkey string) bool {
	return key == "*" || subkey == "*"
}

func (r *RegistrationDB) FindRegistrations(category string, key string, subkey string) Registrations {
	r.RLock()
	defer r.RUnlock()
	if !r.needFilter(key, subkey) {
		k := Registration{category, key, subkey}
		if _, ok := r.registrationMap[k]; ok {
			return Registrations{k}
		}
		return Registrations{}
	}
	results := Registrations{}
	for k := range r.registrationMap {
		if !k.IsMatch(category, key, subkey) {
			continue
		}
		results = append(results, k)
	}
	return results
}

func (r *RegistrationDB) FindProducers(category string, key string, subkey string) Producers {
	r.RLock()
	defer r.RUnlock()
	if !r.needFilter(key, subkey) {
		k := Registration{category, key, subkey}
		return r.registrationMap[k]
	}

	results := Producers{}
	for k, producers := range r.registrationMap {
		if !k.IsMatch(category, key, subkey) {
			continue
		}
		for _, producer := range producers {
			found := false
			for _, p := range results {
				if producer.peerInfo.id == p.peerInfo.id {
					found = true
				}
			}
			if found == false {
				results = append(results, producer)
			}
		}
	}
	return results
}

func (r *RegistrationDB) LookupRegistrations(id string) Registrations {
	r.RLock()
	defer r.RUnlock()
	results := Registrations{}
	for k, producers := range r.registrationMap {
		for _, p := range producers {
			if p.peerInfo.id == id {
				results = append(results, k)
				break
			}
		}
	}
	return results
}

func (k Registration) IsMatch(category string, key string, subkey string) bool {
	if category != k.Category {
		return false
	}
	if key != "*" && k.Key != key {
		return false
	}
	if subkey != "*" && k.SubKey != subkey {
		return false
	}
	return true
}

func (rr Registrations) Filter(category string, key string, subkey string) Registrations {
	output := Registrations{}
	for _, k := range rr {
		if k.IsMatch(category, key, subkey) {
			output = append(output, k)
		}
	}
	return output
}

func (rr Registrations) Keys() []string {
	keys := make([]string, len(rr))
	for i, k := range rr {
		keys[i] = k.Key
	}
	return keys
}

func (rr Registrations) SubKeys() []string {
	subkeys := make([]string, len(rr))
	for i, k := range rr {
		subkeys[i] = k.SubKey
	}
	return subkeys
}

func (pp Producers) FilterByActive(inactivityTimeout time.Duration, tombstoneLifetime time.Duration) Producers {
	now := time.Now()
	results := Producers{}
	for _, p := range pp {
		cur := time.Unix(0, atomic.LoadInt64(&p.peerInfo.lastUpdate))
		if now.Sub(cur) > inactivityTimeout || p.IsTombstoned(tombstoneLifetime) {
			continue
		}
		results = append(results, p)
	}
	return results
}

func (pp Producers) PeerInfo() []*PeerInfo {
	results := []*PeerInfo{}
	for _, p := range pp {
		results = append(results, p.peerInfo)
	}
	return results
}

func (s *httpServer) printHighPrioTopicList() {
	s.ctx.nsqlookupd.DB.FindRegistrations("high_priority_topic", "*", "")
}

/*
Yao
important Note
when a producer has a registration looks like ("topic", topicName, "#inactive")
it means that the producer was serving the topic, but now another nsqd is serving it
we could make it once again active by tweaking the "#inactive" -> "#active" flag
*/
