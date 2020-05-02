package nsqlookupd

import (
	"fmt"
	"io/ioutil"
	"net"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
	"math/rand"

	"github.com/WU-CPSL/RTM-0.1/nsqio/go-nsq"
	"github.com/WU-CPSL/RTM-0.1/nsqio/nsq/internal/http_api"
	"github.com/WU-CPSL/RTM-0.1/nsqio/nsq/internal/protocol"
	"github.com/WU-CPSL/RTM-0.1/nsqio/nsq/internal/util"
	"github.com/WU-CPSL/RTM-0.1/nsqio/nsq/internal/version"
)

type NSQLookupd struct {
	sync.RWMutex
	opts         *Options
	tcpListener  net.Listener
	httpListener net.Listener
	waitGroup    util.WaitGroupWrapper
	DB           *RegistrationDB
	Manager      *MTCAddrManager
	ReportBack   map[*ClientV1]bool
	profileTopic chan string
}

func New(opts *Options) *NSQLookupd {
	if opts.MulticastFlag {
		n := &NSQLookupd{
			opts:         opts,
			DB:           NewRegistrationDB(),
			Manager:      NewMTCAddrManager(opts),
			ReportBack:   make(map[*ClientV1]bool),
			profileTopic: make(chan string),
		}
		n.logf(version.String("nsqlookupd"))
		return n
	} else {
		n := &NSQLookupd{
			opts:       opts,
			DB:         NewRegistrationDB(),
			ReportBack: make(map[*ClientV1]bool),
		}
		n.logf(version.String("nsqlookupd"))
		return n
	}
}

func (l *NSQLookupd) logf(f string, args ...interface{}) {
	if l.opts.Logger == nil {
		return
	}
	l.opts.Logger.Output(2, fmt.Sprintf(f, args...))
}

func (l *NSQLookupd) Main() {
	ctx := &Context{l}

	//yao
	l.grabHighPriorityTopics(l.opts.HighPriorityTopicFile)

	tcpListener, err := net.Listen("tcp", l.opts.TCPAddress)
	if err != nil {
		l.logf("FATAL: listen (%s) failed - %s", l.opts.TCPAddress, err)
		os.Exit(1)
	}
	l.Lock()
	l.tcpListener = tcpListener
	l.Unlock()
	tcpServer := &tcpServer{ctx: ctx}
	l.waitGroup.Wrap(func() {
		protocol.TCPServer(tcpListener, tcpServer, l.opts.Logger)
	})

	httpListener, err := net.Listen("tcp", l.opts.HTTPAddress)
	if err != nil {
		l.logf("FATAL: listen (%s) failed - %s", l.opts.HTTPAddress, err)
		os.Exit(1)
	}
	l.Lock()
	l.httpListener = httpListener
	l.Unlock()
	httpServer := newHTTPServer(ctx)
	l.waitGroup.Wrap(func() {
		http_api.Serve(httpListener, httpServer, "HTTP", l.opts.Logger)
	})
	//RTM.dtb
	l.DB.brokerList = make(map[string][]*Producer)
	l.DB.profileTopic = make(chan int, 1)
	l.DB.profileChan = make(chan int, 1)
	l.DB.TBChan = make(chan int64, 1)
	go l.dist()
}

func (l *NSQLookupd) dist() {
	var brokers []*Producer
	for {
		fmt.Printf("%v brokers: waiting for pubNum\n", len(brokers))
		<-l.DB.profileTopic
		brokers = l.DB.brokerList[l.DB.TPName]
		for _, b := range brokers {
			b.peerInfo.profileChan = make(chan int, 1)
			fmt.Printf("%v\n", b.peerInfo.RemoteAddress)
		}
		K := 1
		i := 0
		rate := l.DB.TPRate
		l.DB.keys = make([]int, len(l.DB.groups))
		kIndex := 0
		for k, _ := range l.DB.groups {
			l.DB.keys[kIndex] = k
			kIndex++
		}
		sort.Slice(l.DB.keys, func(i, j int) bool { return l.DB.keys[i] < l.DB.keys[j] })
		for _, b := range brokers {
			b.peerInfo.pubs = []int{}
			b.peerInfo.tpRate = 0
			b.peerInfo.rate = 0
		}
		cap := 0
		for K = 0; K < len(brokers); K++ {
			cap += brokers[K].peerInfo.rcap
			if cap >= rate {
				break
			}
		}
		K++
		fmt.Printf("!!!!!!~~~ We need K=%v brokers\n", K)
		time.Sleep(5 * time.Second)
		rate_free := rate
		for i = K - 1; i >= 0; i-- {
			if K > 1 {
				time.Sleep(30*time.Second)
			}
			rate_target := brokers[i].peerInfo.rcap
			if rate_free/(i+1) < rate_target {
				rate_target = rate_free / (i + 1)
			}
			ratio := float64(rate_target) / float64(l.DB.TPRate)
			if i == 0 {
				ratio = float64(1.0)
			}
			pubs, rate_add := l.assemble(ratio, rate_target)
			rate_free -= rate_add
			info := []byte(brokers[i].peerInfo.BroadcastAddress + ":" + strconv.Itoa(brokers[i].peerInfo.TCPPort))
			fmt.Printf("~~~~~~~~~~~~~!!!!!!!!!! migrate %v %v to broker[%v]\n", l.DB.TPName, len(pubs), i)
			l.sendMigrate(l.DB.initBroker, pubs, info)
			time.Sleep(3 * time.Second)

			brokers[i].peerInfo.pubs = append(brokers[i].peerInfo.pubs, pubs...)
			brokers[i].peerInfo.tpRate = rate_add
			brokers[i].peerInfo.groups = make(map[int]*CorGroup)
			for _, key := range l.DB.keys {
				g := l.DB.groups[key]
				brokers[i].peerInfo.groups[key] = &CorGroup{
					num:     0,
					index:   0,
					tmp_index: 0,
					pubs:    []int{},
					assigns: []int{},
				}
				j := g.index
				for ; j < g.tmp_index; j++ {
					//l.DB.mmDist[g.pubs[j]]=brokers[i]
					brokers[i].peerInfo.groups[key].num++
					brokers[i].peerInfo.groups[key].pubs = append(brokers[i].peerInfo.groups[key].pubs, g.pubs[j])
				}
				g.index = j
			}

		}
		waitTime := 0
		for i = K - 1; i >= 0; i-- {
			waitTime += len(brokers[i].peerInfo.pubs)
		}
		//time.Sleep(30*time.Second)
		fmt.Printf("~~~~~~~!!!!!! Start parallel fitting after %v\n", waitTime)
		if waitTime < 3000 {
			waitTime = 3000
		}
		time.Sleep(time.Duration(waitTime*10) * time.Millisecond)
		var wg sync.WaitGroup
		l.DB.curK = K
		l.DB.realK = K
		l.DB.curK2 = 0
		for i = K - 1; i >= 0; i-- {
			wg.Add(1)
			brokers[i].peerInfo.inProfile = 2
			go l.parFit(brokers[i], &wg)
		}
		wg.Wait()
		for _, g := range l.DB.groups {
			g.pubs = []int{}
			g.num = 0
			g.tmp_index = 0
			g.tmp_index2 = 0
			g.index = 0
		}
		l.DB.oriBroker = make(map[int]int)
		for i := 0; i < K; i++ {
			fmt.Printf("!!!!!!!!!~~~~ broker-%v: tpRate=%v, rcap=%v\n", i, brokers[i].peerInfo.tpRate, brokers[i].peerInfo.rcap)
			brokers[i].peerInfo.inProfile = 0
			brokers[i].peerInfo.rate = brokers[i].peerInfo.tpRate
			if brokers[i].peerInfo.rcap > 0 {
				continue
			}
			for _, key := range l.DB.keys {
				g := brokers[i].peerInfo.groups[key]
				if g.num == 0 {
					continue
				}
				g1 := l.DB.groups[key]
				g1.pubs = append(g1.pubs, g.pubs[g.tmp_index:]...)
				for _, p := range g.pubs[g.tmp_index:] {
					l.DB.oriBroker[p] = i
				}
				g1.num = len(g1.pubs)
				g.pubs = g.pubs[:g.tmp_index]
				g.num = len(g.pubs)
			}
		}
		rate_free = 0
		for _, g := range l.DB.groups {
			for _, p := range g.pubs {
				rate_free += l.DB.rates[p]
			}
		}
		rate = rate_free
		fmt.Printf("~~~~~~ Unassigned: %v\n", rate_free)
		var lastBroker *Producer
		realK := 0
		onemore := false
		for rate_free > 0 || onemore {
			for i := 0; i < K; i++ {
                        	brokers[i].peerInfo.rate = brokers[i].peerInfo.tpRate
                        }
			if onemore {
				K++
				realK++
				goto out1
			}
			cap = 0
			realK = 0
			for K = 0; K < len(brokers); K++ {
				if brokers[K].peerInfo.curProfile != -1 {
					realK++
				}
				cap += brokers[K].peerInfo.rcap
				if cap >= rate_free {
					break
				}
			}
			K++
out1:
			fmt.Printf("!!!!!!~~~ We need K=%v, realK=%v brokers to balance\n", K, realK)

			for i := K - 1; (i >= 0 && rate_free > 0) || onemore; i-- {
				if brokers[i].peerInfo.rcap == 0 {
					continue
				}
				tiny_target := brokers[i].peerInfo.rcap
				if l.DB.TPRate/realK < tiny_target {
					tiny_target = l.DB.TPRate / realK
				}
				if brokers[i].peerInfo.tpRate == 0 && rate_free < tiny_target {
					fmt.Printf("~~~~~~~!!!!!!!! last and migrate %v\n", rate_free)
					brokers[i].peerInfo.rate = brokers[i].peerInfo.tpRate
					lastBroker = brokers[i]
					ori := make([][]int, len(brokers))
					for j := 0; j < len(brokers); j++ {
						ori[j] = []int{}
					}
					brokers[i].peerInfo.groups = make(map[int]*CorGroup)
					for _, key := range l.DB.keys {
						g := l.DB.groups[key]
						brokers[i].peerInfo.groups[key] = &CorGroup{
							num:        0,
							index:      0,
							pubs:       []int{},
							tmp_index:  0,
							tmp_index2: 0,
						}
						j := g.index
						for ; j < g.num; j++ {
							//l.DB.mmDist[g.pubs[j]]=brokers[i]
							brokers[i].peerInfo.groups[key].num++
							brokers[i].peerInfo.groups[key].pubs = append(brokers[i].peerInfo.groups[key].pubs, g.pubs[j])
							ori[l.DB.oriBroker[g.pubs[j]]] = append(ori[l.DB.oriBroker[g.pubs[j]]], g.pubs[j])
						}
						brokers[i].peerInfo.groups[key].tmp_index2 = brokers[i].peerInfo.groups[key].num
						g.index = j
					}
					info := []byte(brokers[i].peerInfo.BroadcastAddress + ":" + strconv.Itoa(brokers[i].peerInfo.TCPPort))
					for j := 0; j < len(brokers); j++ {
						if len(ori[j]) > 0 {
							fmt.Printf("~~~~~!!!!!!!!!move %v pubs from %v\n", len(ori[j]),j)
							l.sendMigrate(brokers[j], ori[j], info)
							waitTime = len(ori[j])
							if waitTime < 3000 {
								waitTime = 3000
							}
							time.Sleep(time.Duration(waitTime*10) * time.Millisecond)
							//time.Sleep(3 * time.Second)
						}
					}
					brokers[i].peerInfo.rcap -= rate_free
					brokers[i].peerInfo.tpRate += rate_free
					vacancy := tiny_target - rate_free
					for j := 0; j < len(brokers) && vacancy > 0; j++ {
						if i == j || brokers[j].peerInfo.curProfile == -1 {
							continue
						}
						rate_migrate := brokers[j].peerInfo.tpRate - l.DB.TPRate/realK
						if vacancy < rate_migrate {
							rate_migrate = vacancy
						}
						if rate_migrate <= 0 {
							continue
						}
						pubs, rate_add := l.assembleMM(float64(rate_migrate)/float64(brokers[j].peerInfo.tpRate), rate_migrate, brokers[j])
						if len(pubs)==0 {
							continue
						}
						for _, key := range l.DB.keys {
							g := brokers[j].peerInfo.groups[key]
							j1 := 0
							for ; j1 < g.tmp_index; j1++ {
								brokers[i].peerInfo.groups[key].num++
								brokers[i].peerInfo.groups[key].pubs = append(brokers[i].peerInfo.groups[key].pubs, g.pubs[j1])
							}
							g.index = 0
							g.pubs = g.pubs[g.tmp_index:]
							g.num = len(g.pubs)
						}
						fmt.Printf("~~~~~!!!!!!!!!move %v pubs from %v to fill vacancy\n", len(pubs),j)
						l.sendMigrate(brokers[j], pubs, info)
						time.Sleep(3 * time.Second)
						brokers[i].peerInfo.rcap -= rate_add
						brokers[i].peerInfo.tpRate += rate_add

						brokers[j].peerInfo.rate = brokers[j].peerInfo.tpRate
						brokers[j].peerInfo.rcap += rate_add
						brokers[j].peerInfo.tpRate -= rate_add
					}
					rate_free = 0
					if onemore {
						onemore=false
					}
				} else {
					brokers[i].peerInfo.rate = brokers[i].peerInfo.tpRate
					rate_target := brokers[i].peerInfo.rcap
					if rate_free < rate_target {
						rate_target = rate_free
					}
					_, rate_add := l.assemble(float64(rate_target)/float64(rate), rate_target)
					rate_free -= rate_add
					fmt.Printf("~~~~~~~!!!!!!!Put %v at %v\n", rate_add, i)
					ori := make([][]int, len(brokers))
					for j := 0; j < len(brokers); j++ {
						ori[j] = []int{}
					}
					if brokers[i].peerInfo.groups == nil {
						brokers[i].peerInfo.groups = make(map[int]*CorGroup)
					}
					for _, key := range l.DB.keys {
						g := l.DB.groups[key]
						if _, ok := brokers[i].peerInfo.groups[key]; !ok {
							brokers[i].peerInfo.groups[key] = &CorGroup{
								num:        0,
								index:      0,
								pubs:       []int{},
								tmp_index:  0,
								tmp_index2: 0,
							}
						}
						j := g.index
						for ; j < g.tmp_index; j++ {
							brokers[i].peerInfo.groups[key].num++
							brokers[i].peerInfo.groups[key].pubs = append(brokers[i].peerInfo.groups[key].pubs, g.pubs[j])
							ori[l.DB.oriBroker[g.pubs[j]]] = append(ori[l.DB.oriBroker[g.pubs[j]]], g.pubs[j])
						}
						g.index = j
					}

					info := []byte(brokers[i].peerInfo.BroadcastAddress + ":" + strconv.Itoa(brokers[i].peerInfo.TCPPort))
					//fmt.Printf("%v %v %v\n", ori[0], len(ori[0]), rate_add)
					for j := 0; j < len(brokers); j++ {
						if len(ori[j]) > 0 {
							fmt.Printf("~~~~~!!!!!!!!!move %v pubs from %v\n", len(ori[j]),j)
							l.sendMigrate(brokers[j], ori[j], info)
							waitTime = len(ori[j])
							if waitTime < 3000 {
								waitTime = 3000
							}
							time.Sleep(time.Duration(waitTime*10) * time.Millisecond)
						}
					}
					brokers[i].peerInfo.rcap -= rate_add
					brokers[i].peerInfo.tpRate += rate_add
				}
			}

			for bindex, broker := range brokers {
				if broker.peerInfo.tpRate > 0 {
					broker.peerInfo.pubs, _ = l.assembleMM(float64(1.0), broker.peerInfo.tpRate, broker)
				}
				fmt.Printf("~~~~%v %v\n", bindex, broker.peerInfo.tpRate)
				if broker.peerInfo.tpRate > 0 {
					for _, key := range l.DB.keys {
                                        	fmt.Printf("%v~%v !", key, broker.peerInfo.groups[key].num)
					}
					fmt.Printf("\n")
				}
			}
			time.Sleep(30 * time.Second)
			l.DB.curK=0
			l.DB.curK2=0
			firstNew := -1
			for i = len(brokers)-1; i >= 0; i-- {
				if brokers[i].peerInfo.tpRate <= 0 {
					continue
				}
				fmt.Printf("!!!!! broker[%v]: old=%v, new=%v\n", i, brokers[i].peerInfo.rate, brokers[i].peerInfo.tpRate)
				if brokers[i].peerInfo.tpRate <= brokers[i].peerInfo.rate && i < firstNew {
					go l.oneFit(brokers[i])
					fmt.Printf("one fit sent, %v %v\n", i, firstNew)
					brokers[i].peerInfo.inProfile = 1
					brokers[i].peerInfo.lastLatency = 10000000000
					l.DB.curK++
					continue
				}
				wg.Add(1)
				//go l.parFit2(brokers[i], &wg)
				go l.parFit(brokers[i], &wg)
				if firstNew < 0 {
					firstNew = i
				}
				brokers[i].peerInfo.inProfile = 2
				brokers[i].peerInfo.lastLatency = 100000000000
				l.DB.curK++
			}
			//l.DB.realK = l.DB.curK
			l.DB.realK = K
			wg.Wait()
			for _, g := range l.DB.groups {
				g.pubs = []int{}
				g.num = 0
				g.tmp_index = 0
				g.index = 0
			}
			finish := true
			l.DB.oriBroker = make(map[int]int)
			for i := 0; i < K; i++ {
				brokers[i].peerInfo.inProfile = 0
				if brokers[i].peerInfo.rcap > 0 {
					continue
				}
				for _, key := range l.DB.keys {
					g := brokers[i].peerInfo.groups[key]
					if g.num == 0 {
						continue
					}
					g1 := l.DB.groups[key]
					g1.pubs = append(g1.pubs, g.pubs[g.tmp_index:]...)
					for _, p := range g.pubs[g.tmp_index:] {
						l.DB.oriBroker[p] = i
					}
					if g.tmp_index2 == 0 && len(g.pubs[g.tmp_index:]) > 0 {
						finish = false
					}
					if g.tmp_index2 > 0 && g.tmp_index < g.tmp_index2 {
						finish = false
					}
					g1.num = len(g1.pubs)
					g.pubs = g.pubs[:g.tmp_index]
					g.num = len(g.pubs)
				}
			}
			onemore = false
			if finish && l.DB.lastTail > 1000000 {
				finish = false
				onemore = true
			}
			if finish {
				ori := make([][]int, K)
				for j := 0; j < K; j++ {
					ori[j] = []int{}
				}
				for _, g := range l.DB.groups {
					for _, p := range g.pubs {
						ori[l.DB.oriBroker[p]] = append(ori[l.DB.oriBroker[p]], p)
					}
				}
				for j := 0; j < K; j++ {
					info := []byte(brokers[j].peerInfo.BroadcastAddress + ":" + strconv.Itoa(brokers[j].peerInfo.TCPPort))
					if len(ori[j]) > 0 {
						l.sendMigrate(lastBroker, ori[j], info)
						rate_migrate := 0
						for _, p := range ori[j] {
							rate_migrate += l.DB.rates[p]
						}
						brokers[j].peerInfo.tpRate += rate_migrate
						brokers[j].peerInfo.rcap -= rate_migrate
						time.Sleep(3 * time.Second)
					}
				}
				break
			}
			rate_free = 0
			for _, g := range l.DB.groups {
				for _, p := range g.pubs {
					rate_free += l.DB.rates[p]
				}
			}
			rate = rate_free
		}

		for i := 0; i < len(brokers); i++ {
			finalRate := 0
			for _, p := range brokers[i].peerInfo.pubs {
				finalRate += l.DB.rates[p]
			}
			fmt.Printf("broker[%v]: %v %v\n", i, brokers[i].peerInfo.tpRate, finalRate)
		}
	}
}
func (l *NSQLookupd) oneFit(broker *Producer) {
	//l.sendProfile(broker, broker.peerInfo.pubs)
	l.sendProfileFirst(broker, broker.peerInfo.pubs)
	_ = <-broker.peerInfo.profileChan
	broker.peerInfo.inProfile = 0
	
}

func (l *NSQLookupd) parFit(broker *Producer, wg *sync.WaitGroup) {
	l.sendProfileFirst(broker, broker.peerInfo.pubs)
	broker.peerInfo.curProfile = len(broker.peerInfo.pubs)
	broker.peerInfo.prevProfile = broker.peerInfo.curProfile
	exceed := <-broker.peerInfo.profileChan
	if exceed == 2 {
		l.sendUnProfile(broker, broker.peerInfo.pubs)
		time.Sleep(30 * time.Second)
		rate_assign := l.fitMM(broker, 0, broker.peerInfo.tpRate)
		broker.peerInfo.tpRate = rate_assign
		broker.peerInfo.rcap = 0
	} else {
		broker.peerInfo.rcap -= broker.peerInfo.tpRate
	}
	broker.peerInfo.inProfile = 0
	wg.Done()
}
func (l *NSQLookupd) parFit2(broker *Producer, wg *sync.WaitGroup) {
	//broker.peerInfo.pubs, _ = l.assembleMM(float64(1.0), broker.peerInfo.tpRate, broker)
	l.sendProfileFirst(broker, broker.peerInfo.pubs)
	broker.peerInfo.curProfile = len(broker.peerInfo.pubs)
	broker.peerInfo.prevProfile = broker.peerInfo.curProfile
	exceed := <-broker.peerInfo.profileChan
	if exceed == 2 {
		l.sendUnProfile(broker, broker.peerInfo.pubs)
		time.Sleep(30 * time.Second)
		rate_assign := l.fitMM(broker, broker.peerInfo.rate, broker.peerInfo.tpRate)
		broker.peerInfo.tpRate = rate_assign
		broker.peerInfo.rcap = 0
	} else {
		broker.peerInfo.rcap -= broker.peerInfo.tpRate
	}
	broker.peerInfo.inProfile = 0
	wg.Done()
}

func (l *NSQLookupd) assemble(ratio float64, rate int) ([]int, int) {
	if ratio > 1.0 {
		ratio = 1.0
	}
	pubs := []int{}
	rate_add := 0
	index := 0
	for _, key := range l.DB.keys {
		g := l.DB.groups[key]
		i := g.index
		num := int(float64(g.num) * ratio)
		for ; i < g.num && i < (g.index+num); i++ {
			pubs = append(pubs, g.pubs[i])
			rate_add += l.DB.rates[g.pubs[i]]
		}
		g.tmp_index = i
	}
	if rate_add < (rate*8)/10 {
		r1 := rand.New(rand.NewSource(time.Now().UnixNano()))
		index = 0
		for rate_add < rate {
			index = r1.Intn(len(l.DB.keys))
			g := l.DB.groups[l.DB.keys[index]]
			for g.tmp_index >= g.num {
				index = (index + 1) % len(l.DB.groups)
				g = l.DB.groups[l.DB.keys[index]]
			}
			pubs = append(pubs, g.pubs[g.tmp_index])
			rate_add += l.DB.rates[g.pubs[g.tmp_index]]
			g.tmp_index++
		}
	}
	return pubs, rate_add
}

func (l *NSQLookupd) assembleMM(ratio float64, rate int, broker *Producer) ([]int, int) {
	if ratio > 1.0 {
		ratio = 1.0
	}
	pubs := []int{}
	rate_add := 0
	poisson := false
	for _, key := range l.DB.keys {
		g := broker.peerInfo.groups[key]
		i := g.index
		num := int(float64(g.num) * ratio)
		if g.num == 1 {
			poisson = true
		}
		for ; i < g.num && i < (g.index+num); i++ {
			pubs = append(pubs, g.pubs[i])
			rate_add += l.DB.rates[g.pubs[i]]
		}
		g.tmp_index = i
	}
	//if rate_add < (rate*8)/10 {
	if poisson {
		r1 := rand.New(rand.NewSource(time.Now().UnixNano()))
                index := 0
                for rate_add < rate {
                        index = r1.Intn(len(l.DB.keys))
                        g := broker.peerInfo.groups[l.DB.keys[index]]
                        for g.tmp_index >= g.num {
                                index = (index + 1) % len(l.DB.groups)
                                g = broker.peerInfo.groups[l.DB.keys[index]]
                        }
                        pubs = append(pubs, g.pubs[g.tmp_index])
                        rate_add += l.DB.rates[g.pubs[g.tmp_index]]
                        g.tmp_index++
                }
	}
	return pubs, rate_add
}

func (l *NSQLookupd) fitMM(broker *Producer, left int, right int) int {
	fmt.Printf("Fit [%v %v]\n", left, right)
	if left == right {
		pub_add, rate_add := l.assembleMM(float64(left)/float64(broker.peerInfo.tpRate), left, broker)
		l.DB.Lock()
		l.DB.curK++
		l.DB.Unlock()
		l.sendProfile(broker, pub_add)
		broker.peerInfo.prevProfile = broker.peerInfo.curProfile
		broker.peerInfo.curProfile = len(pub_add)
		exceed := <-broker.peerInfo.profileChan
		if exceed == 2 {
			l.sendUnProfile(broker, pub_add)
			return -1
		} else {
			return rate_add
		}
	}
	mid := left + (right-left)/2
	pub_add, rate_add := l.assembleMM(float64(mid)/float64(broker.peerInfo.tpRate), mid, broker)
	l.DB.Lock()
	l.DB.curK++
	l.DB.Unlock()
	l.sendProfile(broker, pub_add)
	broker.peerInfo.prevProfile = broker.peerInfo.curProfile
	broker.peerInfo.curProfile = len(pub_add)
	exceed := <-broker.peerInfo.profileChan
	if exceed == 3 {
		for _, key := range l.DB.keys {
                	g := broker.peerInfo.groups[key]
                	g.tmp_index = g.index
        	}
		broker.peerInfo.curProfile = -1
		return 0	
	} else if exceed == 2 {
		l.sendUnProfile(broker, pub_add)
		return l.fitMM(broker, left, rate_add)
	} else if exceed == 0 {
		index := l.fitMM(broker, rate_add, right)
		if index == -1 {
			return rate_add
		} else {
			return index
		}
	} else {
		return rate_add
	}
}

func (l *NSQLookupd) sendProfileFirst(broker *Producer, pub_add []int) {
        index := 0
        //l.DB.curK2 = 0
        for i, p := range l.DB.brokerList[l.DB.TPName] {
                if p.peerInfo.id == broker.peerInfo.id {
                        index = i
                        break
                }
        }
        pubs := [][]byte{}
        pubs = append(pubs, []byte(l.DB.TPName))
        pubs = append(pubs, []byte(strconv.Itoa(len(pub_add))))
        pubs = append(pubs, []byte(strconv.Itoa(index)))
	pubs = append(pubs, []byte("FIRST"))
        cmd := nsq.Profile(pubs)
        cmd.WriteTo(broker.peerInfo.backup.client)
        x := 0
        for {
                params := [][]byte{}
                for y := 0; y < 1000 && x < len(pub_add); y++ {
                        params = append(params, []byte(strconv.Itoa(pub_add[x])))
                        x++
                }
                cmd := nsq.Profile(params)
                cmd.WriteTo(broker.peerInfo.backup.client)
                if x >= len(pub_add) {
                        break
                }
        }
}

func (l *NSQLookupd) sendProfile(broker *Producer, pub_add []int) {
	index := 0
	//l.DB.curK2 = 0
	for i, p := range l.DB.brokerList[l.DB.TPName] {
		if p.peerInfo.id == broker.peerInfo.id {
			index = i
			break
		}
	}
	pubs := [][]byte{}
	pubs = append(pubs, []byte(l.DB.TPName))
	pubs = append(pubs, []byte(strconv.Itoa(len(pub_add))))
	pubs = append(pubs, []byte(strconv.Itoa(index)))
	cmd := nsq.Profile(pubs)
	cmd.WriteTo(broker.peerInfo.backup.client)
	x := 0
	for {
		params := [][]byte{}
		for y := 0; y < 1000 && x < len(pub_add); y++ {
			params = append(params, []byte(strconv.Itoa(pub_add[x])))
			x++
		}
		cmd := nsq.Profile(params)
		cmd.WriteTo(broker.peerInfo.backup.client)
		if x >= len(pub_add) {
			break
		}
	}
}
func (l *NSQLookupd) sendUnProfile(broker *Producer, pub_add []int) {
	pubs := [][]byte{}
	pubs = append(pubs, []byte(l.DB.TPName))
	pubs = append(pubs, []byte(strconv.Itoa(len(pub_add))))
	cmd := nsq.UnProfile(pubs)
	cmd.WriteTo(broker.peerInfo.backup.client)
	x := 0
	for {
		params := [][]byte{}
		for y := 0; y < 1000 && x < len(pub_add); y++ {
			params = append(params, []byte(strconv.Itoa(pub_add[x])))
			x++
		}
		cmd := nsq.UnProfile(params)
		cmd.WriteTo(broker.peerInfo.backup.client)
		if x >= len(pub_add) {
			break
		}
	}
}
func (l *NSQLookupd) sendMigrate(broker *Producer, pub_add []int, info []byte) {
	pubs := [][]byte{}
	pubs = append(pubs, []byte(l.DB.TPName))
	pubs = append(pubs, info)
	pubs = append(pubs, []byte(strconv.Itoa(len(pub_add))))
	cmd := nsq.Migrate(pubs)
	cmd.WriteTo(broker.peerInfo.backup.client)
	x := 0
	for {
		params := [][]byte{}
		for y := 0; y < 1000 && x < len(pub_add); y++ {
			params = append(params, []byte(strconv.Itoa(pub_add[x])))
			x++
		}
		cmd := nsq.Migrate(params)
		cmd.WriteTo(broker.peerInfo.backup.client)
		if x >= len(pub_add) {
			break
		}
	}
}

//YAO
//Initilize the high_priority topic list
func (l *NSQLookupd) grabHighPriorityTopics(fileName string) {

	content, err := ioutil.ReadFile(fileName)
	if err != nil {
		println("YAO:READ ERROR")
	}
	lines := strings.Split(string(content), "\n")

	for _, line := range lines {
		temp := strings.Split(line, " ")
		topicName := temp[0]
		key := Registration{"high_priority_topic", topicName, ""}
		l.DB.AddRegistration(key)
	}

}

func (l *NSQLookupd) RealTCPAddr() *net.TCPAddr {
	l.RLock()
	defer l.RUnlock()
	return l.tcpListener.Addr().(*net.TCPAddr)
}

func (l *NSQLookupd) RealHTTPAddr() *net.TCPAddr {
	l.RLock()
	defer l.RUnlock()
	return l.httpListener.Addr().(*net.TCPAddr)
}

func (l *NSQLookupd) Exit() {
	if l.tcpListener != nil {
		l.tcpListener.Close()
	}

	if l.httpListener != nil {
		l.httpListener.Close()
	}
	l.waitGroup.Wait()
}
