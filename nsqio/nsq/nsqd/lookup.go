//+build rtm

package nsqd

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math"
	"net"
	"os"
	"strconv"
	"strings"
	"syscall"
	"time"
	"sort"
	"log"
	"runtime"

	"github.com/WU-CPSL/RTM-0.1/nsqio/go-nsq"
	"github.com/WU-CPSL/RTM-0.1/nsqio/nsq/internal/version"
	"github.com/WU-CPSL/RTM-0.1/nsqio/rate"
	//"github.com/gonum/stat"
)

func connectCallback(n *NSQD, hostname string, syncTopicChan chan *lookupPeer) func(*lookupPeer) {
	return func(lp *lookupPeer) {

		ci := make(map[string]interface{})
		ci["version"] = version.Binary
		//strs := strings.Split(n.getOpts().ProducerAddress, AddressSeparator)
		ci["tcp_port"] = n.RealTCPAddr().Port
		//ci["producer_address"] = strs[0]
		ci["http_port"] = n.RealHTTPAddr().Port
		ci["hostname"] = hostname
		ci["broadcast_address"] = n.getOpts().BroadcastAddress
		//strs := strings.Split(n.getOpts().BroadcastAddress, AddressSeparator)
		//ci["broadcast_address"] = strs[0]
		//ci["broadcast_port"], _ = strconv.Atoi(strs[1])
		//ci["connection_type"] = n.getOpts().ConnType

		//yao
		//add priority intot
		ci["daemon_priority"] = n.getOpts().DaemonPriority

		println("YAO:", n.getOpts().DaemonPriority)

		cmd, err := nsq.Identify(ci)
		if err != nil {
			lp.Close()
			return
		}
		resp, err := lp.Command(cmd)
		if err != nil {
			n.logf("LOOKUPD(%s): ERROR %s - %s", lp, cmd, err)
		} else if bytes.Equal(resp, []byte("E_INVALID")) {
			n.logf("LOOKUPD(%s): lookupd returned %s", lp, resp)
		} else {
			err = json.Unmarshal(resp, &lp.Info)
			if err != nil {
				n.logf("LOOKUPD(%s): ERROR parsing response - %s", lp, resp)
			} else {
				n.logf("LOOKUPD(%s): peer info %+v", lp, lp.Info)
			}
		}

		go func() {
			syncTopicChan <- lp
		}()
	}
}
func connectCallback2(n *NSQD, hostname string) func(*lookupPeer) {
        return func(lp *lookupPeer) {

                ci := make(map[string]interface{})
                ci["version"] = version.Binary
                ci["tcp_port"] = n.RealTCPAddr().Port
                ci["http_port"] = n.RealHTTPAddr().Port
                ci["hostname"] = hostname
                ci["broadcast_address"] = n.getOpts().BroadcastAddress

                ci["daemon_priority"] = "BACKUP"

                println("YAO:", n.getOpts().DaemonPriority)

                cmd, err := nsq.Identify(ci)
                if err != nil {
                        lp.Close()
                        return
                }
                resp, err := lp.Command(cmd)
		if err != nil {
                        n.logf("LOOKUPD(%s): ERROR %s - %s", lp, cmd, err)
                } else if bytes.Equal(resp, []byte("E_INVALID")) {
                        n.logf("LOOKUPD(%s): lookupd returned %s", lp, resp)
                } else {
                        err = json.Unmarshal(resp, &lp.Info)
                        if err != nil {
                                n.logf("LOOKUPD(%s): ERROR parsing response - %s", lp, resp)
                        } else {
                                n.logf("LOOKUPD(%s): peer info %+v", lp, lp.Info)
                        }
                }

        }
}


//RTM
func getCPUSample2() (idle, total uint64) {
	contents, err := ioutil.ReadFile("/proc/stat")
	if err != nil {
		return
	}
	lines := strings.Split(string(contents), "\n")
	for _, line := range lines {
		fields := strings.Fields(line)
		if fields[0] == "cpu"  {
			numFields := len(fields)
			for i := 1; i < numFields; i++ {
				val, err := strconv.ParseUint(fields[i], 10, 64)
				if err != nil {
					fmt.Println("Error: ", i, fields[i], err)
				}
				total += val // tally up all the numbers to get total ticks
				if i == 4 {  // idle is the 5th field in the cpu line
					idle = val
				}
			}
			return
		}
	}
	return
}
func getCPUSample() (idle, total uint64) {
        contents, err := ioutil.ReadFile("/proc/stat")
        if err != nil {
                return
        }
        lines := strings.Split(string(contents), "\n")
	for j:=9; j<=10; j++ {
        	fields := strings.Fields(lines[j])
        	numFields := len(fields)
                        for i := 1; i < numFields; i++ {
                                val, err := strconv.ParseUint(fields[i], 10, 64)
                                if err != nil {
                                        fmt.Println("Error: ", i, fields[i], err)
                                }
                                total += val // tally up all the numbers to get total ticks
                                if i == 4 {  // idle is the 5th field in the cpu line
                                        idle += val
                                }
                        }
	}
        return
}

func (n *NSQD) lookupLoop() {
	var lookupPeers []*lookupPeer
	var lookupAddrs []string
	syncTopicChan := make(chan *lookupPeer)
	connect := true

	hostname, err := os.Hostname()
	if err != nil {
		n.logf("FATAL: failed to get hostname - %s", err)
		os.Exit(1)
	}

	//RTM
	var readers []*bufio.Reader
	var tickerCPU <-chan time.Time
	var tickerCPU_P <-chan time.Time
	var tickerCPU_H <-chan time.Time
	/*tickerCPU = time.Tick(1000 * time.Millisecond)
	if n.getOpts().DaemonPriority == "HIGH" {
		tickerCPU_P = time.Tick(10 * time.Millisecond)
	} else {
		tickerCPU_H = time.Tick(10 * time.Millisecond)
	}*/
	winSize := 3000
	rateWindSize := 1000
	CPUStats := make([]float64, winSize)
	//lastMsgCount := uint64(0)
	msgStats := make([]uint64, winSize)
	for i := 0; i < winSize; i++ {
		CPUStats[i] = float64(0)
		msgStats[i] = uint64(0)
	}
	//CPUCount := 0
	//CPUMax := float64(0)
	CPUAvg := float64(0)
	CPUVar := float64(0)
	//last_CPUAvg := float64(0)
	//last_CPUVar := float64(0)
	CPUCur := float64(0)
	CPUIndex := 0
	//CPUSkew := float64(0)
	//CPUKurt := float64(0)

	rateIndex := 0
	var rateTicker <-chan time.Time
	//rateTicker = time.Tick(10 * time.Millisecond)

	/*var byteMeasure []byte
	measureNum := 0
	var byteCPU []byte
	reportNum := 0*/

	reportChan := make(chan int, 1)
	go func() {
		time.Sleep(1 * time.Second)
		reportChan <- 1
	}()
	load := &syscall.Rusage{}
	syscall.Getrusage(syscall.RUSAGE_SELF, load)
	last_cpu := int(load.Utime.Nano() + load.Stime.Nano())
	last_time := time.Now().UnixNano()
	cpu := 0
	this_time := time.Now().UnixNano()

	idle0, total0 := getCPUSample()
	idle1, total1 := getCPUSample()
	idleTicks := float64(idle1 - idle0)
	totalTicks := float64(total1 - total0)
	//cpuUsage := (totalTicks - idleTicks) / totalTicks

	// for announcements, lookupd determines the host automatically

	ticker := time.Tick(15 * time.Second)
	for {
		if connect {
			for _, host := range n.getOpts().NSQLookupdTCPAddresses {
				if in(host, lookupAddrs) {
					continue
				}
				n.logf("LOOKUP(%s): adding peer", host)
				lookupPeer := newLookupPeer(host, n.getOpts().MaxBodySize, n.getOpts().Logger,
					connectCallback(n, hostname, syncTopicChan))
				lookupPeer.Command(nil) // start the connection
				lookupPeers = append(lookupPeers, lookupPeer)
				lookupAddrs = append(lookupAddrs, host)
				//RTM
				readers = append(readers, bufio.NewReader(lookupPeer))
			}
			for _, host := range n.getOpts().NSQLookupdTCPAddresses {
                                n.logf("LOOKUP(%s): adding peer", host)
                                lookupPeer := newLookupPeer(host, n.getOpts().MaxBodySize, n.getOpts().Logger,
                                        connectCallback2(n, hostname))
                                lookupPeer.Command(nil) // start the connection
                                lookupPeers = append(lookupPeers, lookupPeer)
                                lookupAddrs = append(lookupAddrs, host)
                                //RTM
                                readers = append(readers, bufio.NewReader(lookupPeer))
                        }
			n.lookupPeers.Store(lookupPeers)
			connect = false
			//RTM.profile
			go func(){
				for {
					line, _ := readers[1].ReadString('\n')
                                	line = strings.TrimSpace(line)
                                	params := strings.Split(line, " ")

                                	if params[0] == "PROFILE" {
						runtime.GC()
						time.Sleep(3*time.Second)
                                        	topic:= params[1]
						t:=n.topicMap[topic]
						proNum := 0
						for {
							t.RLock()
							cLen := len(t.channelMap)
							t.RUnlock()
							if cLen > 0 {
								break
							} else {
								time.Sleep(1*time.Second)
							}
						}
						numP, _ := strconv.Atoi(params[2])
						brokerIndex := params[3]
						if len(params) > 4 {
							t.inDC = false
						} else {
							//t.inDC = true
							t.inDC = false
						}
						fmt.Printf("Recv profile %v %v %v\n", topic, brokerIndex, numP)
						curP := 0
						for curP < numP {
							line, _ = readers[1].ReadString('\n')
							line = strings.TrimSpace(line)
							params = strings.Split(line, " ")
							for _, px := range params[1:] {
                                                           pub,_:=strconv.Atoi(px)
                                                           if p, ok:= t.pubClients[pub]; ok {
                                                                    	p.profile = true
                                                                       	p.s1Index = 0
                                                                       	p.s2Index = 0
                                                                       	p.s3Index = 0
									proNum++
                                                           }
							   curP++
                                                           t.allPubClients[pub].profileChan <- topic
							   t.allPubClients[pub].profile = true
							   //t.allPubClients[pub].arrNums = 0
                                                	}
						}
						n.RLock()
                        			realTopics := make([]*Topic, 0, len(n.topicMap))
                        			for _, tx := range n.topicMap {
                                			realTopics = append(realTopics, tx)
                        			}
                        			n.RUnlock()
                        			for _, tx := range realTopics {
							if tx == t {
								continue
							} else {
								continue
							}
							for _, p := range tx.pubClients {
								p.profile = true
								p.s1Index = 0
								p.s2Index = 0
								p.s3Index = 0
								p.profileChan <- tx.name
							}
						}

						time.Sleep(70*time.Second)
						wait := true
                        			for wait {
                                			wait = false
                                			for _,p:= range t.pubClients {
                                        			if p.s1Index < p.sNum {
                                                			time.Sleep(1*time.Second)
                                                			wait=true
                                               			 	break
                                        			}
                                			}
                        			}

						fmt.Printf("Start calculation from %v conns\n", len(t.pubClients))	
						/*for _, c:= range t.allPubClients {
							c.CongestChan <- topic
							//c.profile = false
						}*/
						ct1:=time.Now().UnixNano()
						keys := make([]int, len(t.pubClients))
                        			kIndex := 0
                        			for k, _ := range t.pubClients {
                                			keys[kIndex]=k
                                			kIndex++
                        			}
                        			sort.Slice(keys, func(i, j int) bool {return keys[i] < keys[j]})
                        			delays1:=[]int64{}
                        			begin:=(t.pubClients[keys[0]].sNum/6)
                        			end:=(t.pubClients[keys[0]].sNum/6)*5
                        			fmt.Printf("take %v to %v\n", begin, end)
                        			for _, k:= range keys {
							if !t.pubClients[k].profile {
								continue
							}
                                			c := t.pubClients[k]
							if c.s1Index < end {
								fmt.Printf("!!!!!!!!!!!! not enough samples %v\n", c.s1Index)
							}
                                			for i:=begin; i< end; i++ {
                                        			delays1 = append(delays1, c.s1Times[i])
                                			}
                        			}
						delays2:=[]int64{}
                        			for _, k:= range keys {
							if !t.pubClients[k].profile {
								continue
							}
                                			c := t.pubClients[k]
                                			for i:=begin; i< end; i++ {
                                        			delays2 = append(delays2, c.s2Times[i])
                                			}
                        			}

                        			delays3 :=[]int64{}
                        			for _, k:= range keys {
							if !t.pubClients[k].profile {
								continue
							}
                                			c := t.pubClients[k]
                                			for i:=begin; i< end; i++ {
                                       			 	delays3 = append(delays3, c.s3Times[i])
                                			}
                        			}

                       				log.Printf("total=%v\n", len(delays1))
                        			log.Printf("total=%v\n", len(delays2))
                        			log.Printf("total=%v\n", len(delays3))
                        			s := make([]int64, len(delays1))
						for i:=0; i<len(s); i++ {
                                			s[i]= delays1[i]/2+ delays2[i]+ delays3[i]/2
                        			}
						tail:= s[int(float64(len(s))*0.99)]
						params := [][]byte{}
                        			sort.Slice(s, func(i,j int) bool {return s[i]<s[j]})
                        			log.Printf("total=%v %v\n", len(s), n.startTime)
                        			log.Printf("%v:50=%v, 95=%v, 99=%v\n", brokerIndex, s[int(float64(len(s))*0.5)], s[int(float64(len(s))*0.95)], s[int(float64(len(s))*0.99)])
						/*tail= s[int(float64(len(s))*0.99)]
						if tail < 1000000 && tail > 800000 {
							time.Sleep(10*time.Second)
						}
						params = append(params, []byte(brokerIndex))
                                                params = append(params, []byte(strconv.FormatInt(tail, 10)))*/
						
						i:=0
                                                for {
                                                        params = [][]byte{}
                                                        params = append(params, []byte("profile"))
                                                        params = append(params, []byte(t.name))
							params = append(params, []byte(brokerIndex))
                                                        for j:= 0; j<1000 && i<len(s); j++ {
                                                                params=append(params, []byte(strconv.FormatInt(s[i], 10)))
                                                                i++
                                                        }
                                                        cmd := nsq.ReportTP(params)
                                                        lookupPeers[1].CommandNoResp(cmd)
                                                        time.Sleep(30*time.Millisecond)
                                                        if i == len(s) {
                                                                break
                                                        }
                                                }
						params = [][]byte{}
                                                params = append(params, []byte("profile"))
                                                params = append(params, []byte(t.name))
                                                params = append(params, []byte(brokerIndex))
						params = append(params, []byte("end"))
						params = append(params, []byte(strconv.FormatInt(s[int(float64(len(s))*0.99)], 10)))
						cmd := nsq.ReportTP(params)
                                                lookupPeers[1].CommandNoResp(cmd)
						fmt.Printf("~~~~~~~!!!!!! calc time= %v\n", time.Now().UnixNano()-ct1)
						continue

						/******************************/
						for _, tx:= range realTopics {
						if tx == t {
							continue
						} else {
							continue
						}
						keys = make([]int, len(tx.pubClients))
                                                kIndex = 0
                                                for k, _ := range tx.pubClients {
                                                        keys[kIndex]=k
                                                        kIndex++
                                                }
                                                sort.Slice(keys, func(i, j int) bool {return keys[i] < keys[j]})
                                                delays1=[]int64{}
                                                begin=(tx.pubClients[keys[0]].sNum/6)
                                                end=(tx.pubClients[keys[0]].sNum/6)*5
                                                for _, k:= range keys {
                                                        if !tx.pubClients[k].profile {
                                                                continue
                                                        }
                                                        c := tx.pubClients[k]
                                                        for i:=begin; i< end; i++ {
                                                                delays1 = append(delays1, c.s1Times[i])
                                                        }
                                                }
						delays2=[]int64{}
                                                for _, k:= range keys {
                                                        if !tx.pubClients[k].profile {
                                                                continue
                                                        }
                                                        c := tx.pubClients[k]
                                                        for i:=begin; i< end; i++ {
                                                                delays2 = append(delays2, c.s2Times[i])
                                                        }
                                                }

                                                delays3 =[]int64{}
                                                for _, k:= range keys {
                                                        if !tx.pubClients[k].profile {
                                                                continue
                                                        }
                                                        c := tx.pubClients[k]
                                                        for i:=begin; i< end; i++ {
                                                                delays3 = append(delays3, c.s3Times[i])
                                                        }
                                                }

						s = make([]int64, len(delays1))
                                                for i:=0; i<len(s); i++ {
                                                        s[i]= delays1[i]/2+ delays2[i]+delays3[i]/2
                                                }
                                                sort.Slice(s, func(i,j int) bool {return s[i]<s[j]})
                                                log.Printf("total=%v\n", len(s))
                                                log.Printf("50=%v, 95=%v, 99=%v\n", s[int(float64(len(s))*0.5)], s[int(float64(len(s))*0.95)], s[int(float64(len(s))*0.99)])
                                                tail = s[int(float64(len(s))*0.99)]
                                        	params = append(params, []byte(strconv.FormatInt(tail, 10)))

						}
						cmd = nsq.ReportTime(params)
                                                lookupPeers[1].CommandNoResp(cmd)

					} else if params[0] == "UNPROFILE" {
						topic:= params[1]
                                                t:=n.topicMap[topic]
						numP, _:=strconv.Atoi(params[2])
                                                fmt.Printf("Recv unprofile %v %v\n", topic, numP)
						curP := 0
                                                for curP < numP {
                                                        line, _ = readers[1].ReadString('\n')
                                                        line = strings.TrimSpace(line)
                                                        params = strings.Split(line, " ")

                                               		for _, px:= range params[1:] {
                                                        	pub,_:=strconv.Atoi(px)
                                                        	t.allPubClients[pub].CongestChan <- topic
								t.allPubClients[pub].profile = false
								curP++
                                                	}
						}

					} else if params[0] == "MIGRATE" {
						topic:= params[1]
						t:=n.topicMap[topic]
						newDist:=params[2]
						numP, _ := strconv.Atoi(params[3])
						fmt.Printf("Recv migrate %v %v\n", topic, numP)
						mt1:=time.Now().UnixNano()
						curP := 0
                                                for curP < numP {
                                                        line, _ = readers[1].ReadString('\n')
                                                        line = strings.TrimSpace(line)
                                                        params = strings.Split(line, " ")

							for _, px := range params[1:] {
								time.Sleep(1*time.Millisecond)
								pub, _:= strconv.Atoi(px)
								//fmt.Printf("%v %v %v\n", pub, curP, numP)
								t.allPubClients[pub].MigrateChan <- newDist
								t.allPubClients[pub].profile=false
								t.clientLock.Lock()
								delete(t.clients, t.allPubClients[pub].ID)
								delete(t.allPubClients, pub)
                                                        	delete(t.pubClients, pub)
								t.clientLock.Unlock()
								curP++
							}
						}
						fmt.Printf("~~~~~!!!!!!!!!! migrate time= %v\n", time.Now().UnixNano()-mt1)
						fmt.Printf("finish migrate\n")
						if n.getOpts().DaemonPriority == "HIGH" {
							for _, p := range t.allPubClients {
								p.CongestChan <- topic
								p.profile = false
							}
						}
					}
				}
			}()
		}

		select {
		case <-ticker:
			// send a heartbeat and read a response (read detects closed conns)
			for _, lookupPeer := range lookupPeers[:1] {
				n.logf("LOOKUPD(%s): sending heartbeat", lookupPeer)
				cmd := nsq.Ping()
				//RTM
				//_, err := lookupPeer.Command(cmd)
				err := lookupPeer.CommandNoResp(cmd)
				if err != nil {
					n.logf("LOOKUPD(%s): ERROR %s - %s", lookupPeer, cmd, err)
				}
			}
		case <-tickerCPU_P: //RTM
			syscall.Getrusage(syscall.RUSAGE_SELF, load)
			cpu = int(load.Utime.Nano() + load.Stime.Nano())
			this_time = time.Now().UnixNano()
			//1.0 represents the daemon process has one dedicated CPU cores
			//params := [][]byte{[]byte(strconv.FormatFloat((float64(cpu-last_cpu)/float64(this_time-last_time))/16.0, 'f', 5, 64))}
			//fmt.Printf("entire CPU %f\n", (float64(cpu-last_cpu)/float64(this_time-last_time))/16.0)
			/* Moving window, max value
			if CPUStats[CPUIndex] < CPUMax {
				CPUStats[CPUIndex] = (float64(cpu-last_cpu) / float64(this_time-last_time)) / 16.0
				if CPUStats[CPUIndex] > CPUMax {
					CPUMax = CPUStats[CPUIndex]
				}
			} else {
				CPUStats[CPUIndex] = (float64(cpu-last_cpu) / float64(this_time-last_time)) / 16.0
				if CPUStats[CPUIndex] > CPUMax {
					CPUMax = CPUStats[CPUIndex]
				} else {
					CPUMax = float64(0)
					for i := 0; i < 30; i++ {
						if CPUStats[i] > CPUMax {
							CPUMax = CPUStats[i]
						}
					}
				}
			}
			CPUIndex = (CPUIndex + 1) % 30
			params := [][]byte{[]byte(strconv.FormatFloat(CPUMax, 'f', 5, 64))}*/

			//Moving average
			CPUCur = (float64(cpu-last_cpu) / float64(this_time-last_time)) / 2.0
			//fmt.Printf("%v, %v\n", cpu-last_cpu, this_time-last_time)

			//if CPUCur > CPUMax {
			//CPUMax = CPUCur
			//}
			//CPUCount++
			//if CPUCount == 100 {
			//CPUAvg = last_CPUAvg + (CPUMax-CPUStats[CPUIndex])/float64(winSize)
			//CPUVar = last_CPUVar + (CPUMax-CPUAvg+CPUStats[CPUIndex]-last_CPUAvg)*(CPUMax-CPUStats[CPUIndex])/(float64(winSize-1))
			CPUStats[CPUIndex] = CPUCur

			//last_CPUAvg = CPUAvg
			//last_CPUVar = CPUVar
			//CPUCount = 0
			//CPUMax = float64(0)
			/*n.RLock()
			realTopics := make([]*Topic, 0, len(n.topicMap))
			for _, t := range n.topicMap {
				realTopics = append(realTopics, t)
			}
			n.RUnlock()
			msgStats[CPUIndex] = 0
			for _, t := range realTopics {
				t.RLock()
				msgStats[CPUIndex] += t.messageCount - t.lastMsgCount2
				t.lastMsgCount2 = t.messageCount
				t.RUnlock()
			}*/
			//}
			CPUIndex = (CPUIndex + 1) % winSize
			//Exponential filter
			//CPUMax = 0.75*CPUMax + (1.0-0.75)*((float64(cpu-last_cpu)/float64(this_time-last_time))/16.0)
			//params := [][]byte{[]byte(strconv.FormatFloat(CPUMax, 'f', 5, 64))}
			//fmt.Println(CPUMax)

			last_cpu = cpu
			last_time = this_time

		case <-tickerCPU_H: //RTM
			idle1, total1 = getCPUSample()
			idleTicks = float64(idle1 - idle0)
			totalTicks = float64(total1 - total0)
			if totalTicks <= float64(0) {
				goto NextTick
			}
			//cpuUsage = (totalTicks - idleTicks) / totalTicks
			//Moving average
			//fmt.Printf("%v %v\n", idleTicks, totalTicks)
			CPUCur = (totalTicks - idleTicks) / totalTicks
			CPUStats[CPUIndex] = CPUCur
			CPUIndex = (CPUIndex + 1) % winSize
			/*if CPUCur > CPUMax {
				CPUMax = CPUCur
			}
			CPUCount++
			if CPUCount == 100 {
				//CPUAvg = last_CPUAvg + (CPUMax-CPUStats[CPUIndex])/float64(winSize)
				//CPUVar = last_CPUVar + (CPUMax-CPUAvg+CPUStats[CPUIndex]-last_CPUAvg)*(CPUMax-CPUStats[CPUIndex])/(float64(winSize-1))
				CPUStats[CPUIndex] = CPUMax
				CPUIndex = (CPUIndex + 1) % winSize
				//last_CPUAvg = CPUAvg
				//last_CPUVar = CPUVar
				CPUCount = 0
				CPUMax = float64(0)

			}

			//Exponential filter
			//CPUMax = 0.75*CPUMax + (1.0-0.75)*((float64(cpu-last_cpu)/float64(this_time-last_time))/16.0)
			//params := [][]byte{[]byte(strconv.FormatFloat(CPUMax, 'f', 5, 64))}
			//fmt.Println(CPUMax)*/
NextTick:

			idle0 = idle1
			total0 = total1

		case <-tickerCPU: //RTM
			for _, lookupPeer := range lookupPeers[:1] {
				CPUAvg = float64(0)
				CPUVar = float64(0)
				for _, c := range CPUStats {
					CPUAvg += c
				}
				CPUAvg = CPUAvg / float64(winSize)
				for _, c := range CPUStats {
					CPUVar += math.Pow((CPUAvg - c), 2)
				}
				CPUVar = CPUVar / float64(winSize)
				params := [][]byte{[]byte(strconv.FormatFloat(CPUAvg, 'f', 5, 64))}
				params = append(params, []byte(strconv.FormatFloat(CPUVar, 'f', 5, 64)))
				cmd := nsq.ReportCPU(params)
				err := lookupPeer.CommandNoResp(cmd)
				if err != nil {
					n.logf("LOOKUPD(%s): ERROR %s - %s", lookupPeer, cmd, err)
				}

			}
		case t := <-n.topicChan:
			//CPUSkew = stat.Skew(CPUStats, nil)
                        //CPUKurt = stat.ExKurtosis(CPUStats, nil)
			//fmt.Printf("~~~~~~~~~~~~!!!!!!! CPUAvg=%v, CPUVar=%v, skew=%v, Kurt=%v\n", CPUAvg, CPUVar, CPUSkew, CPUKurt)
			for _, lookupPeer := range lookupPeers[:1] {
				if strings.HasPrefix(t.name, "tb") {
                                for _, c := range t.allPubClients {
                                        params := [][]byte{}
                                        params = append(params, []byte(t.name))
                                        params = append(params, []byte(c.pubID))
                                        params = append(params, []byte(strconv.FormatInt(int64(c.arrNums), 10)))
                                        for i:=0; i<int(c.arrNums); i++ {
                                                //params = append(params, []byte(strconv.FormatInt(c.arrTimes[i], 10)))
                                                timeInfo,_ := c.arrTimes[i].MarshalJSON()
						params = append(params, timeInfo)
                                        }
                                        cmd := nsq.ReportTP(params)
                                        err := lookupPeer.CommandNoResp(cmd)
                                        if err != nil {
                                                n.logf("LOOKUPD(%s): ERROR %s - %s", lookupPeer, cmd, err)
                                        }
					//time.Sleep(2*time.Millisecond)
                                }
				} else {
						keys := make([]int, len(t.pubClients))
                                                kIndex := 0
                                                for k, _ := range t.pubClients {
                                                        keys[kIndex]=k
                                                        kIndex++
                                                }
                                                sort.Slice(keys, func(i, j int) bool {return keys[i] < keys[j]})
                                                delays1:=[]int64{}
                                                begin:=(t.pubClients[keys[0]].sNum/6)
                                                end:=(t.pubClients[keys[0]].sNum/6)*5
                                                for _, k:= range keys {
                                                        c := t.pubClients[k]
                                                        if c.s1Index < end {
                                                                fmt.Printf("!!!!!!! not enough samples %v\n", c.s1Index)
                                                        }
                                                        for i:=begin; i< end; i++ {
                                                                delays1 = append(delays1, c.s1Times[i])
                                                        }
                                                }
                                                delays2:=[]int64{}
                                                for _, k:= range keys {
                                                        c := t.pubClients[k]
                                                        for i:=begin; i< end; i++ {
                                                                delays2 = append(delays2, c.s2Times[i])
                                                        }
                                                }

                                                delays3 :=[]int64{}
                                                for _, k:= range keys {
                                                        c := t.pubClients[k]
                                                        for i:=begin; i< end; i++ {
                                                                delays3 = append(delays3, c.s3Times[i])
                                                        }
                                                }
						s := make([]int64, len(delays1))
                                                for i:=0; i<len(s); i++ {
                                                        s[i]= delays1[i]/2+ delays2[i]+delays3[i]/2
                                                }
						sort.Slice(s, func(i,j int) bool {return s[i]<s[j]})
                                                log.Printf("total=%v %v\n", len(s), n.startTime)
                                                log.Printf("50=%v, 95=%v, 99=%v\n", s[int(float64(len(s))*0.5)], s[int(float64(len(s))*0.95)], s[int(float64(len(s))*0.99)])

						i:=0
						for {
							params := [][]byte{}
							params = append(params, []byte("latency"))
							params = append(params, []byte(t.name))
							for j:= 0; j<1000 && i<len(s); j++ {
								params=append(params, []byte(strconv.FormatInt(s[i], 10)))
								i++
							}
							cmd := nsq.ReportTP(params)
                                        		lookupPeer.CommandNoResp(cmd)
							if i == len(s) {
								break
							}
							time.Sleep(30*time.Millisecond)
						}
				}
                        }
		case <-rateTicker:
			//fmt.Println(time.Now().UnixNano())
			n.RLock()
			realTopics := make([]*Topic, 0, len(n.topicMap))
			for _, t := range n.topicMap {
				realTopics = append(realTopics, t)
			}
			n.RUnlock()
			for _, t := range realTopics {
				//SRTM: rebalancing of previous version
				t.RLock()
				t.rateWind[rateIndex] = int(t.messageCount - t.lastMsgCount)
				t.lastMsgCount = t.messageCount
				t.RUnlock()
				
				//SRTM: backlog-based rebalancing
				lastIndex := rateIndex-1
				if rateIndex == 0 {
					lastIndex = rateWindSize-1
				}
				t.qWind[rateIndex]=t.qWind[lastIndex]+t.rateWind[rateIndex]-int(t.bucket.Limit()*0.01)
				if t.qWind[rateIndex] < 0 {
					t.qWind[rateIndex]=0
				}
				rateIndex = (rateIndex + 1) % rateWindSize 
			}
		case <-reportChan:
			for index, lookupPeer := range lookupPeers[:1] {
				//n.logf("LOOKUPD(%s): sending heartbeat", lookupPeer)
				n.RLock()
				realTopics := make([]*Topic, 0, len(n.topicMap))
				for _, t := range n.topicMap {
					realTopics = append(realTopics, t)
				}
				n.RUnlock()
				params := [][]byte{}
				params = append(params, []byte("1"))
				totalR := 0
				rateUpload3 := 0
				for _, t := range realTopics {
					params = append(params, []byte(t.name))
					t.RLock()
					rateUpload3 = int(t.messageCount - t.lastMsgCount3)
					totalR += rateUpload3
					if rateUpload3 == 0 || t.congest {
						if t.lastRateUpload != 0 {
							rateUpload3 = t.lastRateUpload
						} else {
							rateUpload3 = int(t.bucket.Limit())
						}
						//rateUpload3 = int(t.bucketRate)
					}
					t.lastMsgCount3 = t.messageCount
					t.lastRateUpload = rateUpload3
					t.RUnlock()
					/*
					rateUpload := 0
					rateMax := 0
					qMax := 0
					for _, r := range t.rateWind {
						rateUpload += r
						if r > rateMax {
							rateMax = r
						}
					}
					rateUpload = rateUpload/10
					for _, r := range t.qWind {
						if r > qMax {
							qMax = r
						}
					}
					//fmt.Println(t.rateWind)
					if rateUpload == 0 || t.congest || qMax == 0 {
						if t.lastRateUpload != 0 {
							rateUpload = t.lastRateUpload
							rateMax = t.lastRateMax
							qMax = t.lastQMax
						} else {
							rateUpload = int(t.bucket.Limit())
							rateMax = t.bucket.Burst()
							qMax = t.bucket.Burst()
						}
					}
					t.lastRateUpload = rateUpload
					t.lastRateMax = rateMax
					t.lastQMax = qMax*/
					/*t.clientLock.RLock()
					pubLen := len(t.clients)
					t.clientLock.RUnlock()
					fmt.Printf("%v.pub = %v %v\n", t.name, pubLen, rateUpload)
					*/
					//fmt.Printf("~~~~~%v %v %v %v %v\n", t.name, rateUpload, qMax, rateMax, rateUpload3)
					//If traffic is very bursty and variable, use rateUpload3 instead of rateUpload&rateMax
					params = append(params, []byte(strconv.Itoa(rateUpload3)))
					params = append(params, []byte(strconv.Itoa(rateUpload3)))
				}
				params[0]=[]byte(strconv.Itoa(totalR))
				cmd := nsq.ReportRate(params)
				err := lookupPeer.CommandNoResp(cmd)
				if err != nil {
					n.logf("LOOKUPD(%s): ERROR %s - %s", lookupPeer, cmd, err)
				}

				line, _ := readers[index].ReadString('\n')
				line = strings.TrimSpace(line)
				backs := strings.Split(line, " ")

				if backs[0] == "BACK" {
					i := 1
					for i < len(backs) {
						topic := string(backs[i])
						i++
						rateBack, _ := strconv.ParseFloat(backs[i], 64)
						//_, _ = strconv.ParseFloat(backs[i],64)
						i++
						burstBack, _ := strconv.ParseFloat(backs[i], 64)
						//_, _ = strconv.ParseFloat(backs[i],64)
						i++
						//num, _ := strconv.Atoi(backs[i])
						_, _ = strconv.Atoi(backs[i])
						i++

						if n.topicMap[topic] == nil {
							fmt.Printf("invalid %s\n", line)
						}
						n.topicMap[topic].Lock()
						if strings.HasPrefix(n.topicMap[topic].name, "p") && n.topicMap[topic].inDC == true {
							rateBack=(float64(n.topicMap[topic].lastRateUpload)*1.1)/float64(n.topicMap[topic].bucketRate)
							if rateBack > float64(1.0) {
								rateBack = float64(1.0)
							}
							burstBack = rateBack	
						}
						n.topicMap[topic].bucket.SetLimit(rate.Limit(math.Max(rateBack * float64(n.topicMap[topic].bucketRate), float64(n.topicMap[topic].quantum))))
						
						//n.topicMap[topic].bucket.SetLimit(rate.Limit(n.topicMap[topic].bucketRate/float64(num)))
						//RTM.dtb
						n.topicMap[topic].bucket.SetBurst(int(math.Max((burstBack * float64(n.topicMap[topic].bucketSize)), float64(n.topicMap[topic].quantum))))
						//n.topicMap[topic].bucket.SetBurst(int(math.Max(float64(n.topicMap[topic].bucketSize/num), 1.0)))
						n.topicMap[topic].Unlock()
						//fmt.Printf("%v %f %d\n", topic, float64(n.topicMap[topic].bucket.Limit()), n.topicMap[topic].bucket.Burst())
					}
				}
				go func() {
					time.Sleep(1 * time.Second)
					reportChan <- 1
				}()
			}
		case val := <-n.notifyChan:
			var cmd *nsq.Command
			var branch string
			var topic *Topic
			var channel *Channel
			registerFlag := false

			switch val.(type) {
			case *Channel:
				// notify all nsqlookupds that a new channel exists, or that it's removed
				branch = "channel"
				channel = val.(*Channel)
				if channel.Exiting() == true {
					cmd = nsq.UnRegister(channel.topicName, channel.name)
				} else {
					cmd = nsq.Register(channel.topicName, channel.name)
					registerFlag = true
				}
			case *Topic:
				// notify all nsqlookupds that a new topic exists, or that it's removed
				branch = "topic"
				topic = val.(*Topic)
				if topic.Exiting() == true {
					cmd = nsq.UnRegister(topic.name, "")
				} else {
					cmd = nsq.Register(topic.name, "")
					registerFlag = true
				}
			case *clientV2:
				// RTM
				// notify all nsqlookupds that a new client exists, or that it's removed
				branch = "client"
				client := val.(*clientV2)
				if client.exitFlag == 1 {
					cmd = nsq.UnRegister("", "")
				} else {
					cmd = nsq.Register("", "")
				}
			}

			for _, lookupPeer := range lookupPeers[:1] {
				n.logf("LOOKUPD(%s): %s %s", lookupPeer, branch, cmd)
				resp, err := lookupPeer.Command(cmd)
				if err != nil {
					n.logf("LOOKUPD(%s): ERROR %s - %s", lookupPeer, cmd, err)
				}
				// get allocated multicast Address
				if branch == "topic" && registerFlag && !bytes.Equal(resp, []byte("OK")) {
					s := string(resp[:len(resp)])
					n.logf("LOOKUPD(%s): Return Multicast Address of Topic %s, Address %s", lookupPeer, topic.name, s)
					topic.Lock()
					addr, _ := net.ResolveUDPAddr("udp", s)
					topic.MTCAddr = addr
					topic.Unlock()
					topic.GetDefaultChannel()
				}
			}
		case lookupPeer := <-syncTopicChan:
			var commands []*nsq.Command
			// build all the commands first so we exit the lock(s) as fast as possible
			n.RLock()
			for _, topic := range n.topicMap {
				topic.RLock()
				if len(topic.channelMap) == 0 {
					commands = append(commands, nsq.Register(topic.name, ""))
				} else {
					for _, channel := range topic.channelMap {
						commands = append(commands, nsq.Register(channel.topicName, channel.name))
					}
				}
				topic.RUnlock()
			}
			n.RUnlock()

			for _, cmd := range commands {
				n.logf("LOOKUPD(%s): %s", lookupPeer, cmd)
				_, err := lookupPeer.Command(cmd)
				if err != nil {
					n.logf("LOOKUPD(%s): ERROR %s - %s", lookupPeer, cmd, err)
					break
				}
			}
		case <-n.optsNotificationChan:
			var tmpPeers []*lookupPeer
			var tmpAddrs []string
			for _, lp := range lookupPeers[:1] {
				if in(lp.addr, n.getOpts().NSQLookupdTCPAddresses) {
					tmpPeers = append(tmpPeers, lp)
					tmpAddrs = append(tmpAddrs, lp.addr)
					continue
				}
				n.logf("LOOKUP(%s): removing peer", lp)
				lp.Close()
			}
			lookupPeers = tmpPeers
			lookupAddrs = tmpAddrs
			connect = true
		case <-n.exitChan:
			goto exit
		}
	}

exit:
	n.logf("LOOKUP: closing")
}

func in(s string, lst []string) bool {
	for _, v := range lst {
		if s == v {
			return true
		}
	}
	return false
}

func (n *NSQD) lookupdHTTPAddrs() []string {
	var lookupHTTPAddrs []string
	lookupPeers := n.lookupPeers.Load()
	if lookupPeers == nil {
		return nil
	}
	for _, lp := range lookupPeers.([]*lookupPeer) {
		if len(lp.Info.BroadcastAddress) <= 0 {
			continue
		}
		addr := net.JoinHostPort(lp.Info.BroadcastAddress, strconv.Itoa(lp.Info.HTTPPort))
		lookupHTTPAddrs = append(lookupHTTPAddrs, addr)
	}
	return lookupHTTPAddrs
}
