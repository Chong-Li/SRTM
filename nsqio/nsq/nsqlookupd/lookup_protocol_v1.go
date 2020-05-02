package nsqlookupd

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math"
	"net"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
	//"container/list"

	"github.com/WU-CPSL/RTM-0.1/nsqio/go-nsq"
	"github.com/WU-CPSL/RTM-0.1/nsqio/nsq/internal/protocol"
	"github.com/WU-CPSL/RTM-0.1/nsqio/nsq/internal/version"
	"github.com/gonum/stat"
	"github.com/WU-CPSL/RTM-0.1/nsqio/rate"
	//"github.com/emirpasic/gods/trees/redblacktree"
	//"github.com/emirpasic/gods/utils"
)

type LookupProtocolV1 struct {
	ctx *Context
}

func (p *LookupProtocolV1) IOLoop(conn net.Conn) error {
	var err error
	var line string

	client := NewClientV1(conn)
	reader := bufio.NewReader(client)
	for {
		line, err = reader.ReadString('\n')
		if err != nil {
			break
		}
		line = strings.TrimSpace(line)
		params := strings.Split(line, " ")

		var response []byte
		response, err = p.Exec(client, reader, params)
		if err != nil {
			ctx := ""
			if parentErr := err.(protocol.ChildErr).Parent(); parentErr != nil {
				ctx = " - " + parentErr.Error()
			}
			p.ctx.nsqlookupd.logf("ERROR: [%s] - %s%s", client, err, ctx)

			_, sendErr := protocol.SendResponse(client, []byte(err.Error()))
			if sendErr != nil {
				p.ctx.nsqlookupd.logf("ERROR: [%s] - %s%s", client, sendErr, ctx)
				break
			}

			// errors of type FatalClientErr should forceably close the connection
			if _, ok := err.(*protocol.FatalClientErr); ok {
				break
			}
			continue
		}

		if response != nil {
			_, err = protocol.SendResponse(client, response)
			if err != nil {
				break
			}
		}
	}

	p.ctx.nsqlookupd.logf("CLIENT(%s): closing", client)
	if client.peerInfo != nil {
		registrations := p.ctx.nsqlookupd.DB.LookupRegistrations(client.peerInfo.id)
		for _, r := range registrations {
			if removed, _ := p.ctx.nsqlookupd.DB.RemoveProducer(r, client.peerInfo.id); removed {
				p.ctx.nsqlookupd.logf("DB: client(%s) UNREGISTER category:%s key:%s subkey:%s",
					client, r.Category, r.Key, r.SubKey)
			}
		}
	}
	return err
}

func (p *LookupProtocolV1) Exec(client *ClientV1, reader *bufio.Reader, params []string) ([]byte, error) {
	switch params[0] {
	case "PING":
		return p.PING(client, params)
	case "IDENTIFY":
		return p.IDENTIFY(client, reader, params[1:])
	case "REGISTER":
		return p.REGISTER(client, reader, params[1:])
	case "UNREGISTER":
		return p.UNREGISTER(client, reader, params[1:])
	case "RATE": //RTM
		return p.doRATE(client, reader, params[1:])
	case "CPU": //RTM
		return p.doCPU(client, reader, params[1:])
	case "TIME": //RTM
		//p.doDist(client, reader, params[1:])
		p.doProfile(client, reader, params[1:])
		return nil, nil
	case "TP": //RTM
		//p.doTP(client, reader, params[1:])
		if params[1] == "latency" {
			go p.doLatency(client, reader, params[2:])
		} else if params[1] == "profile" {
			go p.doLatencyPro(client, reader, params[2:])
		} else {
			go p.doTB(client, reader, params[1:])
		}
		return nil, nil
	}
	return nil, protocol.NewFatalClientErr(nil, "E_INVALID", fmt.Sprintf("invalid command %s", params[0]))
}

func getTopicChan(command string, params []string) (string, string, error) {
	if len(params) == 0 {
		//RTM
		//return "", "", protocol.NewFatalClientErr(nil, "E_INVALID", fmt.Sprintf("%s insufficient number of params", command))
		return "", "", nil
	}

	topicName := params[0]
	var channelName string
	if len(params) >= 2 {
		channelName = params[1]
	}

	if !protocol.IsValidTopicName(topicName) {
		return "", "", protocol.NewFatalClientErr(nil, "E_BAD_TOPIC", fmt.Sprintf("%s topic name '%s' is not valid", command, topicName))
	}

	if channelName != "" && !protocol.IsValidChannelName(channelName) {
		return "", "", protocol.NewFatalClientErr(nil, "E_BAD_CHANNEL", fmt.Sprintf("%s channel name '%s' is not valid", command, channelName))
	}

	return topicName, channelName, nil
}

//RTM
func (p *LookupProtocolV1) doRATE(client *ClientV1, reader *bufio.Reader, params []string) ([]byte, error) {
	if client.peerInfo == nil {
		return nil, protocol.NewFatalClientErr(nil, "E_INVALID", "client must IDENTIFY")
	}
	i := 0
	//ReportBack := make(map[*ClientV1]bool)
	p.ctx.nsqlookupd.Lock()
	p.ctx.nsqlookupd.ReportBack[client] = true
	p.ctx.nsqlookupd.Unlock()
	client.peerInfo.totalRate, _ = strconv.Atoi(params[i])
	i++
	/*pps := p.ctx.nsqlookupd.DB.FindProducers("client", "", "")
	for _, pp:= range pps {
		if pp.peerInfo.id == client.peerInfo.id {
			pp.peerInfo.totalRate = client.peerInfo.totalRate
		}
	}*/
	for i < len(params) {
		topic := string(params[i])
		i++
		rate, _ := strconv.Atoi(params[i])
		i++
		burst, _ := strconv.Atoi(params[i])
		i++
		//key := Registration{"topic", topic, ""}
		producers := p.ctx.nsqlookupd.DB.FindProducers("topic", topic, "")
		alldone := true
		for _, producer := range producers {
			if producer.peerInfo.id == client.peerInfo.id {
				producer.peerInfo.rates[topic].rate = rate
				producer.peerInfo.rates[topic].burst = burst
				producer.peerInfo.rates[topic].updated = true
			} else if !producer.peerInfo.rates[topic].updated {
				alldone = false
			}
		}
		if alldone {
			sum_r := 0
			sum_b := 0
			for _, producer := range producers {
				sum_r += producer.peerInfo.rates[topic].rate
				sum_b += producer.peerInfo.rates[topic].burst
			}
			if sum_r > 0 {
				for _, producer := range producers {
					if producer.peerInfo.rates[topic].rate == 0 {
						continue
					}
					producer.peerInfo.Lock()
					//RTM.dtb
					/*if topic == p.ctx.nsqlookupd.DB.TPName {
						producer.peerInfo.rates[topic].newrate =
                                                float64(producer.peerInfo.rates[topic].rate) / float64(p.ctx.nsqlookupd.DB.TPRate)
                                                producer.peerInfo.rates[topic].newburst =
                                                float64(producer.peerInfo.rates[topic].rate) / float64(p.ctx.nsqlookupd.DB.TPRate)
					} else {*/
						producer.peerInfo.rates[topic].newrate =
						float64(producer.peerInfo.rates[topic].rate) / float64(sum_r) //RTM.dtb
						producer.peerInfo.rates[topic].newburst =
						float64(producer.peerInfo.rates[topic].burst) / float64(sum_b) //RTM.dtb
					//}
					producer.peerInfo.rates[topic].sendback = true
					producer.peerInfo.rates[topic].updated = false
					//ReportBack[producer.peerInfo] = true
					producer.peerInfo.BackMsg = append(producer.peerInfo.BackMsg, []byte(topic))
					producer.peerInfo.BackMsg = append(producer.peerInfo.BackMsg,
						[]byte(strconv.FormatFloat(producer.peerInfo.rates[topic].newrate, 'f', 5, 64)))
					//if _,ok:=producer.peerInfo.bursts[topic];ok {
					/*if p.ctx.nsqlookupd.DB.topicCount[topic] == p.ctx.nsqlookupd.DB.TCtotal {
						producer.peerInfo.BackMsg = append(producer.peerInfo.BackMsg,
							[]byte(strconv.FormatFloat(producer.peerInfo.bursts[topic], 'f', 5, 64)))
					} else {*/
						producer.peerInfo.BackMsg = append(producer.peerInfo.BackMsg,
							[]byte(strconv.FormatFloat(producer.peerInfo.rates[topic].newburst, 'f', 5, 64)))
					//}
					producer.peerInfo.BackMsg = append(producer.peerInfo.BackMsg, []byte(strconv.Itoa(len(producers))))
					producer.peerInfo.Unlock()
				}
			}
		}
	}
	p.ctx.nsqlookupd.Lock()
	if len(p.ctx.nsqlookupd.ReportBack) == len(p.ctx.nsqlookupd.DB.FindProducers("client", "", "")) {
		//p.ctx.nsqlookupd.RLock()
		for c, _ := range p.ctx.nsqlookupd.ReportBack {
			//_, err = protocol.SendResponse(producer.peerInfo.client, producer.peerInfo.BackMsg)
			if len(c.peerInfo.BackMsg) > 2 {
				//fmt.Printf("Back to %s %s %s %s\n", c.peerInfo.RemoteAddress, string(c.peerInfo.BackMsg[0]), string(c.peerInfo.BackMsg[1]), string(c.peerInfo.BackMsg[2]))
			}
			c.peerInfo.RLock()
			cmd := nsq.Back(c.peerInfo.BackMsg)
			c.peerInfo.RUnlock()
			cmd.WriteTo(c)
			c.peerInfo.Lock()
			c.peerInfo.BackMsg = [][]byte{}
			c.peerInfo.Unlock()
		}
		//p.ctx.nsqlookupd.RUnlock()
		//p.ctx.nsqlookupd.Lock()
		p.ctx.nsqlookupd.ReportBack = make(map[*ClientV1]bool)
		//p.ctx.nsqlookupd.Unlock()
	}
	p.ctx.nsqlookupd.Unlock()
	return nil, nil
}
func sumSlice(a, b []bEntry) []bEntry {
	i := 0
	j := 0
	c := make([]bEntry, 2000)
	index := 0
	for {
		if a[i].index > b[j].index {
			for b[j].index < a[i].index {
				c[index] = bEntry{b[j].index, b[j].burst}
				index++
				if index == cap(c) {
					newSlice := make([]bEntry, cap(c)*2)
					copy(newSlice, c)
					c = newSlice
				}
				j++
				if b[j].burst == 0 {
					for a[i].burst != 0 {
						c[index] = bEntry{a[i].index, a[i].burst}
						index++
						if index == cap(c) {
							newSlice := make([]bEntry, cap(c)*2)
							copy(newSlice, c)
							c = newSlice
						}
						i++
					}
					goto out
				}
			}
		} else if a[i].index < b[j].index {
			for a[i].index < b[j].index {
				c[index] = bEntry{a[i].index, a[i].burst}
				index++
				if index == cap(c) {
					newSlice := make([]bEntry, cap(c)*2)
					copy(newSlice, c)
					c = newSlice
				}
				i++
				if a[i].burst == 0 {
					for b[j].burst != 0 {
						c[index] = bEntry{b[j].index, b[j].burst}
						index++
						if index == cap(c) {
							newSlice := make([]bEntry, cap(c)*2)
							copy(newSlice, c)
							c = newSlice
						}
						j++
					}
					goto out
				}
			}
		} else if a[i].index == b[j].index {
			c[index] = bEntry{a[i].index, a[i].burst + b[j].burst}
			index++
			if index == cap(c) {
				newSlice := make([]bEntry, cap(c)*2)
				copy(newSlice, c)
				c = newSlice
			}
			i++
			if a[i].burst == 0 {
				for b[j].burst != 0 {
					c[index] = bEntry{b[j].index, b[j].burst}
					index++
					if index == cap(c) {
						newSlice := make([]bEntry, cap(c)*2)
						copy(newSlice, c)
						c = newSlice
					}
					j++
				}
				goto out
			}
			j++
			if b[j].burst == 0 {
				for a[i].burst != 0 {
					c[index] = bEntry{a[i].index, a[i].burst}
					index++
					if index == cap(c) {
						newSlice := make([]bEntry, cap(c)*2)
						copy(newSlice, c)
						c = newSlice
					}
					i++
				}
				goto out
			}
		}
	}
out:
	return c
}

func ccSlice(a, b []bEntry) int {
	i := 0
	j := 0
	cc := 0
	for {
		if a[i].index > b[j].index {
			for b[j].index < a[i].index {
				j++
				if b[j].burst == 0 {
					goto out
				}
			}
		} else if a[i].index < b[j].index {
			for a[i].index < b[j].index {
				i++
				if a[i].burst == 0 {
					goto out
				}
			}
		} else if a[i].index == b[j].index {
			cc += (a[i].burst * b[j].burst)
			i++
			if a[i].burst == 0 {
				goto out
			}
			j++
			if b[j].burst == 0 {
				goto out
			}
		}
	}
out:
	return cc
}

type Cand []*Producer

func (s Cand) Len() int {
	return len(s)
}
func (s Cand) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}
func (s Cand) Less(i, j int) bool {
	return s[i].peerInfo.rCap > s[j].peerInfo.rCap
	//return s[i].peerInfo.rCap < s[j].peerInfo.rCap
}

type TT []*Timeline

func (a TT) Len() int           { return len(a) }
func (a TT) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a TT) Less(i, j int) bool { return a[i].rate > a[j].rate }

var K2 int

func (p *LookupProtocolV1) rcap(Var float64, Mean float64) float64 {
	overT := 0.95
	prob := 0.15
	results, _ := ioutil.ReadFile("/home/RTM/run-test/prob")
	for _, value := range bytes.Split(results[0:], []byte{'\n'}) {
		//prob, _ = strconv.ParseFloat(string(value), 64)
		K2, _= strconv.Atoi(string(value))
		break
	}

	return overT - math.Sqrt((1-prob)*Var/prob) - Mean
}
func (p *LookupProtocolV1) doProfile2(client *ClientV1, reader *bufio.Reader, params []string) {
	tbDelay := <-p.ctx.nsqlookupd.DB.TBChan
	tail,_ := strconv.ParseInt(params[0], 10, 64)
	for i:=1; i< len(params); i++ {
		other_tail, _:= strconv.ParseInt(params[i], 10, 64)
		if other_tail > 1000000 {
			p.ctx.nsqlookupd.DB.profileChan <- 2
			return
		}
	}
	tail = tail - tbDelay
	if tail > 1000000 {
		p.ctx.nsqlookupd.DB.profileChan <- 2
	} else if tail < 800000 {
		p.ctx.nsqlookupd.DB.profileChan <- 0
	} else {
		p.ctx.nsqlookupd.DB.profileChan <- 1
	}
}

func (p *LookupProtocolV1) doProfile(client *ClientV1, reader *bufio.Reader, params []string) {
        fmt.Printf("Recv profile from %v\n", client.peerInfo.RemoteAddress)
	index, _:= strconv.Atoi(params[0])
	broker := p.ctx.nsqlookupd.DB.brokerList[p.ctx.nsqlookupd.DB.TPName][index]
	tail,_ := strconv.ParseInt(params[1], 10, 64)
        for i:=2; i< len(params); i++ {
                other_tail, _:= strconv.ParseInt(params[i], 10, 64)
                if other_tail > 1000000 {
                        broker.peerInfo.profileChan <- 2
                        return
                }
        }
        if tail > 1000000 {
                broker.peerInfo.profileChan <- 2
        } else if tail < 800000 {
                broker.peerInfo.profileChan <- 0
        } else {
                broker.peerInfo.profileChan <- 1
        }
}

//RTM
func (p *LookupProtocolV1) doDist(client *ClientV1, reader *bufio.Reader, params []string) {
	//fmt.Printf("recv timeline %v~~~~~~~\n", client.peerInfo.RemoteAddress)
	i := 0
	topic := string(params[i])
	topic_raw := topic
	if strings.HasPrefix(topic, "p") {
		topic = topic[1:]
	}
	i++
	timeLen := 0
	if strings.HasPrefix(topic, "0") {
		timeLen = 500
	} else {
		timeLen = 5000
	}
	p.ctx.nsqlookupd.DB.Lock()
	if p.ctx.nsqlookupd.DB.times[topic] == nil {
		p.ctx.nsqlookupd.DB.times[topic] = make([][]int64, p.ctx.nsqlookupd.DB.topicCount[topic])
	}
	p.ctx.nsqlookupd.DB.Unlock()
	pubNum := 0
	for i < len(params) {
		pubID, _ := strconv.Atoi(params[i])
		i++
		pubNum++
		//fmt.Printf("%v %v %v\n", pubID, i, len(params))
		p.ctx.nsqlookupd.DB.times[topic][pubID] = make([]int64, timeLen)
		for j := 0; j < timeLen; j++ {
			p.ctx.nsqlookupd.DB.times[topic][pubID][j], _ = strconv.ParseInt(params[i], 10, 64)
			i++
		}
	}
	client.peerInfo.RLock()
	client.peerInfo.newRisk = client.peerInfo.risk
	client.peerInfo.newCPU = client.peerInfo.CPU
	client.peerInfo.newVar = client.peerInfo.CPUVar
	client.peerInfo.RUnlock()
	p.ctx.nsqlookupd.DB.Lock()
	p.ctx.nsqlookupd.DB.topicCount2[topic] += pubNum
	p.ctx.nsqlookupd.DB.Unlock()
	//fmt.Printf("len now = %v\n", p.ctx.nsqlookupd.DB.topicCount2[topic])
	if p.ctx.nsqlookupd.DB.topicCount2[topic] == p.ctx.nsqlookupd.DB.topicCount[topic] {
		fmt.Printf("!!!!!!!!~~~~~~len now = %v\n", p.ctx.nsqlookupd.DB.topicCount2[topic])
		bMatrix := make([]*Timeline, p.ctx.nsqlookupd.DB.topicCount[topic])
		totalRate := float64(0)
		start := p.ctx.nsqlookupd.DB.times[topic][0][0]
		end := p.ctx.nsqlookupd.DB.times[topic][0][timeLen-1]
		diameter := int64(10000000)
		right := start
		left := right - diameter
		numBins := int((end - start) / diameter)
		fmt.Printf("numBins = %v\n", numBins)
		for i = 0; i < p.ctx.nsqlookupd.DB.topicCount[topic]; i++ {
			bMatrix[i] = &Timeline{}
			bMatrix[i].pubID = i
			bMatrix[i].time = make([]bEntry, 2000)
			bMatrix[i].scanIndex = 0
			msgTime := p.ctx.nsqlookupd.DB.times[topic][i]
			index := 0
			right = start
			left = right - diameter
			//fmt.Printf("%v\n", i)
			for j := 0; j < numBins; j++ {
				left = right
				right = left + diameter
				//fmt.Printf("j=%v\n", j)
				for x := bMatrix[i].scanIndex; x < timeLen; x++ {
					//fmt.Printf("%v %v \n",x,  left)
					if msgTime[x] >= left && msgTime[x] < right {
						if bMatrix[i].time[index].burst == 0 {
							bMatrix[i].time[index] = bEntry{j, 1}
						} else {
							bMatrix[i].time[index].burst += 1
						}
					} else if msgTime[x] >= right {
						bMatrix[i].scanIndex = x
						if bMatrix[i].time[index].burst != 0 {
							index++
							if index == cap(bMatrix[i].time) {
								newSlice := make([]bEntry, cap(bMatrix[i].time)*2)
								copy(newSlice, bMatrix[i].time)
								bMatrix[i].time = newSlice
							}
						}
						break
					}
				}
			}
			bMatrix[i].rate = float64(timeLen) / (float64(msgTime[timeLen-1]-msgTime[0]) / float64(1000000000))
			totalRate += bMatrix[i].rate
			//fmt.Printf("%v.rate=%v\n",i, bMatrix[i].rate)
		}

		sort.Sort(TT(bMatrix))
		p.ctx.nsqlookupd.DB.matrix[topic] = bMatrix

		subProducers := p.ctx.nsqlookupd.DB.FindProducers("topic", topic_raw, "")
		totalCPU := float64(0)
		totalVar := float64(0)
		for _, c := range subProducers {
			//totalCPU += (c.peerInfo.newRisk - c.peerInfo.oldRisk)
			totalCPU += math.Max(float64(0), (c.peerInfo.newCPU - c.peerInfo.oldCPU))
			totalVar += (c.peerInfo.newVar - c.peerInfo.oldVar)
		}
		totalVar = math.Max(float64(0), totalVar)
		fmt.Printf("\n totalCPU = %v %v\n", totalCPU, totalVar)
		if totalVar < 0 {
			totalVar = 0
		}
		//totalCPU= 1.34
		p.ctx.nsqlookupd.DB.totalCPU[topic] = totalCPU
		p.ctx.nsqlookupd.DB.totalRate[topic] = totalRate

		producers := p.ctx.nsqlookupd.DB.FindProducers("client", "", "")
		for _, c := range producers {
			c.peerInfo.rCap = p.rcap(c.peerInfo.oldVar+totalVar, c.peerInfo.oldCPU)
			fmt.Printf("CPU=%v Var=%v rCap=%v\n", c.peerInfo.oldCPU, c.peerInfo.oldVar, c.peerInfo.rCap)
			//c.peerInfo.rCap = 0.25
			if c.peerInfo.rCap < 0 {
				c.peerInfo.rCap = 0
			}
		}
		/*NOTE: comment for publisher-assignment test only*/
		//sort.Sort(Cand(producers))
		rCPU := totalCPU
		k := 0
		p.ctx.nsqlookupd.DB.preDist[topic] = make([]*Producer, 0, len(producers))
		/*NOTE: for publisher-assignment test only*/
		for i, c := range producers {
			c.peerInfo.curRisk = 0
			c.peerInfo.curConn = 0
			c.peerInfo.curCPU = c.peerInfo.oldCPU
			c.peerInfo.curVar = c.peerInfo.oldVar
			c.peerInfo.time = make([]bEntry, 2000)
			k = i + 1
			p.ctx.nsqlookupd.DB.preDist[topic] = append(p.ctx.nsqlookupd.DB.preDist[topic], c)
			rCPU -= c.peerInfo.rCap
			/*if rCPU < 0 || rCPU == 0 {
				break
			}*/
			if k == K2 {
				break
			}
		}
		fmt.Printf("k=%v preDist=%v\n", k, p.ctx.nsqlookupd.DB.preDist[topic])
		//rCap is not large enough. Just use the maximum number of brokers
		if rCPU > 0 {
			for i, c := range producers {
                                if i == k {
                                        break
                                }
                                c.peerInfo.rCap = totalCPU*1.1 / float64(k)
                        }
	
		}
		//Same reason in Random Assignment in http.go
		if producers[0].peerInfo.rCap < float64(0.1) {
			newSum := float64(0)
			for i, c := range producers {
				if i == k {
					break
				}
				newSum += c.peerInfo.rCap
			}
			for i, c := range producers {
				if i == k {
					break
				}
				c.peerInfo.rCap = newSum / float64(k)
			}

		}
		fmt.Printf("avg. rcap = %v\n", producers[0].peerInfo.rCap)

		p.ctx.nsqlookupd.DB.topicDist[topic] = make([]*Producer, len(bMatrix))
		brokers := make(map[string]*Producer)
		times := p.ctx.nsqlookupd.DB.times[topic]
		curRate := float64(0)
		r := totalRate*1.1/float64(k)
		for i, pub := range bMatrix {
			pubCPU := totalCPU * (pub.rate / totalRate)
			//pubVar := totalVar * (pub.rate / totalRate)
			curRate += pub.rate
			//minCC := 100000000000000
			minCC := float64(100000000000000)
			minID := -1
			if strings.Contains(topic, "wc") {
				for i = 0; i < k; i++ {
					if producers[i].peerInfo.curRisk+pubCPU <= producers[i].peerInfo.rCap {
						minID = i
						break
					}
				}
			} else {
				for i = 0; i < k; i++ {
					/*if producers[i].peerInfo.curRisk+pubCPU <= producers[i].peerInfo.rCap {
						cc := ccSlice(pub.time, producers[i].peerInfo.time)
						if cc < minCC {
							minCC = cc
							minID = i
						}
					}*/
					tp_times := []int64{}
					tp_times = append(tp_times, producers[i].peerInfo.tp_times...)
					tp_times = append(tp_times, times[pub.pubID]...)
					sort.Slice(tp_times, func(i, j int) bool {return tp_times[i]<tp_times[j]})
					aTimes := make([]float64, len(tp_times)-1)
        				for j:=1; j<len(tp_times); j++ {
                				aTimes[j-1]=float64(tp_times[j]-tp_times[j-1])/float64(1000000000)
       					}
					v:=stat.Variance(aTimes, nil)
					//fmt.Printf("pub %v at broker %v, v=%v\n", pub.pubID, i, v)
					producers[i].peerInfo.curVar = v
					u:=float64(0)
					for j:=0; j< k; j++ {
						if j==i {
							u+=((producers[i].peerInfo.curConn+pub.rate)/curRate)*(producers[i].peerInfo.curConn+pub.rate)*v/(2*(1-(producers[i].peerInfo.curConn+pub.rate)/r))
						} else {
							u+=(producers[j].peerInfo.curConn/curRate)*(producers[j].peerInfo.curConn*producers[j].peerInfo.curRisk)/(2*(1-producers[j].peerInfo.curConn/r))
						}
					}		
					//fmt.Printf("u=%v\n", u)
					if u < minCC {
						minCC = u
						minID = i
					}
				}
			}
			if minID != -1 {
				/*producers[minID].peerInfo.curRisk += pubCPU
				producers[minID].peerInfo.curConn += 1
				producers[minID].peerInfo.curCPU += pubCPU
				producers[minID].peerInfo.curVar += pubVar
				producers[minID].peerInfo.time = sumSlice(pub.time, producers[minID].peerInfo.time)
				p.ctx.nsqlookupd.DB.topicDist[topic][pub.pubID] = producers[minID]
				brokers[producers[minID].peerInfo.id] = producers[minID]*/

				producers[minID].peerInfo.curRisk = producers[minID].peerInfo.curVar
				producers[minID].peerInfo.curConn += pub.rate
				producers[minID].peerInfo.tp_times = append(producers[minID].peerInfo.tp_times, times[pub.pubID]...)
				p.ctx.nsqlookupd.DB.topicDist[topic][pub.pubID] = producers[minID]
				brokers[producers[minID].peerInfo.id] = producers[minID]
			}
		}
		/*for key, c:= range p.ctx.nsqlookupd.DB.topicDist[topic] {
			fmt.Printf("%v %v\n", key, c.peerInfo.RemoteAddress)
		}*/
		p.ctx.nsqlookupd.DB.burstDist[topic] = make([]*Producer, 0, len(brokers))
		sum := float64(0)
		for _, b := range brokers {
			index := 0
			max := 0
			for {
				if b.peerInfo.time[index].burst > 0 {
					if b.peerInfo.time[index].burst > max {
						max = b.peerInfo.time[index].burst
					}
					index++
				} else {
					break
				}
			}
			//b.peerInfo.bursts[topic]=float64(max)
			//sum += float64(max)
			//split bucket size based on # of conns
			b.peerInfo.bursts[topic] = float64(b.peerInfo.curConn)
			sum += float64(b.peerInfo.curConn)
			p.ctx.nsqlookupd.DB.burstDist[topic] = append(p.ctx.nsqlookupd.DB.burstDist[topic], b)
		}
		for _, b := range brokers {
			b.peerInfo.bursts[topic] = b.peerInfo.bursts[topic] / sum
			fmt.Printf("~~~~~~~~~~~ bursts = %v\n", b.peerInfo.bursts[topic])
		}
	}
}
func (p *LookupProtocolV1) doEst2(v float64, rate float64) float64 {
        mu1 := int64(4481)
        t := float64(500000)
        peak := float64(0.000264)
        ro := rate*float64(mu1)
        //B2 := (1.0+rate*rate*v)/2.0
        x := math.Pow((1.0/rate-1.0/peak),2)
        B2:= (x+v)/(2.0*x)
        fmt.Printf("rate=%v, var=%v, B2=%v\n",rate, ro,  B2)
        B := B2
        BI := int(B)+1
        P:= float64(0)
        for i:= 1; i<= BI; i++ {
                if t >= float64(i)*float64(mu1) {
                        //P+= ro*math.Exp(-((1.0/float64(mu1)-rate)/float64(B))*(t-float64(i)*float64(mu1)))    		 
		        P += ro*math.Exp(-(1.0-ro)*(t-float64(i)*float64(mu1))/(B*(1.0-rate/peak)*(float64(mu1)-1.0/peak)))
                } else {
                        P+=1.0
                }
        }
        answer := P/float64(BI)
        fmt.Printf("B=%v, BI=%v, P=%v, answer=%v\n", B, BI, P, answer)
        return answer
}

func (p *LookupProtocolV1) doProb4(c *Producer, times []int64) float64 {
        mu1 := int64(4481)
        t := float64(500000)
	//peak := float64(0.000264)
	rate2 := float64(len(times))/float64(times[len(times)-1]-times[0])
        rate := c.peerInfo.tokenRate + rate2
        ro := rate*float64(mu1)
        aTimes := make([]float64, len(times)-1)
        for i:=0; i< len(aTimes); i++ {
                aTimes[i]=float64(times[i+1]-times[i])
                //fmt.Println(aTimes[i])
        }
        v := stat.Variance(aTimes, nil)
        B2 := (1.0+rate2*rate2*v)/2.0
	//x := math.Pow((1.0/rate2-1.0/peak),2)
	//B2:= (x+v)/(2.0*x)
        fmt.Printf("rate2=%v, var=%v, B2=%v\n",rate, v, B2)
	B := (c.peerInfo.tokenRate*c.peerInfo.curRisk + rate2*B2)/rate
	BI := int(B)+1
        P:= float64(0)
        for i:= 1; i<= BI; i++ {
                if t >= float64(i)*float64(mu1) {
                        P+= ro*math.Exp(-((1.0/float64(mu1)-rate)/float64(B))*(t-float64(i)*float64(mu1)))    
                	//P += ro*math.Exp(-(1.0-ro)*(t-float64(i)*float64(mu1))/(B*(1.0-rate/peak)*(float64(mu1)-1.0/peak)))
		} else {
                        P+=1.0
                }
        }
        answer := P/float64(BI)
        fmt.Printf("B=%v, BI=%v, P=%v, answer=%v\n", B, BI, P, answer)
        if answer > 0.01 {
               
        } else {
                c.peerInfo.tokenRate = rate
		c.peerInfo.curRisk = B
        }
        return answer
}

func (p *LookupProtocolV1) doEstCap(c *Producer, rate2 float64, B2 float64) float64 {
        mu1 := int64(4481)
        //mu1 := int64(11000)
        t := float64(500000)
        rate := c.peerInfo.tokenRate + rate2+c.peerInfo.curConn
        ro := rate*float64(mu1)
        //B2 := (1.0+rate2*rate2*v)/2.0
        //fmt.Printf("rate=%v, B2=%v\n",rate, B2)
        B := (c.peerInfo.tokenRate*c.peerInfo.curRisk + (c.peerInfo.curConn+rate2)*B2)/rate
        BI := int(B)+1
        P:= float64(0)
        for i:= 1; i<= BI; i++ {
                if t >= float64(i)*float64(mu1) {
                        P+= ro*math.Exp(-((1.0/float64(mu1)-rate)/float64(B))*(t-float64(i)*float64(mu1)))
                } else {
                        P+=1.0
                }
        }
        answer := P/float64(BI)
	c.peerInfo.curVar = answer
        //fmt.Printf("B=%v, BI=%v, P=%v, answer=%v\n", B, BI, P, answer)
        if answer > 0.01 {

        } else {
                //c.peerInfo.tokenRate = rate
                //c.peerInfo.curRisk = B
		c.peerInfo.curConn+=rate2
        }
	return answer
}

func (p *LookupProtocolV1) doProb3(c *Producer, times []int64) float64 {
	if len(times) == 0 {
		return float64(0)
	}
	mu1 := int64(4481)
	t := float64(500000)
	tree := c.peerInfo.tree
	rate := c.peerInfo.tokenRate + float64(len(times))/float64(times[len(times)-1]-times[0])
	ro := rate*float64(mu1)
	for _, x := range times {
                tree.Put(x, int64(0))
        }
        it := tree.Iterator()
	num := 0
        p1 := mu1
	it.Next()
	prev := it.Key().(int64)
        p2 := int64(0)
	//var bp []byte
	//len := 0
        for it.Next() {
                  p2 = p1- (it.Key().(int64)-prev)
		  //len++
                  if p2 < 0 {
                           p2 = 0
			   //bp=append(bp,strconv.Itoa(len)...)
			   //bp=append(bp, "\n"...)
			   //len=0
			   num++
                  }
                  p2 += mu1
                  p1=p2
		  prev=it.Key().(int64)
        }
	//ioutil.WriteFile("bp1", bp, 0777)
        B := float64(tree.Size())/float64(num)
	BI := int(B)+1
	P:= float64(0)
	for i:= 1; i<= BI; i++ {
		if t >= float64(i)*float64(mu1) {
			P+= ro*math.Exp(-((1.0/float64(mu1)-rate)/float64(B))*(t-float64(i)*float64(mu1)))	
		} else {
			P+=1.0
		}
	}
	answer := P/float64(BI)
	fmt.Printf("busy period: num=%v, B=%v, BI=%v, P=%v, answer=%v\n", num, B, BI, P, answer)
	/*if answer > 0.01 {
		for _, x:= range times {
			tree.Remove(x)
		}
	} else {
		c.peerInfo.tokenRate = rate
	}*/
	return answer
}

func (p *LookupProtocolV1) doProb2(c *Producer, times []int64) float64 {
        mu := int64(4481)
        threshold := int64(500000)
	prev_over := c.peerInfo.over2
	tree := c.peerInfo.tree
	/*inserts := make([]*redblacktree.Node, len(times))
	if tree.Root == nil {
		tree.Put(times[0], int64(0))
		inserts[i]=tree.Root
		i++
	} else {
		node := tree.Put2(times[0], int64(0), tree.Root)
		inserts[i]=node
		i++
	}
	for ; i< len(times);i++ {
		//node := tree.Put2(times[i], int64(0), inserts[i-1])
		node := tree.Put2(times[i], int64(0), tree.Root)
		inserts[i]=node
	}*/
	for _, x := range times {
		tree.Put(x, int64(0))
	}
	it := tree.Iterator()
	it.Next()
	for i, x := range times {
		node := tree.Lookup2(x)
		/*node := inserts[i]
		if node.Key.(int64)!=times[i] {
			fmt.Println("Wrong")
		}*/
		it.Set_Node(node)
		if it.Prev() {
			node.Value = it.Cur_Node().Value.(int64)-(node.Key.(int64)-it.Cur_Node().Key.(int64))
			if node.Value.(int64) < 0 {
				node.Value = int64(0)
			}
			node.Value = node.Value.(int64)+mu
			if node.Value.(int64) > threshold {
				c.peerInfo.over2++
			}
		} else {
			node.Value = mu
			it.Next()
			//fmt.Println("the begin")
		}
		for {
			it.Set_Node(node)
			found := it.Next()
			if ((i<len(times)-1)&&it.Cur_Node().Key.(int64) == times[i+1]) || !found {
				break
			}
			//fmt.Printf("key=%v, i=%v\n",it.Cur_Node().Key.(int64),i)
			temp := node.Value.(int64)-(it.Cur_Node().Key.(int64)-node.Key.(int64))
                        if temp < 0 {
				break
                        }
                        temp += mu
                        if temp > threshold && it.Cur_Node().Value.(int64) <= threshold {
                                c.peerInfo.over2++
                        }
			it.Cur_Node().Value = temp
			node = it.Cur_Node()
		}
	}
	answer := float64(c.peerInfo.over2)/float64(tree.Size()-1)
	if answer > 0.01 {
		it.Begin()
		it.Next()
		x:=tree.Root
		for i, t := range times {
			node := tree.Lookup2(t)		
                	it.Set_Node(node)
                	if it.Prev() {
                        	prev_key:=it.Cur_Node().Key.(int64)
				prev_value:=it.Cur_Node().Value.(int64)
				it.Set_Node(node)
                        	found := it.Next()
                        	if ((i<len(times)-1)&&it.Cur_Node().Key.(int64) == times[i+1]) || !found {
                                	goto remove2
                        	}
				
                        	temp := prev_value-(it.Cur_Node().Key.(int64)-prev_key)
                        	if temp < 0 {
                                	temp = int64(0)
                        	}
				temp += mu
				if temp == it.Cur_Node().Value.(int64) {
					goto remove2
				}
                        	it.Cur_Node().Value = temp
                        	
                	} else {
                        	it.Next()
				it.Set_Node(node)
                                found := it.Next()
                                if ((i<len(times)-1)&&it.Cur_Node().Key.(int64) == times[i+1]) || !found {
                                        goto remove2
                                }
                                if it.Cur_Node().Value.(int64) == mu {
					goto remove2
				}
				it.Cur_Node().Value = mu
                	}
			x= it.Cur_Node()
                	for {
                        	it.Set_Node(x)
                        	found := it.Next()
                        	if ((i<len(times)-1)&&it.Cur_Node().Key.(int64) == times[i+1]) || !found {
                                	break
                        	}
                        	temp := x.Value.(int64)-(it.Cur_Node().Key.(int64)-x.Key.(int64))
                        	if temp < 0 {
                                	temp = int64(0)
                        	}
				temp += mu
				if temp == it.Cur_Node().Value.(int64) {
					goto remove2
				}
                        	it.Cur_Node().Value = temp
                        	x = it.Cur_Node()
                	}
remove2:
			tree.Remove2(node)
		}
		c.peerInfo.over2 = prev_over
	}
	return answer
}

func (p *LookupProtocolV1) doEst(times []int64) float64 {
        if len(times)< 2 {
                return 0
        }
        /*aTimes := make([]int64, len(times)-1)
        for i:=1; i<len(times); i++ {
                aTimes[i-1]=times[i]-times[i-1]
        }*/
        mu := int64(4481)
        threshold := int64(500000)
        over := 0
        p1 := mu
        p2 := int64(0)
        length := len(times)-1
        for i:=1; i< length; i++ {
                                //p2 = p1 - aTimes[i-1]
                                p2 = p1- (times[i]-times[i-1])
                                if p2 < 0 {
                                        p2 = 0
                                }
                                p2 += mu
                                if p2 > threshold {
                                        over++
                                }
                                p1=p2
        }
        return float64(over)/float64(length)
}

func (p *LookupProtocolV1) merge(a []int64, b []int64) []int64 {

    answer := make([]int64, len(a)+len(b))
    i := 0
    j := 0
    k := 0

    for i<len(a) && j<len(b) {
        if a[i] < b[j] {
                answer[k]=a[i]
                k++
                i++
        } else {
                answer[k]=b[j]
                k++
                j++
        }
    }

    for i < len(a) {
        answer[k] = a[i]
        k++
        i++
    }


    for j < len(b) {
        answer[k] = b[j]
        k++
        j++
    }

    return answer
}

func (p *LookupProtocolV1) CVEst(r1 float64, m1 float64, v1 float64, r2 float64, m2 float64, v2 float64) float64 {
        if r1 == float64(0) {
                return v2/math.Pow(m2,2)
        }
        ro := (r1+r2)*float64(4481)
        v := math.Pow(r1+r2,2)/(math.Pow(r1,2)+math.Pow(r2,2))
        w := math.Pow((1.0+4.0*math.Pow(1.0-ro,2)*(v-1.0)), -1)
        c := w*(r1*v1/math.Pow(m1,2)+r2*v2/math.Pow(m2,2))/(r1+r2) + (1.0-w)
        return c
}

/*func (p *LookupProtocolV1) doFIT(client *ClientV1, reader *bufio.Reader, params []string) {
	i := 0
        topic := string(params[i])
        if strings.HasPrefix(topic, "p") {
                topic = topic[1:]
        }
        i++
        delay99, _ := strconv.ParseInt(params[i], 10, 64)
        
}*/
//RTM
func (p *LookupProtocolV1) doLatency(client *ClientV1, reader *bufio.Reader, params []string) {
        //fmt.Printf("recv timeline %v~~~~~~~\n", len(params[1:]))
        i := 0
        topic := string(params[i])
        i++
	samples := make([]int64, len(params[i:]))
	for j:=0; j<len(samples); j++ {
		samples[j], _ = strconv.ParseInt(params[i], 10, 64)
		i++
	}
        p.ctx.nsqlookupd.DB.Lock()
        if p.ctx.nsqlookupd.DB.latency[topic] == nil {
                p.ctx.nsqlookupd.DB.latency[topic] = samples
        } else {
		p.ctx.nsqlookupd.DB.latency[topic]=append(p.ctx.nsqlookupd.DB.latency[topic], samples...)
	}
        p.ctx.nsqlookupd.DB.Unlock()
	if len(p.ctx.nsqlookupd.DB.latency[topic]) == 40000 {
		s:= p.ctx.nsqlookupd.DB.latency[topic]
		sort.Slice(s, func(i, j int) bool {return s[i]<s[j]})
		fmt.Printf("~~~~~~~~~~~~~~~~~!!!!!!!!!!!!!\n 50=%v, 95=%v, 99=%v\n", s[int(float64(len(s))*0.5)], s[int(float64(len(s))*0.95)], s[int(float64(len(s))*0.99)])
	}
}

//RTM
func (p *LookupProtocolV1) doLatencyPro(client *ClientV1, reader *bufio.Reader, params []string) {
        //fmt.Printf("recv timeline %v~~~~~~~\n", len(params[1:]))
        i := 0
        topic := string(params[i])
        i++
	index, _:= strconv.Atoi(params[i])
	i++
        samples := make([]int64, len(params[i:]))
	if len(params[i:])==2 && params[i] == "end" {
		p.ctx.nsqlookupd.DB.brokerList[p.ctx.nsqlookupd.DB.TPName][index].peerInfo.curLatency, _ = strconv.ParseInt(params[i+1], 10, 64)
		p.ctx.nsqlookupd.DB.curK2++
		if p.ctx.nsqlookupd.DB.curK2 == p.ctx.nsqlookupd.DB.curK {
			goto calc
		}
	}
        for j:=0; j<len(samples); j++ {
                samples[j], _ = strconv.ParseInt(params[i], 10, 64)
                i++
        }
        p.ctx.nsqlookupd.DB.Lock()
        p.ctx.nsqlookupd.DB.latency[topic]=append(p.ctx.nsqlookupd.DB.latency[topic], samples...)
        p.ctx.nsqlookupd.DB.Unlock()
	return
        //if len(p.ctx.nsqlookupd.DB.latency[topic]) == 40000 {
calc:
		ct1:=time.Now().UnixNano()
                s:= p.ctx.nsqlookupd.DB.latency[topic]
                sort.Slice(s, func(i, j int) bool {return s[i]<s[j]})
                fmt.Printf("~~~~~~~~~~~~~~~~~!!!!!!!!!!!!!\n %v 50=%v, 95=%v, 99=%v\n", len(p.ctx.nsqlookupd.DB.latency[topic]), s[int(float64(len(s))*0.5)], s[int(float64(len(s))*0.95)], s[int(float64(len(s))*0.99)])
        
        	tail := s[int(float64(len(s))*0.99)]
		p.ctx.nsqlookupd.DB.curK2=0
		p.ctx.nsqlookupd.DB.curK=0
		for x:=0; x<p.ctx.nsqlookupd.DB.realK; x++ {
			broker := p.ctx.nsqlookupd.DB.brokerList[p.ctx.nsqlookupd.DB.TPName][x]
			if broker.peerInfo.inProfile == 0 {
				continue
			} else if broker.peerInfo.inProfile == 1 {
				broker.peerInfo.profileChan <- 0
			} else {
        			if tail > 1000000 && broker.peerInfo.curLatency > 1000000 && broker.peerInfo.curLatency > broker.peerInfo.lastLatency && broker.peerInfo.curProfile < broker.peerInfo.prevProfile {
					broker.peerInfo.profileChan <- 3
				} else if tail > 1000000 && broker.peerInfo.curLatency > 1000000 {
                			broker.peerInfo.profileChan <- 2
        			} else if (tail < 800000 && broker.peerInfo.curLatency < 800000) || (tail > 1000000 && broker.peerInfo.curLatency < 800000) {
                			broker.peerInfo.profileChan <- 0
        			} else {
                			broker.peerInfo.profileChan <- 1
        			}
				p.ctx.nsqlookupd.DB.lastTail=tail
				broker.peerInfo.lastLatency=broker.peerInfo.curLatency
			}
		}
		p.ctx.nsqlookupd.DB.latency[topic]=[]int64{}
		fmt.Printf("~~~~~~~!!!!! calc time= %v\n", time.Now().UnixNano()-ct1)
	//}

}

//RTM
func (p *LookupProtocolV1) doTB(client *ClientV1, reader *bufio.Reader, params []string) {
        //fmt.Printf("recv timeline %v~~~~~~~\n", client.peerInfo.RemoteAddress)
        i := 0
        topic := string(params[i])
        i++
        p.ctx.nsqlookupd.DB.Lock()
        if p.ctx.nsqlookupd.DB.tb_times[topic] == nil {
                p.ctx.nsqlookupd.DB.tb_times[topic] = make([][]TimeStamp, p.ctx.nsqlookupd.DB.topicCount[topic])
        }
        p.ctx.nsqlookupd.DB.Unlock()
        pubNum := 0
        for i < len(params) {
                pubID, _ := strconv.Atoi(params[i])
                i++
                pubNum++
                timeLen, _ := strconv.Atoi(params[i])
                i++
                p.ctx.nsqlookupd.DB.tb_times[topic][pubID] = make([]TimeStamp, timeLen)
		fmt.Printf("%v %v %v %v\n", pubID, pubNum, timeLen, len(p.ctx.nsqlookupd.DB.tb_times[topic]))
                for j := 0; j < timeLen; j++ {
                        //p.ctx.nsqlookupd.DB.tb_times[topic][pubID][j], _ = strconv.ParseInt(params[i], 10, 64)
                        p.ctx.nsqlookupd.DB.tb_times[topic][pubID][j]=TimeStamp{t: time.Time{}, nano: 0,}
			p.ctx.nsqlookupd.DB.tb_times[topic][pubID][j].t.UnmarshalJSON([]byte(params[i]))
			p.ctx.nsqlookupd.DB.tb_times[topic][pubID][j].nano = p.ctx.nsqlookupd.DB.tb_times[topic][pubID][j].t.UnixNano()
			i++
                }
        }
	p.ctx.nsqlookupd.DB.Lock()
        p.ctx.nsqlookupd.DB.topicCount2[topic] += pubNum
	p.ctx.nsqlookupd.DB.Unlock()
	//fmt.Printf("%v %v\n", p.ctx.nsqlookupd.DB.topicCount2[topic], p.ctx.nsqlookupd.DB.topicCount[topic])
	if p.ctx.nsqlookupd.DB.topicCount2[topic]==p.ctx.nsqlookupd.DB.topicCount[topic] {
		fmt.Println("~~~~~~~~~~~!!!!!!!!! Start TB test")
		timeline := []TimeStamp{}
		for _, pub:= range p.ctx.nsqlookupd.DB.tb_times[topic] {
			//timeline = p.mergeStamp(timeline, pub)
			timeline=append(timeline,pub...)
		}
		sort.Slice(timeline, func(x,y int) bool {return timeline[x].nano < timeline[y].nano})
		bucketRate := p.ctx.nsqlookupd.DB.TPRate 
		bucketBurst := p.ctx.nsqlookupd.DB.TPBurst
		//bucket := rate.NewLimiter(rate.Limit(bucketRate), bucketBurst)
		fmt.Printf("%v %v %v\n", bucketRate, bucketBurst, len(timeline))
		delay := make([]int64, len(timeline))
		newBSize := p.FitTB(timeline, delay, bucketRate, 1, bucketBurst)
		//p.ctx.nsqlookupd.DB.TBChan <- delay[int(float64(len(delay))*0.99)]
		fmt.Printf("~~~~~~~~~~!!!!!!!!!!!!!!! b=%v\n", newBSize)
	}
	return
}
func (p *LookupProtocolV1) FitTB(timeline []TimeStamp, delay []int64, r int, left int, right int) int {
	fmt.Printf("Fit [%v %v]\n", left, right)
        if left == right {
                bucket := rate.NewLimiter(rate.Limit(r), left)
                index := 0
                for index < len(timeline) {
                        r1 := bucket.ReserveN(timeline[index].t, 1)
                        delay[index]=int64(r1.DelayFrom(timeline[index].t))
                        index++
                }
                sort.Slice(delay, func(i, j int) bool {return delay[i]<delay[j]})
                log.Printf("50=%v, 95=%v, 99=%v \n", delay[int(float64(len(delay))*0.5)], delay[int(float64(len(delay))*0.95)], delay[int(float64(len(delay))*0.99)])
		if delay[int(float64(len(delay))*0.99)] > 1 {
                        return -1
                } else {
                        return left
                }
        }
        mid:= left + (right-left)/2
	//mid:=right //for ground-truth test
        bucket := rate.NewLimiter(rate.Limit(r), mid)
        index := 0
        for index < len(timeline) {
                r1 := bucket.ReserveN(timeline[index].t, 1)
                delay[index]=int64(r1.DelayFrom(timeline[index].t))
                index++
        }
        sort.Slice(delay, func(i, j int) bool {return delay[i]<delay[j]})
        log.Printf("50=%v, 95=%v, 99=%v \n", delay[int(float64(len(delay))*0.5)], delay[int(float64(len(delay))*0.95)], delay[int(float64(len(delay))*0.99)])
	//return mid //for ground-truth test

	if delay[int(float64(len(delay))*0.99)] > 1 {
                return p.FitTB(timeline, delay, r, mid+1, right)
        }
	result := p.FitTB(timeline, delay, r,  left, mid)
        if result == -1 {
                return mid
        } else {
                return result
        }
}

//RTM
func (p *LookupProtocolV1) doTP(client *ClientV1, reader *bufio.Reader, params []string) {
        //fmt.Printf("recv timeline %v~~~~~~~\n", client.peerInfo.RemoteAddress)
        i := 0
        topic := string(params[i])
        if strings.HasPrefix(topic, "p") {
                topic = topic[1:]
        }
        i++
        p.ctx.nsqlookupd.DB.Lock()
        if p.ctx.nsqlookupd.DB.tp_times[topic] == nil {
                p.ctx.nsqlookupd.DB.tp_times[topic] = make([][]int64, p.ctx.nsqlookupd.DB.topicCount[topic])
        }
        p.ctx.nsqlookupd.DB.Unlock()
        pubNum := 0
        for i < len(params) {
                pubID, _ := strconv.Atoi(params[i])
                i++
                pubNum++
                timeLen, _ := strconv.Atoi(params[i])
		i++
                p.ctx.nsqlookupd.DB.tp_times[topic][pubID] = make([]int64, timeLen)
                for j := 0; j < timeLen; j++ {
                        p.ctx.nsqlookupd.DB.tp_times[topic][pubID][j], _ = strconv.ParseInt(params[i], 10, 64)
                        i++
                }
        }
	p.ctx.nsqlookupd.DB.Lock()
        p.ctx.nsqlookupd.DB.topicCount2[topic] += pubNum
        p.ctx.nsqlookupd.DB.Unlock()
	if p.ctx.nsqlookupd.DB.topicCount2[topic]==p.ctx.nsqlookupd.DB.topicCount[topic] {
		producers := p.ctx.nsqlookupd.DB.FindProducers("client", "", "")
		for _, c:=range producers{
			c.peerInfo.rCap = p.doEst(c.peerInfo.tp_times)
			fmt.Printf("the 1st rcap est=%v\n", c.peerInfo.rCap)
		}
		sort.Sort(Cand(producers))
		k := p.ctx.nsqlookupd.DB.kValue
		tp_times := p.ctx.nsqlookupd.DB.tp_times[topic]

		for _, c:= range producers {
			c.peerInfo.tp_times = []int64{}
			c.peerInfo.tp_times2 = c.peerInfo.tp_times
			fmt.Printf("token rate = %v\n",  c.peerInfo.tokenRate)
		}
		p.ctx.nsqlookupd.DB.topicDist[topic] = make([]*Producer, p.ctx.nsqlookupd.DB.topicCount[topic])
                curRate := float64(0)
                for x:=0; x<len(tp_times);x++ {
			rate := float64(len(tp_times[x]))/float64(tp_times[x][len(tp_times[x])-1]-tp_times[x][0])*float64(1000000000)
                        curRate += rate
                        minCC := float64(100000000000000)
                        minID := -1
                                for i = 0; i < k; i++ {
					producers[i].peerInfo.tp_times2=p.merge(producers[i].peerInfo.tp_times, tp_times[x])
                                        aTimes := make([]float64, len(producers[i].peerInfo.tp_times2)-1)
                                        for j:=1; j<len(producers[i].peerInfo.tp_times2); j++ {
                                                aTimes[j-1]=float64(producers[i].peerInfo.tp_times2[j]-producers[i].peerInfo.tp_times2[j-1])/float64(1000000000)
                                        }
                                        v:=stat.Variance(aTimes, nil)
                                        //fmt.Printf("[%v]pub %v, rate=%v, at broker %v, v=%v\n",time.Now().UnixNano(), x, rate, i, v)
                                        producers[i].peerInfo.curVar = v
                                        u:=float64(0)
                                        for j:=0; j< k; j++ {
                                                if j==i {
                                                        u+=((producers[i].peerInfo.curConn+rate)/curRate)*(producers[i].peerInfo.curConn+rate)*v/(2*(1-(producers[i].peerInfo.curConn+rate)/producers[i].peerInfo.tokenRate))
                                                } else {
                                                        u+=(producers[j].peerInfo.curConn/curRate)*(producers[j].peerInfo.curConn*producers[j].peerInfo.curRisk)/(2*(1-producers[j].peerInfo.curConn/producers[j].peerInfo.tokenRate))
                                                }
                                        }
                                        //fmt.Printf("u=%v\n", u)
                                        if u < minCC {
                                                minCC = u
                                                minID = i
                                        }
                                }
			if minID != -1 {
                                producers[minID].peerInfo.curRisk = producers[minID].peerInfo.curVar
                                producers[minID].peerInfo.curConn += rate
                                producers[minID].peerInfo.tp_times = producers[minID].peerInfo.tp_times2
				p.ctx.nsqlookupd.DB.topicDist[topic][x] = producers[minID]
				if x%1000 == 0 {
					fmt.Printf("[%v]minCC=%v, minID=%v\n", time.Now().UnixNano(), minCC, minID)
				}
                        }
                }
		for _, c:= range producers {
			c.peerInfo.tp_times= []int64{}
			c.peerInfo.tp_times2 = []int64{}
			fmt.Printf("rate = %v\n", c.peerInfo.curConn)
		}
	}
}


//RTM
func (p *LookupProtocolV1) doCPU(client *ClientV1, reader *bufio.Reader, params []string) ([]byte, error) {
	if client.peerInfo == nil {
		return nil, protocol.NewFatalClientErr(nil, "E_INVALID", "client must IDENTIFY")
	}
	//_, _ = protocol.SendResponse(producer.peerInfo.client, []byte("OK"))
	//client.peerInfo.QLen, _ = strconv.Atoi(params[0])
	client.peerInfo.Lock()
	client.peerInfo.CPU, _ = strconv.ParseFloat(params[0], 64)
	if len(params) > 1 {
		client.peerInfo.CPUVar, _ = strconv.ParseFloat(params[1], 64)
	}
	//fmt.Printf("CPU=%v  Var=%v\n", client.peerInfo.CPU, client.peerInfo.CPUVar)
	if len(params) > 2 {
		client.peerInfo.risk, _ = strconv.ParseFloat(params[2], 64)
		//fmt.Printf("risk is %v\n", client.peerInfo.risk)
		//client.peerInfo.rCap = 0.8 - client.peerInfo.risk
	}
	client.peerInfo.Unlock()
	client.peerInfo.PrevConnCount = client.peerInfo.ConnCount
	return nil, nil
}

func (p *LookupProtocolV1) REGISTER(client *ClientV1, reader *bufio.Reader, params []string) ([]byte, error) {
	if client.peerInfo == nil {
		return nil, protocol.NewFatalClientErr(nil, "E_INVALID", "client must IDENTIFY")
	}

	topic, channel, err := getTopicChan("REGISTER", params)
	if err != nil {
		return nil, err
	}

	//RTM
	if topic == "" && channel == "" {
		client.peerInfo.ConnCount += 1
		//RTM
		return []byte("OK"), nil
		//return nil, nil
	}

	if channel != "" {
		key := Registration{"channel", topic, channel}
		if p.ctx.nsqlookupd.DB.AddProducer(key, &Producer{peerInfo: client.peerInfo}) {
			p.ctx.nsqlookupd.logf("DB: client(%s) REGISTER category:%s key:%s subkey:%s",
				client, "channel", topic, channel)
			//RTM
			client.peerInfo.ChannelCount += 1
		}
	}
	key := Registration{"topic", topic, ""}

	if client.peerInfo.DaemonPriority == "HIGH" { //RTM.dtb: don't register at initbroker; Future work!

	if p.ctx.nsqlookupd.DB.AddProducer(key, &Producer{peerInfo: client.peerInfo}) {
		p.ctx.nsqlookupd.logf("DB: client(%s) REGISTER category:%s key:%s subkey:%s",
			client, "topic", topic, "")
		//RTM
		client.peerInfo.TopicCount += 1
		client.peerInfo.rates[topic] = &RateUpdate{
			rate:     0,
			updated:  false,
			newrate:  0,
			sendback: false,
		}
	}
	}

	// jarry multicast
	if channel == "" && p.ctx.nsqlookupd.opts.MulticastFlag {
		// return multicast addr when register topic
		MTCAddr, errno := p.ctx.nsqlookupd.Manager.RegisterMTCAddr(topic)
		p.ctx.nsqlookupd.logf("MANAGER: Register topic %s for address %s", topic, MTCAddr.String())
		if errno != -1 {
			return []byte(MTCAddr.String()), nil
		}
	}

	//RTM
	return []byte("OK"), nil
	//return nil, nil
}

func (p *LookupProtocolV1) UNREGISTER(client *ClientV1, reader *bufio.Reader, params []string) ([]byte, error) {
	if client.peerInfo == nil {
		return nil, protocol.NewFatalClientErr(nil, "E_INVALID", "client must IDENTIFY")
	}

	topic, channel, err := getTopicChan("UNREGISTER", params)
	if err != nil {
		return nil, err
	}

	//RTM
	if topic == "" && channel == "" {
		client.peerInfo.ConnCount -= 1
		//client.peerInfo.FConnCount -= 1
		return []byte("OK"), nil
		//return nil, nil
	}

	if channel != "" {
		key := Registration{"channel", topic, channel}
		removed, left := p.ctx.nsqlookupd.DB.RemoveProducer(key, client.peerInfo.id)
		if removed {
			p.ctx.nsqlookupd.logf("DB: client(%s) UNREGISTER category:%s key:%s subkey:%s",
				client, "channel", topic, channel)
			//RTM
			client.peerInfo.ChannelCount -= 1
		}
		// for ephemeral channels, remove the channel as well if it has no producers
		if left == 0 && strings.HasSuffix(channel, "#ephemeral") {
			p.ctx.nsqlookupd.DB.RemoveRegistration(key)
		}
	} else {
		// no channel was specified so this is a topic unregistration
		// remove all of the channel registrations...
		// normally this shouldn't happen which is why we print a warning message
		// if anything is actually removed
		registrations := p.ctx.nsqlookupd.DB.FindRegistrations("channel", topic, "*")
		for _, r := range registrations {
			if removed, _ := p.ctx.nsqlookupd.DB.RemoveProducer(r, client.peerInfo.id); removed {
				p.ctx.nsqlookupd.logf("WARNING: client(%s) unexpected UNREGISTER category:%s key:%s subkey:%s",
					client, "channel", topic, r.SubKey)
				//RTM
				client.peerInfo.ChannelCount -= 1
			}
		}

		// key := Registration{"topic", topic, ""}
		// if removed, _ := p.ctx.nsqlookupd.DB.RemoveProducer(key, client.peerInfo.id); removed {
		// 	p.ctx.nsqlookupd.logf("DB: client(%s) UNREGISTER category:%s key:%s subkey:%s",
		// 		client, "topic", topic, "")

		// 	//yao
		// 	if strings.HasSuffix(topic, "#ephemeral") {
		// 		p.ctx.nsqlookupd.DB.RemoveRegistration(key)
		// 	}
		// 	//
		// }
		//yao
		for _, r := range p.ctx.nsqlookupd.DB.FindRegistrations("topic", topic, "*") {
			if removed, _ := p.ctx.nsqlookupd.DB.RemoveProducer(r, client.peerInfo.id); removed {
				p.ctx.nsqlookupd.logf("DB: client(%s) UNREGISTER category:%s key:%s subkey:%s",
					client, "topic", topic, r.SubKey)
				//RTM
				client.peerInfo.TopicCount -= 1
				client.peerInfo.rates[topic] = nil
			}
			/*
				if strings.HasSuffix(topic, "#ephemeral") {
					p.ctx.nsqlookupd.logf("DB: client(%s) REMOVE category:%s key:%s subkey:%s",
						client, "topic", topic, r.SubKey)
					p.ctx.nsqlookupd.DB.RemoveRegistration(r)
				}
			*/
		}

	}
	// jarry: remove multicast addr
	if channel == "" && p.ctx.nsqlookupd.opts.MulticastFlag {
		p.ctx.nsqlookupd.Manager.UnregisterMTCAddr(topic)
	}
	//RTM
	return []byte("OK"), nil
	//return nil, nil
}

func (p *LookupProtocolV1) IDENTIFY(client *ClientV1, reader *bufio.Reader, params []string) ([]byte, error) {
	var err error

	if client.peerInfo != nil {
		return nil, protocol.NewFatalClientErr(err, "E_INVALID", "cannot IDENTIFY again")
	}

	var bodyLen int32
	err = binary.Read(reader, binary.BigEndian, &bodyLen)
	if err != nil {
		return nil, protocol.NewFatalClientErr(err, "E_BAD_BODY", "IDENTIFY failed to read body size")
	}

	body := make([]byte, bodyLen)
	_, err = io.ReadFull(reader, body)
	if err != nil {
		return nil, protocol.NewFatalClientErr(err, "E_BAD_BODY", "IDENTIFY failed to read body")
	}

	// body is a json structure with producer information
	peerInfo := PeerInfo{id: client.RemoteAddr().String()}
	err = json.Unmarshal(body, &peerInfo)
	if err != nil {
		return nil, protocol.NewFatalClientErr(err, "E_BAD_BODY", "IDENTIFY failed to decode JSON body")
	}

	peerInfo.RemoteAddress = client.RemoteAddr().String()

	//RTM
	peerInfo.TopicCount = 0
	peerInfo.rates = make(map[string]*RateUpdate)
	peerInfo.bursts = make(map[string]float64)
	peerInfo.tp_times = []int64{}
	peerInfo.tp_times2 = []int64{}
	peerInfo.client = client
	peerInfo.BackMsg = [][]byte{}

	// require all fields
	if peerInfo.BroadcastAddress == "" || peerInfo.TCPPort == 0 || peerInfo.HTTPPort == 0 || peerInfo.Version == "" || peerInfo.DaemonPriority == "" {
		return nil, protocol.NewFatalClientErr(nil, "E_BAD_BODY", "IDENTIFY missing fields")
	}

	atomic.StoreInt64(&peerInfo.lastUpdate, time.Now().UnixNano())

	p.ctx.nsqlookupd.logf("CLIENT(%s): IDENTIFY Address:%s TCP:%d HTTP:%d Version:%s Daemon Priority: %s",
		client, peerInfo.BroadcastAddress, peerInfo.TCPPort, peerInfo.HTTPPort, peerInfo.Version, peerInfo.DaemonPriority)

	client.peerInfo = &peerInfo
	if peerInfo.DaemonPriority == "BACKUP" {
		p.ctx.nsqlookupd.DB.lastClient.peerInfo.backup=&peerInfo
		fmt.Printf("!!!!!!!set a backup client of %s\n", p.ctx.nsqlookupd.DB.lastClient)
	} else {
	if p.ctx.nsqlookupd.DB.AddProducer(Registration{"client", "", ""}, &Producer{peerInfo: client.peerInfo}) {
		p.ctx.nsqlookupd.logf("DB: client(%s) REGISTER category:%s key:%s subkey:%s", client, "client", "", "")
	}
	p.ctx.nsqlookupd.DB.lastClient = client
	}

	// build a response
	data := make(map[string]interface{})
	data["tcp_port"] = p.ctx.nsqlookupd.RealTCPAddr().Port
	data["http_port"] = p.ctx.nsqlookupd.RealHTTPAddr().Port
	data["version"] = version.Binary
	hostname, err := os.Hostname()
	if err != nil {
		log.Fatalf("ERROR: unable to get hostname %s", err)
	}
	data["broadcast_address"] = p.ctx.nsqlookupd.opts.BroadcastAddress
	data["hostname"] = hostname

	response, err := json.Marshal(data)
	if err != nil {
		p.ctx.nsqlookupd.logf("ERROR: marshaling %v", data)
		return []byte("OK"), nil
	}
	return response, nil
}

func (p *LookupProtocolV1) PING(client *ClientV1, params []string) ([]byte, error) {
	if client.peerInfo != nil {
		// we could get a PING before other commands on the same client connection
		//cur := time.Unix(0, atomic.LoadInt64(&client.peerInfo.lastUpdate))
		now := time.Now()
		//p.ctx.nsqlookupd.logf("CLIENT(%s): pinged (last ping %s)", client.peerInfo.id,
		//	now.Sub(cur))
		atomic.StoreInt64(&client.peerInfo.lastUpdate, now.UnixNano())
	}
	return []byte("OK"), nil
}
