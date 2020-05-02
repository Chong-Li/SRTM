package nsqlookupd

import (
	"fmt"
	"math"
	"math/rand"
	"net/http"
	"net/http/pprof"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
	//"sort"

	"github.com/WU-CPSL/RTM-0.1/nsqio/nsq/internal/http_api"
	"github.com/WU-CPSL/RTM-0.1/nsqio/nsq/internal/protocol"
	"github.com/WU-CPSL/RTM-0.1/nsqio/nsq/internal/version"
	"github.com/julienschmidt/httprouter"
)

type httpServer struct {
	ctx    *Context
	router http.Handler
}

func newHTTPServer(ctx *Context) *httpServer {
	log := http_api.Log(ctx.nsqlookupd.opts.Logger)

	router := httprouter.New()
	router.HandleMethodNotAllowed = true
	router.PanicHandler = http_api.LogPanicHandler(ctx.nsqlookupd.opts.Logger)
	router.NotFound = http_api.LogNotFoundHandler(ctx.nsqlookupd.opts.Logger)
	router.MethodNotAllowed = http_api.LogMethodNotAllowedHandler(ctx.nsqlookupd.opts.Logger)
	s := &httpServer{
		ctx:    ctx,
		router: router,
	}

	router.Handle("GET", "/ping", http_api.Decorate(s.pingHandler, log, http_api.PlainText))

	// v1 negotiate
	router.Handle("GET", "/debug", http_api.Decorate(s.doDebug, log, http_api.NegotiateVersion))
	router.Handle("GET", "/lookup", http_api.Decorate(s.doLookup, log, http_api.NegotiateVersion))
	router.Handle("GET", "/topics", http_api.Decorate(s.doTopics, log, http_api.NegotiateVersion))
	router.Handle("GET", "/channels", http_api.Decorate(s.doChannels, log, http_api.NegotiateVersion))
	router.Handle("GET", "/nodes", http_api.Decorate(s.doNodes, log, http_api.NegotiateVersion))
	//yao
	// /priority_nodes priority
	router.Handle("GET", "/priority_nodes", http_api.Decorate(s.doPriorityNodes, log, http_api.NegotiateVersion))
	// /register_topic_priority
	router.Handle("GET", "/register_topic_priority", http_api.Decorate(s.doRegisterTopicPriority, log, http_api.NegotiateVersion))
	// /unregister_topic_priority
	router.Handle("GET", "/unregister_topic_priority", http_api.Decorate(s.doUnregisterTopicPriority, log, http_api.NegotiateVersion))
	//lookup for the producers, based on topic
	router.Handle("GET", "/producer_lookup", http_api.Decorate(s.doProducerLookup, log, http_api.NegotiateVersion))
	//lookup for the producers v2, based on specified priority
	router.Handle("GET", "/producer_lookup_v2", http_api.Decorate(s.doProducerLookupV2, log, http_api.NegotiateVersion))

	//lookup for the producers v3, based on priority and topic
	router.Handle("GET", "/producer_lookup_v3", http_api.Decorate(s.doProducerLookupV3, log, http_api.NegotiateVersion))

	// only v1
	router.Handle("POST", "/topic/create", http_api.Decorate(s.doCreateTopic, log, http_api.V1))
	router.Handle("POST", "/topic/delete", http_api.Decorate(s.doDeleteTopic, log, http_api.V1))
	router.Handle("POST", "/channel/create", http_api.Decorate(s.doCreateChannel, log, http_api.V1))
	router.Handle("POST", "/channel/delete", http_api.Decorate(s.doDeleteChannel, log, http_api.V1))
	router.Handle("POST", "/topic/tombstone", http_api.Decorate(s.doTombstoneTopicProducer, log, http_api.V1))

	// deprecated, v1 negotiate
	router.Handle("GET", "/info", http_api.Decorate(s.doInfo, log, http_api.NegotiateVersion))
	router.Handle("POST", "/create_topic", http_api.Decorate(s.doCreateTopic, log, http_api.NegotiateVersion))
	router.Handle("POST", "/delete_topic", http_api.Decorate(s.doDeleteTopic, log, http_api.NegotiateVersion))
	router.Handle("POST", "/create_channel", http_api.Decorate(s.doCreateChannel, log, http_api.NegotiateVersion))
	router.Handle("POST", "/delete_channel", http_api.Decorate(s.doDeleteChannel, log, http_api.NegotiateVersion))
	router.Handle("POST", "/tombstone_topic_producer", http_api.Decorate(s.doTombstoneTopicProducer, log, http_api.NegotiateVersion))
	router.Handle("GET", "/create_topic", http_api.Decorate(s.doCreateTopic, log, http_api.NegotiateVersion))
	router.Handle("GET", "/delete_topic", http_api.Decorate(s.doDeleteTopic, log, http_api.NegotiateVersion))
	router.Handle("GET", "/create_channel", http_api.Decorate(s.doCreateChannel, log, http_api.NegotiateVersion))
	router.Handle("GET", "/delete_channel", http_api.Decorate(s.doDeleteChannel, log, http_api.NegotiateVersion))
	router.Handle("GET", "/tombstone_topic_producer", http_api.Decorate(s.doTombstoneTopicProducer, log, http_api.NegotiateVersion))

	// debug
	router.HandlerFunc("GET", "/debug/pprof", pprof.Index)
	router.HandlerFunc("GET", "/debug/pprof/cmdline", pprof.Cmdline)
	router.HandlerFunc("GET", "/debug/pprof/symbol", pprof.Symbol)
	router.HandlerFunc("POST", "/debug/pprof/symbol", pprof.Symbol)
	router.HandlerFunc("GET", "/debug/pprof/profile", pprof.Profile)
	router.Handler("GET", "/debug/pprof/heap", pprof.Handler("heap"))
	router.Handler("GET", "/debug/pprof/goroutine", pprof.Handler("goroutine"))
	router.Handler("GET", "/debug/pprof/block", pprof.Handler("block"))
	router.Handler("GET", "/debug/pprof/threadcreate", pprof.Handler("threadcreate"))

	return s
}

func (s *httpServer) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	s.router.ServeHTTP(w, req)
}

func (s *httpServer) pingHandler(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	return "OK", nil
}

func (s *httpServer) doInfo(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	return struct {
		Version string `json:"version"`
	}{
		Version: version.Binary,
	}, nil
}

func (s *httpServer) doTopics(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	topics := s.ctx.nsqlookupd.DB.FindRegistrations("topic", "*", "").Keys()
	return map[string]interface{}{
		"topics": topics,
	}, nil
}

func (s *httpServer) doChannels(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	reqParams, err := http_api.NewReqParams(req)
	if err != nil {
		return nil, http_api.Err{400, "INVALID_REQUEST"}
	}

	topicName, err := reqParams.Get("topic")
	if err != nil {
		return nil, http_api.Err{400, "MISSING_ARG_TOPIC"}
	}

	channels := s.ctx.nsqlookupd.DB.FindRegistrations("channel", topicName, "*").SubKeys()
	return map[string]interface{}{
		"channels": channels,
	}, nil
}

func (s *httpServer) doLookup(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	reqParams, err := http_api.NewReqParams(req)
	if err != nil {
		return nil, http_api.Err{400, "INVALID_REQUEST"}
	}

	topicName, err := reqParams.Get("topic")
	if err != nil {
		return nil, http_api.Err{400, "MISSING_ARG_TOPIC"}
	}

	registration := s.ctx.nsqlookupd.DB.FindRegistrations("topic", topicName, "")
	if len(registration) == 0 {
		return nil, http_api.Err{404, "TOPIC_NOT_FOUND"}
	}

	channels := s.ctx.nsqlookupd.DB.FindRegistrations("channel", topicName, "*").SubKeys()
	producers := s.ctx.nsqlookupd.DB.FindProducers("topic", topicName, "")
	producers = producers.FilterByActive(s.ctx.nsqlookupd.opts.InactiveProducerTimeout,
		s.ctx.nsqlookupd.opts.TombstoneLifetime)
	mtcstring := ""
	if s.ctx.nsqlookupd.opts.MulticastFlag {
		mtcaddr, _ := s.ctx.nsqlookupd.Manager.GetMTCAddr(topicName)
		mtcstring = mtcaddr.String()
	}
	return map[string]interface{}{
		"channels":   channels,
		"producers":  producers.PeerInfo(),
		"mtcaddress": mtcstring,
	}, nil
}

func (s *httpServer) doProducerLookup(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	reqParams, err := http_api.NewReqParams(req)
	if err != nil {
		return nil, http_api.Err{400, "INVALID_REQUEST"}
	}

	topicName, err := reqParams.Get("topic")
	if err != nil {
		return nil, http_api.Err{400, "MISSING_ARG_TOPIC"}
	}

	//check if the we need to return the address of another blank nsqd incase the producer and the active nsqd is not on the same host
	okFlag := false
	senderHostname, err := reqParams.Get("hostname")
	if err != nil {
		return nil, http_api.Err{400, "MISSING_ARG_TOPIC"}
	}
	tempProducers := s.ctx.nsqlookupd.DB.FindProducers("topic", topicName, "")
	tempProducers = tempProducers.FilterByActive(s.ctx.nsqlookupd.opts.InactiveProducerTimeout,
		s.ctx.nsqlookupd.opts.TombstoneLifetime)
	for _, thisProducer := range tempProducers {
		if thisProducer.peerInfo.Hostname == senderHostname {
			okFlag = true
			break
		}
	}

	registration := s.ctx.nsqlookupd.DB.FindRegistrations("topic", topicName, "")
	if len(registration) == 0 || okFlag == false {
		//return nil, http_api.Err{404, "TOPIC_NOT_FOUND"}
		//yao
		//now we do a lookup in the registration table to find if there is a preset topic
		//the preset topic in most time should be a high prioirty topic
		//in that case we should return a NSQd which has a high priority
		//if htere is no such high priority daemon, return error

		//check if the topic is in the high priority list
		registration = s.ctx.nsqlookupd.DB.FindRegistrations("high_priority_topic", topicName, "")
		if len(registration) == 0 {
			return nil, http_api.Err{404, "TOPIC_NOT_FOUND"}
		} else if len(registration) > 1 {
			println("YAO WARNING: DUPLICATE NAME OF HIGH PRIOIRTY TOPIC")
		}
		//if the topic is among the high prioirty list
		//get a daemon with the same prioirty of  the topic
		producers := s.ctx.nsqlookupd.DB.FindProducers("client", "", "").FilterByActive(s.ctx.nsqlookupd.opts.InactiveProducerTimeout, 0)
		var tempPeerInfo []*PeerInfo
		for _, p := range producers {
			priority := p.peerInfo.DaemonPriority
			if priority == "HIGH" {
				tempPeerInfo = append(tempPeerInfo, p.peerInfo)
			}
		}
		var channels []string
		if len(tempPeerInfo) > 0 {
			//s.ctx.nsqlookupd.logf("TOPIC: %s is handled by Daemon %d, Length is: %d", topicName, tempPeerInfo[0].TCPPort, len(tempPeerInfo))
			return map[string]interface{}{
				"channels":  channels,
				"producers": tempPeerInfo,
			}, nil
		}
		//if we dont have any high priority dameons, return nil
		return nil, http_api.Err{404, "NO HIGH PRIORITY NSQds AVAILABLE"}
	}
	//we do find a NSQd serving the topic
	//return that nsqd's info
	channels := s.ctx.nsqlookupd.DB.FindRegistrations("channel", topicName, "*").SubKeys()
	producers := s.ctx.nsqlookupd.DB.FindProducers("topic", topicName, "")
	producers = producers.FilterByActive(s.ctx.nsqlookupd.opts.InactiveProducerTimeout,
		s.ctx.nsqlookupd.opts.TombstoneLifetime)

	//even if the topic is registered in the lookupd, it might be the case that no lookupds are currently holding it
	if len(producers) == 0 {
		return nil, http_api.Err{404, "NO NSQd IS HOLDING THE TOPIC"}
	}
	return map[string]interface{}{
		"channels":  channels,
		"producers": producers.PeerInfo(),
	}, nil
}

func (s *httpServer) doCreateTopic(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	reqParams, err := http_api.NewReqParams(req)
	if err != nil {
		return nil, http_api.Err{400, "INVALID_REQUEST"}
	}

	topicName, err := reqParams.Get("topic")
	if err != nil {
		return nil, http_api.Err{400, "MISSING_ARG_TOPIC"}
	}

	if !protocol.IsValidTopicName(topicName) {
		return nil, http_api.Err{400, "INVALID_ARG_TOPIC"}
	}

	s.ctx.nsqlookupd.logf("DB: adding topic(%s)", topicName)
	key := Registration{"topic", topicName, ""}
	s.ctx.nsqlookupd.DB.AddRegistration(key)

	return nil, nil
}

func (s *httpServer) doDeleteTopic(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	reqParams, err := http_api.NewReqParams(req)
	if err != nil {
		return nil, http_api.Err{400, "INVALID_REQUEST"}
	}

	topicName, err := reqParams.Get("topic")
	if err != nil {
		return nil, http_api.Err{400, "MISSING_ARG_TOPIC"}
	}

	registrations := s.ctx.nsqlookupd.DB.FindRegistrations("channel", topicName, "*")
	for _, registration := range registrations {
		s.ctx.nsqlookupd.logf("DB: removing channel(%s) from topic(%s)", registration.SubKey, topicName)
		s.ctx.nsqlookupd.DB.RemoveRegistration(registration)
	}

	registrations = s.ctx.nsqlookupd.DB.FindRegistrations("topic", topicName, "")
	for _, registration := range registrations {
		s.ctx.nsqlookupd.logf("DB: removing topic(%s)", topicName)
		s.ctx.nsqlookupd.DB.RemoveRegistration(registration)
	}

	return nil, nil
}

func (s *httpServer) doTombstoneTopicProducer(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	reqParams, err := http_api.NewReqParams(req)
	if err != nil {
		return nil, http_api.Err{400, "INVALID_REQUEST"}
	}

	topicName, err := reqParams.Get("topic")
	if err != nil {
		return nil, http_api.Err{400, "MISSING_ARG_TOPIC"}
	}

	node, err := reqParams.Get("node")
	if err != nil {
		return nil, http_api.Err{400, "MISSING_ARG_NODE"}
	}

	s.ctx.nsqlookupd.logf("DB: setting tombstone for producer@%s of topic(%s)", node, topicName)
	producers := s.ctx.nsqlookupd.DB.FindProducers("topic", topicName, "")
	for _, p := range producers {
		thisNode := fmt.Sprintf("%s:%d", p.peerInfo.BroadcastAddress, p.peerInfo.HTTPPort)
		if thisNode == node {
			p.Tombstone()
		}
	}

	return nil, nil
}

func (s *httpServer) doCreateChannel(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	reqParams, err := http_api.NewReqParams(req)
	if err != nil {
		return nil, http_api.Err{400, "INVALID_REQUEST"}
	}

	topicName, channelName, err := http_api.GetTopicChannelArgs(reqParams)
	if err != nil {
		return nil, http_api.Err{400, err.Error()}
	}

	s.ctx.nsqlookupd.logf("DB: adding channel(%s) in topic(%s)", channelName, topicName)
	key := Registration{"channel", topicName, channelName}
	s.ctx.nsqlookupd.DB.AddRegistration(key)

	s.ctx.nsqlookupd.logf("DB: adding topic(%s)", topicName)
	key = Registration{"topic", topicName, ""}
	s.ctx.nsqlookupd.DB.AddRegistration(key)

	return nil, nil
}

func (s *httpServer) doDeleteChannel(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	reqParams, err := http_api.NewReqParams(req)
	if err != nil {
		return nil, http_api.Err{400, "INVALID_REQUEST"}
	}

	topicName, channelName, err := http_api.GetTopicChannelArgs(reqParams)
	if err != nil {
		return nil, http_api.Err{400, err.Error()}
	}

	registrations := s.ctx.nsqlookupd.DB.FindRegistrations("channel", topicName, channelName)
	if len(registrations) == 0 {
		return nil, http_api.Err{404, "CHANNEL_NOT_FOUND"}
	}

	s.ctx.nsqlookupd.logf("DB: removing channel(%s) from topic(%s)", channelName, topicName)
	for _, registration := range registrations {
		s.ctx.nsqlookupd.DB.RemoveRegistration(registration)
	}

	return nil, nil
}

type node struct {
	RemoteAddress    string   `json:"remote_address"`
	TCPPort          int      `json:"tcp_port"`
	Hostname         string   `json:"hostname"`
	ConnType         int      `json:"connection_type"`
	BroadcastAddress string   `json:"broadcast_address"`
	HTTPPort         int      `json:"http_port"`
	Version          string   `json:"version"`
	Tombstones       []bool   `json:"tombstones"`
	Topics           []string `json:"topics"`
}

func (s *httpServer) doNodes(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	// dont filter out tombstoned nodes
	producers := s.ctx.nsqlookupd.DB.FindProducers("client", "", "").FilterByActive(
		s.ctx.nsqlookupd.opts.InactiveProducerTimeout, 0)
	nodes := make([]*node, len(producers))
	for i, p := range producers {
		topics := s.ctx.nsqlookupd.DB.LookupRegistrations(p.peerInfo.id).Filter("topic", "*", "").Keys()

		// for each topic find the producer that matches this peer
		// to add tombstone information
		tombstones := make([]bool, len(topics))
		for j, t := range topics {
			topicProducers := s.ctx.nsqlookupd.DB.FindProducers("topic", t, "")
			for _, tp := range topicProducers {
				if tp.peerInfo == p.peerInfo {
					tombstones[j] = tp.IsTombstoned(s.ctx.nsqlookupd.opts.TombstoneLifetime)
				}
			}
		}

		nodes[i] = &node{
			RemoteAddress:    p.peerInfo.RemoteAddress,
			TCPPort:          p.peerInfo.TCPPort,
			Hostname:         p.peerInfo.Hostname,
			ConnType:         p.peerInfo.ConnType,
			BroadcastAddress: p.peerInfo.BroadcastAddress,
			HTTPPort:         p.peerInfo.HTTPPort,
			Version:          p.peerInfo.Version,
			Tombstones:       tombstones,
			Topics:           topics,
		}
	}

	return map[string]interface{}{
		"producers": nodes,
	}, nil
}

func (s *httpServer) doDebug(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	s.ctx.nsqlookupd.DB.RLock()
	defer s.ctx.nsqlookupd.DB.RUnlock()

	data := make(map[string][]map[string]interface{})
	for r, producers := range s.ctx.nsqlookupd.DB.registrationMap {
		key := r.Category + ":" + r.Key + ":" + r.SubKey
		for _, p := range producers {
			m := map[string]interface{}{
				"id":                p.peerInfo.id,
				"hostname":          p.peerInfo.Hostname,
				"broadcast_address": p.peerInfo.BroadcastAddress,
				"http_port":         p.peerInfo.HTTPPort,
				"version":           p.peerInfo.Version,
				"last_update":       atomic.LoadInt64(&p.peerInfo.lastUpdate),
				"tombstoned":        p.tombstoned,
				"tombstoned_at":     p.tombstonedAt.UnixNano(),
			}
			data[key] = append(data[key], m)
		}
	}

	return data, nil
}

//yao
//register topic priority
//register_topic_priority topicName
func (s *httpServer) doRegisterTopicPriority(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {

	reqParams, err := http_api.NewReqParams(req)
	if err != nil {
		return nil, http_api.Err{400, "INVALID_REQUEST"}
	}

	topicName, err := http_api.GetTopicPriorityArgs(reqParams)
	if err != nil {
		return nil, http_api.Err{400, err.Error()}
	}

	s.ctx.nsqlookupd.logf("DB: adding topic (%s) to high priority", topicName)

	//if there has already had such topic in the high prio list, return
	if len(s.ctx.nsqlookupd.DB.FindRegistrations("high_priority_topic", topicName, "")) > 0 {
		return map[string]interface{}{
			"status": "success",
		}, nil
	}
	//else add it
	key := Registration{"high_priority_topic", topicName, ""}
	s.ctx.nsqlookupd.DB.AddRegistration(key)

	//we need to check if the topic is currently served by any other daemons
	//if there is a daemon and the daemon is low priority, we need to modify this registration so that next time
	//the topic can be served by high priority nsqds
	//set the #inactive flag to make the currently active producer sleep
	producers := s.ctx.nsqlookupd.DB.FindProducers("topic", topicName, "")
	producers = producers.FilterByActive(s.ctx.nsqlookupd.opts.InactiveProducerTimeout, s.ctx.nsqlookupd.opts.TombstoneLifetime)
	for _, p := range producers {
		if p.peerInfo.DaemonPriority == "LOW" {
			previousReg := Registration{"topic", topicName, ""}
			currentReg := Registration{"topic", topicName, "#inactive"}
			//s.ctx.nsqlookupd.DB.RemoveRegistration(previousReg)
			s.ctx.nsqlookupd.DB.RemoveProducer(previousReg, p.peerInfo.id)
			s.ctx.nsqlookupd.DB.AddProducer(currentReg, p)
		}
	}
	//find an inactive correpsonding high priority nsqd
	//switch its flag and set to active
	producers = s.ctx.nsqlookupd.DB.FindProducers("topic", topicName, "#inactive")
	producers = producers.FilterByActive(s.ctx.nsqlookupd.opts.InactiveProducerTimeout, s.ctx.nsqlookupd.opts.TombstoneLifetime)
	if len(producers) == 0 || len(producers) == len(findNSQds(producers, "LOW")) {
		//there is no high prio nsqds having experience serving this topic
		//just delete the registration, as the new nsqd will register it
		reg := Registration{"topic", topicName, ""}
		s.ctx.nsqlookupd.DB.RemoveRegistration(reg)
	} else {
		for _, p := range producers {
			if p.peerInfo.DaemonPriority == "HIGH" {
				previousReg := Registration{"topic", topicName, "#inactive"}
				currentReg := Registration{"topic", topicName, ""}
				s.ctx.nsqlookupd.DB.RemoveProducer(previousReg, p.peerInfo.id)
				s.ctx.nsqlookupd.DB.AddProducer(currentReg, p)
			}
		}
	}
	return map[string]interface{}{
		"status": "success",
	}, nil
}

//yao
//unregister topic priority
//unregister_topic_priority topicName
func (s *httpServer) doUnregisterTopicPriority(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {

	reqParams, err := http_api.NewReqParams(req)
	if err != nil {
		return nil, http_api.Err{400, "INVALID_REQUEST"}
	}

	topicName, err := http_api.GetTopicPriorityArgs(reqParams)
	if err != nil {
		return nil, http_api.Err{400, err.Error()}
	}

	s.ctx.nsqlookupd.logf("DB: deleting topic (%s) from high priority", topicName)

	//if there is no such topic in the high prio list, return
	if len(s.ctx.nsqlookupd.DB.FindRegistrations("high_priority_topic", topicName, "")) == 0 {
		return map[string]interface{}{
			"status": "success",
		}, nil
	}
	//else, remove it
	key := Registration{"high_priority_topic", topicName, ""}
	s.ctx.nsqlookupd.DB.RemoveRegistration(key)

	//we need to check if the topic is currently served by any other daemons
	//if there is a daemon and the daemon is high priority, we need to modify this registration so that next time
	//the topic can be served by high priority nsqds
	//set the #inactive flag to make the currently active producer inactive
	producers := s.ctx.nsqlookupd.DB.FindProducers("topic", topicName, "")
	producers = producers.FilterByActive(s.ctx.nsqlookupd.opts.InactiveProducerTimeout, s.ctx.nsqlookupd.opts.TombstoneLifetime)
	for _, p := range producers {
		if p.peerInfo.DaemonPriority == "HIGH" {
			previousReg := Registration{"topic", topicName, ""}
			currentReg := Registration{"topic", topicName, "#inactive"}
			s.ctx.nsqlookupd.DB.RemoveProducer(previousReg, p.peerInfo.id)
			s.ctx.nsqlookupd.DB.AddProducer(currentReg, p)
		}
	}
	//find an inactive correpsonding low priority nsqd
	//switch its flag and set to active
	producers = s.ctx.nsqlookupd.DB.FindProducers("topic", topicName, "#inactive")
	producers = producers.FilterByActive(s.ctx.nsqlookupd.opts.InactiveProducerTimeout, s.ctx.nsqlookupd.opts.TombstoneLifetime)
	if len(producers) == 0 || len(producers) == len(findNSQds(producers, "HIGH")) {
		//there is no low prio nsqds having experience serving this topic
		//just delete the registration, as the new nsqd will register it
		reg := Registration{"topic", topicName, ""}
		s.ctx.nsqlookupd.DB.RemoveRegistration(reg)
	} else {
		for _, p := range producers {
			if p.peerInfo.DaemonPriority == "LOW" {
				previousReg := Registration{"topic", topicName, "#inactive"}
				currentReg := Registration{"topic", topicName, ""}
				s.ctx.nsqlookupd.DB.RemoveProducer(previousReg, p.peerInfo.id)
				s.ctx.nsqlookupd.DB.AddProducer(currentReg, p)
			}
		}
	}

	return map[string]interface{}{
		"status": "success",
	}, nil
}

//yao
//find certain daemon_priorioty nsqds
//havent finished this part
func (s *httpServer) doPriorityNodes(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	return nil, nil
}

func findNSQds(producers Producers, priority string) Producers {
	var outputProducers Producers
	for _, p := range producers {
		if p.peerInfo.DaemonPriority == priority {
			outputProducers = append(outputProducers, p)
		}
	}
	return outputProducers
}

func (s *httpServer) risk_ub(Var float64) float64 {
	overT := 0.9
	prob := 0.2
	return overT - math.Sqrt((1-prob)*Var/prob)
}
func (s *httpServer) rcap(Var float64, Mean float64) float64 {
        overT := 0.9
        prob := 0.2
        return overT - math.Sqrt((1-prob)*Var/prob)- Mean
}
//yao
//find nsqd address based on the priority specifies
//return a nsqd with appropriate nsqd address
//but might return a random nsqd address if a speicifc kind of nsqd doesnt exist
//eg: ask for high prio, but there is no high prio nsqd. In that case, return addresses of low prio daemons
//Moreover, return all the addresses of specific kind of daemons and let producer decide by itself
func (s *httpServer) doProducerLookupV2(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {

	// dont filter out tombstoned nodes
	producers := s.ctx.nsqlookupd.DB.FindProducers("client", "", "").FilterByActive(
		s.ctx.nsqlookupd.opts.InactiveProducerTimeout, 0)
	//fmt.Printf("number of producers %d\n", len(producers))
	reqParams, err := http_api.NewReqParams(req)
	if err != nil {
		return nil, http_api.Err{400, "INVALID_REQUEST"}
	}

	//RTM.dtb: use priority parameter to report total number of publishers
	infoStr, topic, pubID, err := http_api.Getv3Args(reqParams)
	priorityLevel := "HIGH"
	if err != nil {
		return nil, http_api.Err{400, err.Error()}
	}

	numNSQds := 0
	tolerantFlag := false
	for _, q := range producers {
		if q.peerInfo.DaemonPriority == priorityLevel {
			numNSQds++
		}
	}
	if numNSQds == 0 {
		tolerantFlag = true
	}
	//fmt.Printf("number of correct producers %d\n", numNSQds)

	var nodes []*node

	//RTM
	var candidates []*Producer
	var re *Producer
	min := float64(1000000000000)
	index := 0
	load := float64(0)
	for i, p := range producers {
		if s.ctx.nsqlookupd.DB.initBroker == nil && p.peerInfo.DaemonPriority=="LOW" {
			s.ctx.nsqlookupd.DB.initBroker = p
		}
		if p.peerInfo.DaemonPriority != priorityLevel && !tolerantFlag {
			continue
		}
		candidates = append(candidates, p)
		load = float64(p.peerInfo.FConnCount)
		//load = float64(p.peerInfo.CPU)
		if load < min {
			min = load
			index = i
		}
	}
	re = producers[index]
	

	var ranges []float64
	sum := float64(0)
	total := float64(0)
	index = -1
	var ID int

	if strings.HasPrefix(topic, "f") {
		index, _ = strconv.Atoi(strings.TrimSuffix(topic[3:], "n#ephemeral"))
		fmt.Printf("fake to ~~~~~~~~~~%v\n", index)
		re = candidates[index%len(candidates)]
	} else if strings.HasPrefix(topic, "p") {
		params := strings.Split(infoStr, "x")
        	pubNum,_ := strconv.Atoi(params[0])
        	rate,_ := strconv.Atoi(params[1])
        	rate = (rate*10)/11
        	burst,_:= strconv.Atoi(params[2])
        	key,_:= strconv.Atoi(params[3])
		fmt.Printf("%v %v %v %v %v %v\n", infoStr, pubNum, rate, burst, key)
		realTopic:= topic[:]
		if _, ok := s.ctx.nsqlookupd.DB.brokerList[realTopic]; !ok {
                	s.ctx.nsqlookupd.DB.TPName=realTopic
			s.ctx.nsqlookupd.DB.TPRate=rate
			s.ctx.nsqlookupd.DB.TPBurst=burst
			s.ctx.nsqlookupd.DB.latency[realTopic]=[]int64{}
			s.ctx.nsqlookupd.DB.topicCount[realTopic]=pubNum
			s.ctx.nsqlookupd.DB.topicCount2[realTopic]=0
			s.ctx.nsqlookupd.DB.groups = make(map[int]*CorGroup)
			s.ctx.nsqlookupd.DB.brokerList[realTopic]=make([]*Producer, len(candidates))
			s.ctx.nsqlookupd.DB.initBroker.peerInfo.pubs = []int{}
			for i, p := range candidates {
				p.peerInfo.pubs=[]int{}
			 	s.ctx.nsqlookupd.DB.brokerList[realTopic][i] = p
				/*p.peerInfo.totalRate = 0
				for _, r:= range p.peerInfo.rates {
					p.peerInfo.totalRate += r.rate
				}*/
				p.peerInfo.rcap = 110000 - p.peerInfo.totalRate
				p.peerInfo.lastLatency = 100000000000
				p.peerInfo.inProfile = 0
				fmt.Printf("%v total rate =%v, rcap=%v\n", i, p.peerInfo.totalRate, p.peerInfo.rcap)
			}
			//RTM.dtb: uncomment for multi-topic scenario
			//sort.Slice(s.ctx.nsqlookupd.DB.brokerList[realTopic], func(i, j int) bool {return s.ctx.nsqlookupd.DB.brokerList[realTopic][i].peerInfo.totalRate < s.ctx.nsqlookupd.DB.brokerList[realTopic][j].peerInfo.totalRate})
			//s.ctx.nsqlookupd.DB.curBIndex = 0
			s.ctx.nsqlookupd.DB.mmDist=make(map[int]*Producer)
			s.ctx.nsqlookupd.DB.rates=make(map[int]int)
		}
		//re = s.ctx.nsqlookupd.DB.brokerList[realTopic][s.ctx.nsqlookupd.DB.curBIndex]
		re = s.ctx.nsqlookupd.DB.initBroker
		ID, _ = strconv.Atoi(pubID)
		re.peerInfo.pubs=append(re.peerInfo.pubs, ID)
		s.ctx.nsqlookupd.DB.rates[ID]=rate/pubNum
		if g, ok := s.ctx.nsqlookupd.DB.groups[key]; ok {
			g.num++
			g.pubs = append(g.pubs, ID)
		} else {
			s.ctx.nsqlookupd.DB.groups[key]=&CorGroup{
				num: 1,
				index: 0,
				tmp_index: 0,
				pubs: []int{ID},
				assigns: []int{},
			}
		}
		if len(re.peerInfo.pubs) == pubNum {
			random := rand.New(rand.NewSource(time.Now().UnixNano()))
			for _, g := range s.ctx.nsqlookupd.DB.groups {
                        	swap := make([]int, g.num)
				for i, _ := range swap {
					swap[i]=-1
				}
                        	for _, id := range g.pubs {
					index:=random.Intn(g.num)
					if swap[index] > -1 {
						for newindex:=(index+1)%g.num; newindex!=index; newindex=(newindex+1)%g.num {
							if swap[newindex] == -1 {
								swap[newindex]=id
								break
							}
						}
					} else {
						swap[index]=id
					}
                        	}
				//copy(g.pubs, swap)
				for i, _:= range g.pubs {
					g.pubs[i]=swap[i]
				}
                	}
			go func(){
				fmt.Printf("Arrive %v %v\n", len(re.peerInfo.pubs), pubNum)
				s.ctx.nsqlookupd.DB.profileTopic <- pubNum
			}()
		} 
	} else if strings.HasPrefix(topic, "0") || strings.HasPrefix(topic, "x") || strings.HasPrefix(topic, "e") || strings.HasPrefix(topic, "tb"){
		bNum :=len(candidates)
		bIn  := 0
		if strings.HasPrefix(topic, "0") || strings.HasPrefix(topic, "x") {
			bNum, _ = strconv.Atoi(strings.TrimSuffix(topic[3:], "n#ephemeral"))
		}
		if strings.HasPrefix(topic, "e") {
			bIn, _ = strconv.Atoi(strings.TrimSuffix(topic[4:], "n#ephemeral"))
		}
		if s.ctx.nsqlookupd.DB.topicCands[topic]==nil{
			params := strings.Split(infoStr, "x")
                	pubNum,_ := strconv.Atoi(params[0])
			
			//if strings.HasPrefix(topic, "tb") {
				rate,_ := strconv.Atoi(params[1])
                		burst,_:= strconv.Atoi(params[2])
				s.ctx.nsqlookupd.DB.TPRate=rate
                        	s.ctx.nsqlookupd.DB.TPBurst=burst
				s.ctx.nsqlookupd.DB.perRate=((rate*10)/11)/pubNum
			//}

                        s.ctx.nsqlookupd.DB.topicCount[topic]=pubNum
                        s.ctx.nsqlookupd.DB.topicCount2[topic]=0

			s.ctx.nsqlookupd.DB.topicCands[topic]=make([]*Producer, 0, len(candidates))
			s.ctx.nsqlookupd.DB.topicDist[topic]=make([]*Producer, 0, len(candidates))
			for _, p := range candidates {
				/*p.peerInfo.totalRate = 0
                                for _, r:= range p.peerInfo.rates {
                                        p.peerInfo.totalRate += r.rate
                                }*/
				fmt.Printf("%v\n", p.peerInfo.totalRate)
                                p.peerInfo.rcap = 110000 - p.peerInfo.totalRate
				p.peerInfo.balRate = p.peerInfo.totalRate
				if p.peerInfo.totalRate < 1000 { //eliminate noise
					p.peerInfo.rcap = 110000
					p.peerInfo.balRate = 0
				}
				s.ctx.nsqlookupd.DB.topicCands[topic] = append(s.ctx.nsqlookupd.DB.topicCands[topic], p)
				s.ctx.nsqlookupd.DB.topicDist[topic] = append(s.ctx.nsqlookupd.DB.topicDist[topic], p)
			}
			//RTM.dtb: uncomment for multi-topic scenarios
			//sort.Slice(s.ctx.nsqlookupd.DB.topicCands[topic], func(i, j int) bool {return s.ctx.nsqlookupd.DB.topicCands[topic][i].peerInfo.totalRate < s.ctx.nsqlookupd.DB.topicCands[topic][j].peerInfo.totalRate})
			if strings.HasPrefix(topic, "x") {
				thresh := 0
				for i:=0; i<bNum; i++ {
					thresh += s.ctx.nsqlookupd.DB.topicCands[topic][i].peerInfo.balRate
				}
				thresh += (rate*10)/11
				thresh /= bNum
				for i:=0; i<bNum; i++ {
					pp := s.ctx.nsqlookupd.DB.topicCands[topic][i]
					pp.peerInfo.rcap = thresh - pp.peerInfo.balRate
					pp.peerInfo.balRate = 0
				}
			} else if strings.HasPrefix(topic, "0") {
				rate_add := (rate*10)/11
				for i:=bNum-1; i>=0; i-- {
					pp := s.ctx.nsqlookupd.DB.topicCands[topic][i]
					if rate_add/(i+1) < pp.peerInfo.rcap {
						pp.peerInfo.rcap = rate_add/(i+1)
					}
					rate_add -= pp.peerInfo.rcap
					pp.peerInfo.balRate = 0
				}
			}
		}
		if strings.HasPrefix(topic, "e") {
			re = s.ctx.nsqlookupd.DB.topicDist[topic][bIn]
			goto after
		}
		for i, p := range s.ctx.nsqlookupd.DB.topicCands[topic] {
			if i==bNum {
				break
			}
			Cap := float64(p.peerInfo.rcap)
			//RTM.dtb: if brokerA.CPU=0.001 while brokerB.CPU=0.01, it's unreasonable
			//to give brokerA a weight 10 times larger than brokerB.
			/*if CPU < float64(0.9) {
				CPU = float64(0.1)
			}*/
			sum += Cap
			ranges = append(ranges, Cap)
		}
		total = float64(0)
		for i, r := range ranges {
			ranges[i] = r/sum
			total += ranges[i]
		}
		//random := rand.New(rand.NewSource(time.Now().UnixNano()))
		//x := random.Float64()
		x := s.ctx.nsqlookupd.DB.random.Float64()
		end := float64(0)
		for i, r := range ranges {
			end += r
			if x < end {
				index = i
				break
			}
		}
		re = s.ctx.nsqlookupd.DB.topicCands[topic][index]
		ID, _=strconv.Atoi(pubID)
		
		//params := strings.Split(infoStr, "x")
		//key,_:= strconv.Atoi(params[3])
		//if key >= 0 {
		//correlation-aware
			//index = ID%bNum
			//re=s.ctx.nsqlookupd.DB.topicCands[topic][ID%bNum]
		//}
		
		if re.peerInfo.balRate >= re.peerInfo.rcap {
			newIndex := (index+1)%bNum
			for ; newIndex != index; newIndex=(newIndex+1)%bNum {
				pp:= s.ctx.nsqlookupd.DB.topicCands[topic][newIndex]
				if pp.peerInfo.balRate < pp.peerInfo.rcap {
					re = pp
					break
				}
			}
		}
		re.peerInfo.balRate += s.ctx.nsqlookupd.DB.perRate
		re.peerInfo.FConnCount += 1
        	if ID == s.ctx.nsqlookupd.DB.topicCount[topic]-1 {
                	for _, broker := range s.ctx.nsqlookupd.DB.topicCands[topic] {
                        	fmt.Printf("~~~~~~~!!!!!!!! %v %v\n", broker.peerInfo.FConnCount, broker.peerInfo.rcap)
                        	broker.peerInfo.FConnCount = 0
                	}
        	}

		//ID, _=strconv.Atoi(pubID)
		//re = candidates[ID%bNum]//RR
		//re = candidates[ID/(2000/len(candidates))]//warst-case
	} else {
		ID, _ = strconv.Atoi(pubID)
		re = s.ctx.nsqlookupd.DB.topicDist[topic][ID]
	}
after:
	p := re
	fmt.Printf("%s %s %d %d %d %d %d %d\n", p.peerInfo.RemoteAddress, p.peerInfo.DaemonPriority, p.peerInfo.FConnCount,
		p.peerInfo.TopicCount, p.peerInfo.ConnCount, p.peerInfo.ChannelCount, p.peerInfo.QLen, p.peerInfo.CPU)
	/*Topic := topic
	TCtotal := s.ctx.nsqlookupd.DB.TCtotal
	//p.peerInfo.Lock()
	if strings.HasPrefix(Topic, "p") {
		Topic = topic[1:]
		if _, ok := s.ctx.nsqlookupd.DB.topicCount[Topic]; !ok {
			s.ctx.nsqlookupd.DB.topicCount[Topic] = 0
			for _, x := range producers {
				x.peerInfo.RLock()
				x.peerInfo.oldRisk = x.peerInfo.risk
				x.peerInfo.oldCPU = x.peerInfo.CPU
				x.peerInfo.oldVar = x.peerInfo.CPUVar
				x.peerInfo.RUnlock()
			}
		}
		s.ctx.nsqlookupd.DB.topicCount[Topic] += 1
		p.peerInfo.bursts[Topic]+=1.0
		if s.ctx.nsqlookupd.DB.topicCount[Topic] == TCtotal {
			cSum:=float64(0)
			for _, px:= range candidates {
				cSum += px.peerInfo.bursts[Topic]
			}
			fmt.Printf("~~~~~~~~~~~~~~DTB: ")
			for _, px:= range candidates {
				px.peerInfo.bursts[Topic]=px.peerInfo.bursts[Topic]/cSum
				fmt.Printf("%v ", px.peerInfo.bursts[Topic])
			}
		}
	} else {
		if _, ok := s.ctx.nsqlookupd.DB.topicCount[Topic]; !ok {
			s.ctx.nsqlookupd.DB.topicCount[Topic] = 0
		}
		s.ctx.nsqlookupd.DB.topicCount[Topic] += 1
		if s.ctx.nsqlookupd.DB.topicCount[Topic] == TCtotal+1 {
			s.ctx.nsqlookupd.DB.topicCount[Topic] = 1
			cNum, _ := strconv.Atoi(strings.TrimSuffix(Topic[3:], "n#ephemeral"))
                        for i:=0; i<cNum; i++ {
                                candidates[i].peerInfo.bursts[Topic]=0.0
                        }
			fmt.Printf("~~~~~~~~~~~~~~~~~~~ DTB reset\n")
		}
		p.peerInfo.bursts[Topic]+=1.0
		if s.ctx.nsqlookupd.DB.topicCount[Topic] == TCtotal {
			cNum, _ := strconv.Atoi(strings.TrimSuffix(Topic[3:], "n#ephemeral"))	
			cSum:=float64(0)
			for i:=0; i<cNum; i++ {
				cSum += candidates[i].peerInfo.bursts[Topic]
			}
			fmt.Printf("~~~~~~~~~~~~~~DTB: ")
			for i:=0; i<cNum; i++ {
				candidates[i].peerInfo.bursts[Topic]=candidates[i].peerInfo.bursts[Topic]/cSum
				fmt.Printf("~~~~~~~~~~~~ %v \n", candidates[i].peerInfo.bursts[Topic])
			}
		}
	}*/
	//p.peerInfo.Unlock()
	topics := s.ctx.nsqlookupd.DB.LookupRegistrations(p.peerInfo.id).Filter("topic", "*", "").Keys()
	// for each topic find the producer that matches this peer
	// to add tombstone information
	tombstones := make([]bool, len(topics))
	for j, t := range topics {
		topicProducers := s.ctx.nsqlookupd.DB.FindProducers("topic", t, "")
		for _, tp := range topicProducers {
			if tp.peerInfo == p.peerInfo {
				tombstones[j] = tp.IsTombstoned(s.ctx.nsqlookupd.opts.TombstoneLifetime)
			}
		}
	}

	nodes = append(nodes, &node{
		RemoteAddress:    p.peerInfo.RemoteAddress,
		Hostname:         p.peerInfo.Hostname,
		BroadcastAddress: p.peerInfo.BroadcastAddress,
		TCPPort:          p.peerInfo.TCPPort,
		HTTPPort:         p.peerInfo.HTTPPort,
		Version:          p.peerInfo.Version,
		Tombstones:       tombstones,
		Topics:           topics,
	})

	return map[string]interface{}{
		"producers": nodes,
	}, nil
}

//RTM
//find nsqd address based on the priority and topic specifies
//return a nsqd with appropriate nsqd address
func (s *httpServer) doProducerLookupV4(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {

	// dont filter out tombstoned nodes
	producers := s.ctx.nsqlookupd.DB.FindProducers("client", "", "").FilterByActive(
		s.ctx.nsqlookupd.opts.InactiveProducerTimeout, 0)

	reqParams, err := http_api.NewReqParams(req)
	if err != nil {
		return nil, http_api.Err{400, "INVALID_REQUEST"}
	}
	priorityLevel, topic, err := http_api.Getv2Args(reqParams)
	if err != nil {
		return nil, http_api.Err{400, err.Error()}
	}
	var nodes []*node

	var candidates []*Producer
	subProducers := s.ctx.nsqlookupd.DB.FindProducers("topic", topic, "")
	min := float64(1000000000000)
	index := -1
	load := float64(0)
	for i, p := range subProducers {
		//if p.peerInfo.CPU >= float64(0.3) {
		//continue
		//}
		load = p.peerInfo.CPU
		if load < min {
			min = load
			index = i
		}
		candidates = append(candidates, p)
	}
	if min < float64(0.2) {
		goto wrand
	}
	if len(candidates) > 0 {
		numNSQds := 0
		tolerantFlag := false
		for _, q := range producers {
			if q.peerInfo.DaemonPriority == priorityLevel {
				numNSQds++
			}
		}
		if numNSQds == 0 {
			tolerantFlag = true
		}

		for _, p := range producers {
			if p.peerInfo.DaemonPriority != priorityLevel && !tolerantFlag {
				continue
			}
			if p.peerInfo.CPU < min-float64(0.3) {
				candidates = append(candidates, p)
			}
		}
	}
	if len(candidates) == 0 {
		numNSQds := 0
		tolerantFlag := false
		for _, q := range producers {
			if q.peerInfo.DaemonPriority == priorityLevel {
				numNSQds++
			}
		}
		if numNSQds == 0 {
			tolerantFlag = true
		}

		//RTM.dtb test only
		//min := float64(1000000000000)
		//index = -1
		//load := float64(0)
		for _, p := range producers {
			if p.peerInfo.DaemonPriority != priorityLevel && !tolerantFlag {
				continue
			}
			candidates = append(candidates, p)
			/*load = float64(p.peerInfo.FConnCount)
			if load < min {
				min = load
				index = i
			}*/
		}
	}

wrand:
	//RTM.dtb test only
	//if index < 0 {
	var ranges []float64
	sum := float64(0)
	index = -1
	for i, p := range candidates {
		if p.peerInfo.CPU == float64(0) {
			index = i
			break
		}
		sum += p.peerInfo.CPU
		ranges = append(ranges, p.peerInfo.CPU)
	}
	if index < 0 {
		total := float64(0)
		for i, r := range ranges {
			ranges[i] = sum / r
			total += ranges[i]
		}
		random := rand.New(rand.NewSource(time.Now().UnixNano()))
		x := random.Float64() * total
		end := float64(0)
		for i, r := range ranges {
			end += r
			if x < end {
				index = i
				break
			}
		}
	}
	//}

	p := candidates[index]

	fmt.Printf("v3 %s %s %d %d %d %d %f\n", p.peerInfo.RemoteAddress, p.peerInfo.DaemonPriority,
		p.peerInfo.TopicCount, p.peerInfo.ConnCount, p.peerInfo.ChannelCount, p.peerInfo.QLen, p.peerInfo.CPU)
	p.peerInfo.FConnCount += 1

	//RTM, fast update table
	key := Registration{"topic", topic, ""}

	if s.ctx.nsqlookupd.DB.AddProducer(key, &Producer{peerInfo: p.peerInfo}) {
		s.ctx.nsqlookupd.logf("DB: REGISTER category:%s key:%s subkey:%s",
			"topic", topic, "")
		p.peerInfo.TopicCount += 1
		p.peerInfo.rates[topic] = &RateUpdate{
			rate:     0,
			updated:  false,
			newrate:  0,
			sendback: false,
		}
	}

	topics := s.ctx.nsqlookupd.DB.LookupRegistrations(p.peerInfo.id).Filter("topic", "*", "").Keys()
	// for each topic find the producer that matches this peer
	// to add tombstone information
	tombstones := make([]bool, len(topics))
	for j, t := range topics {
		topicProducers := s.ctx.nsqlookupd.DB.FindProducers("topic", t, "")
		for _, tp := range topicProducers {
			if tp.peerInfo == p.peerInfo {
				tombstones[j] = tp.IsTombstoned(s.ctx.nsqlookupd.opts.TombstoneLifetime)
			}
		}
	}

	nodes = append(nodes, &node{
		RemoteAddress:    p.peerInfo.RemoteAddress,
		Hostname:         p.peerInfo.Hostname,
		BroadcastAddress: p.peerInfo.BroadcastAddress,
		TCPPort:          p.peerInfo.TCPPort,
		HTTPPort:         p.peerInfo.HTTPPort,
		Version:          p.peerInfo.Version,
		Tombstones:       tombstones,
		Topics:           topics,
	})

	return map[string]interface{}{
		"producers": nodes,
	}, nil
}

//RTM
func (s *httpServer) CPU_Risk(p *Producer) float64 {
	risk_threshold := 0.6
	//return 1 - (p.peerInfo.CPUVar+math.Pow((p.peerInfo.CPU-0.5*(risk_threshold+1.0)), 2))/math.Pow((0.5*(1.0-risk_threshold)), 2)
	return p.peerInfo.CPUVar / (p.peerInfo.CPUVar + math.Pow((risk_threshold-p.peerInfo.CPU), 2))
}

//RTM
func (s *httpServer) CPU_Risk_Test(p *Producer) bool {
	risk_threshold := 0.6

	return p.peerInfo.risk < risk_threshold
}

func (s *httpServer) conn_Cap(p *Producer) int {
	risk_threshold := 0.6
	return int(risk_threshold / (p.peerInfo.CPU / float64(p.peerInfo.PrevConnCount)))
}

type Candy []*Producer

func (s Candy) Len() int {
	return len(s)
}
func (s Candy) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}
func (s Candy) Less(i, j int) bool {
	if s[i].peerInfo.CPU < float64(0.1) && s[j].peerInfo.CPU < float64(0.1) {
		return false
	}
	return s[i].peerInfo.CPU < s[j].peerInfo.CPU
}

//RTM
//find nsqd address based on the priority and topic specifies
//return a nsqd with appropriate nsqd address
/*func (s *httpServer) doProducerLookupV3(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {

	// dont filter out tombstoned nodes
	producers := s.ctx.nsqlookupd.DB.FindProducers("client", "", "").FilterByActive(
		s.ctx.nsqlookupd.opts.InactiveProducerTimeout, 0)

	reqParams, err := http_api.NewReqParams(req)
	if err != nil {
		return nil, http_api.Err{400, "INVALID_REQUEST"}
	}
	priorityLevel, topic, err := http_api.Getv2Args(reqParams)
	if err != nil {
		return nil, http_api.Err{400, err.Error()}
	}
	var nodes []*node

	//CPU_risk_probability := 0.3
	var candidates []*Producer
	subProducers := s.ctx.nsqlookupd.DB.FindProducers("topic", topic, "")

	index := -1

	for _, p := range subProducers {
		if s.CPU_Risk_Test(p) {
			candidates = append(candidates, p)
		}
	}
	if len(candidates) == 0 {
		numNSQds := 0
		tolerantFlag := false
		for _, q := range producers {
			if q.peerInfo.DaemonPriority == priorityLevel {
				numNSQds++
			}
		}
		if numNSQds == 0 {
			tolerantFlag = true
		}

		for _, p := range producers {
			if p.peerInfo.DaemonPriority != priorityLevel && !tolerantFlag {
				continue
			}
			if s.CPU_Risk_Test(p) {
				candidates = append(candidates, p)
			}
		}
		if len(candidates) == 0 {
			for _, q := range producers {
				if q.peerInfo.DaemonPriority == priorityLevel {
					candidates = append(candidates, q)
				}
			}
		}
	}

	sort.Sort(Candy(candidates))
	for i, p := range candidates {
		if p.peerInfo.FConnCount < s.conn_Cap(p) {
			index = i
			break
		}
	}
	if index == -1 {
		var ranges []float64
		sum := float64(0)
		for i, p := range candidates {
			if p.peerInfo.CPU == float64(0) {
				index = i
				break
			}
			sum += p.peerInfo.CPU
			ranges = append(ranges, p.peerInfo.CPU)
		}
		if index < 0 {
			total := float64(0)
			for i, r := range ranges {
				ranges[i] = sum / r
				total += ranges[i]
			}
			random := rand.New(rand.NewSource(time.Now().UnixNano()))
			x := random.Float64() * total
			end := float64(0)
			for i, r := range ranges {
				end += r
				if x < end {
					index = i
					break
				}
			}
		}
	}
	p := candidates[index]

	fmt.Printf("v3 %s %s %d %d %d %d %f %f %f %d\n", p.peerInfo.RemoteAddress, p.peerInfo.DaemonPriority,
		p.peerInfo.TopicCount, p.peerInfo.ConnCount, p.peerInfo.ChannelCount, p.peerInfo.QLen, p.peerInfo.CPU, p.peerInfo.CPUVar, s.CPU_Risk(p), s.conn_Cap(p))
	p.peerInfo.FConnCount += 1

	//RTM, fast update table
	key := Registration{"topic", topic, ""}

	if s.ctx.nsqlookupd.DB.AddProducer(key, &Producer{peerInfo: p.peerInfo}) {
		s.ctx.nsqlookupd.logf("DB: REGISTER category:%s key:%s subkey:%s",
			"topic", topic, "")
		p.peerInfo.TopicCount += 1
		p.peerInfo.rates[topic] = &RateUpdate{
			rate:     0,
			updated:  false,
			newrate:  0,
			sendback: false,
		}
	}

	topics := s.ctx.nsqlookupd.DB.LookupRegistrations(p.peerInfo.id).Filter("topic", "*", "").Keys()
	// for each topic find the producer that matches this peer
	// to add tombstone information
	tombstones := make([]bool, len(topics))
	for j, t := range topics {
		topicProducers := s.ctx.nsqlookupd.DB.FindProducers("topic", t, "")
		for _, tp := range topicProducers {
			if tp.peerInfo == p.peerInfo {
				tombstones[j] = tp.IsTombstoned(s.ctx.nsqlookupd.opts.TombstoneLifetime)
			}
		}
	}

	nodes = append(nodes, &node{
		RemoteAddress:    p.peerInfo.RemoteAddress,
		Hostname:         p.peerInfo.Hostname,
		BroadcastAddress: p.peerInfo.BroadcastAddress,
		TCPPort:          p.peerInfo.TCPPort,
		HTTPPort:         p.peerInfo.HTTPPort,
		Version:          p.peerInfo.Version,
		Tombstones:       tombstones,
		Topics:           topics,
	})

	return map[string]interface{}{
		"producers": nodes,
	}, nil
}*/

func (s *httpServer) doProducerLookupV3(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	reqParams, err := http_api.NewReqParams(req)
	if err != nil {
		return nil, http_api.Err{400, "INVALID_REQUEST"}
	}
	_, topic, pubID, err := http_api.Getv3Args(reqParams)
	if err != nil {
		return nil, http_api.Err{400, err.Error()}
	}
	var nodes []*node

	ID, _ := strconv.Atoi(pubID)
	p := s.ctx.nsqlookupd.DB.topicDist[topic][ID]

	fmt.Printf("v3 %s %s %d %d %d %d %f %f %f %d\n", p.peerInfo.RemoteAddress, p.peerInfo.DaemonPriority,
		p.peerInfo.TopicCount, p.peerInfo.ConnCount, p.peerInfo.ChannelCount, p.peerInfo.QLen, p.peerInfo.CPU, p.peerInfo.CPUVar, s.CPU_Risk(p), s.conn_Cap(p))
	p.peerInfo.FConnCount += 1

	//RTM, fast update table
	key := Registration{"topic", topic, ""}

	if s.ctx.nsqlookupd.DB.AddProducer(key, &Producer{peerInfo: p.peerInfo}) {
		s.ctx.nsqlookupd.logf("DB: REGISTER category:%s key:%s subkey:%s",
			"topic", topic, "")
		p.peerInfo.TopicCount += 1
		p.peerInfo.rates[topic] = &RateUpdate{
			rate:     0,
			updated:  false,
			newrate:  0,
			sendback: false,
		}
	}

	topics := s.ctx.nsqlookupd.DB.LookupRegistrations(p.peerInfo.id).Filter("topic", "*", "").Keys()
	// for each topic find the producer that matches this peer
	// to add tombstone information
	tombstones := make([]bool, len(topics))
	for j, t := range topics {
		topicProducers := s.ctx.nsqlookupd.DB.FindProducers("topic", t, "")
		for _, tp := range topicProducers {
			if tp.peerInfo == p.peerInfo {
				tombstones[j] = tp.IsTombstoned(s.ctx.nsqlookupd.opts.TombstoneLifetime)
			}
		}
	}

	nodes = append(nodes, &node{
		RemoteAddress:    p.peerInfo.RemoteAddress,
		Hostname:         p.peerInfo.Hostname,
		BroadcastAddress: p.peerInfo.BroadcastAddress,
		TCPPort:          p.peerInfo.TCPPort,
		HTTPPort:         p.peerInfo.HTTPPort,
		Version:          p.peerInfo.Version,
		Tombstones:       tombstones,
		Topics:           topics,
	})

	return map[string]interface{}{
		"producers": nodes,
	}, nil
}