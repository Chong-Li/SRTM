package http_api

import (
	"errors"

	"github.com/WU-CPSL/RTM-0.1/nsqio/nsq/internal/protocol"
)

type getter interface {
	Get(key string) (string, error)
}

func GetTopicChannelArgs(rp getter) (string, string, error) {
	topicName, err := rp.Get("topic")
	if err != nil {
		return "", "", errors.New("MISSING_ARG_TOPIC")
	}

	if !protocol.IsValidTopicName(topicName) {
		return "", "", errors.New("INVALID_ARG_TOPIC")
	}

	channelName, err := rp.Get("channel")
	if err != nil {
		return "", "", errors.New("MISSING_ARG_CHANNEL")
	}

	if !protocol.IsValidChannelName(channelName) {
		return "", "", errors.New("INVALID_ARG_CHANNEL")
	}

	return topicName, channelName, nil
}

//yao
//get topic priority parameters
func GetTopicPriorityArgs(rp getter) (string, error) {
	topicName, err := rp.Get("topic")
	if err != nil {
		return "", errors.New("MISSING_ARG_TOPIC")
	}

	if !protocol.IsValidTopicName(topicName) {
		return "", errors.New("INVALID_ARG_TOPIC")
	}

	return topicName, nil
}

//yao
//get priority parameters
func GetPriorityArgs(rp getter) (string, error) {
	priorityLevel, err := rp.Get("priority")
	if err != nil {
		return "", errors.New("MISSING_ARG_TOPIC")
	}

	return priorityLevel, nil
}

//RTM
//get priority and topic
func Getv2Args(rp getter) (string, string, error) {
	priorityLevel, err := rp.Get("priority")
	if err != nil {
		return "", "", errors.New("MISSING_ARG_PRIO")
	}
	topic, err := rp.Get("topic")
	if err != nil {
		return "", "", errors.New("MISSING_ARG_TOPIC")
	}

	return priorityLevel, topic, nil
}

//RTM
//get priority and topic and publisherID
func Getv3Args(rp getter) (string, string, string, error) {
	priorityLevel, err := rp.Get("priority")
	if err != nil {
		return "", "", "", errors.New("MISSING_ARG_PRIO")
	}
	topic, err := rp.Get("topic")
	if err != nil {
		return "", "", "", errors.New("MISSING_ARG_TOPIC")
	}
	pubID, err := rp.Get("pub")
	if err != nil {
		return "", "", "", errors.New("MISSING_ARG_PUB")
	}

	return priorityLevel, topic, pubID, nil
}
