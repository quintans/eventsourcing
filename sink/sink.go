package sink

import (
	"context"
	"hash/fnv"
	"strconv"

	"github.com/quintans/eventstore"
)

type Message struct {
	ResumeToken string
	Event       eventstore.Event
}

type Sinker interface {
	Sink(ctx context.Context, e eventstore.Event) error
	LastMessage(ctx context.Context, partition int) (*Message, error)
	Close()
}

func TopicWithPartition(topic string, partition int) string {
	if partition == 0 {
		return topic
	}
	return topic + "." + strconv.Itoa(partition)
}

func PartitionTopic(key, topic string, partitions uint32) string {
	if partitions != 0 {
		h := hash(key)
		m := h % partitions
		topic = TopicWithPartition(topic, int(m+1))
	}
	return topic
}

func hash(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}
