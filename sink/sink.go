package sink

import (
	"context"
	"strconv"

	"github.com/quintans/eventstore"
)

type Message struct {
	ResumeToken string
	Event       eventstore.Event
}

type Sink interface {
	LastMessage(ctx context.Context, partition int) (*Message, error)
	Send(ctx context.Context, e eventstore.Event) error
	Close()
}

func TopicWithPartition(topic string, partition int) string {
	if partition == 0 {
		return topic
	}
	return topic + "." + strconv.Itoa(partition)
}
