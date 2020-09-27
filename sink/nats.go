package sink

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/nats-io/stan.go"
	"github.com/quintans/eventstore"
)

type NatsSink struct {
	topic         string
	client        stan.Conn
	partitions    uint32
	stanClusterID string
	clientID      string
	options       []stan.Option
}

// NewNatsSink instantiate PulsarSink
func NewNatsSink(topic string, partitions uint32, stanClusterID, clientID string, options ...stan.Option) *NatsSink {
	return &NatsSink{
		topic:         topic,
		partitions:    partitions,
		stanClusterID: stanClusterID,
		clientID:      clientID,
		options:       options,
	}
}

// Init starts the feed
func (p *NatsSink) Init() error {
	c, err := stan.Connect(p.stanClusterID, p.clientID, p.options...)
	if err != nil {
		return fmt.Errorf("Could not instantiate Nats connection: %w", err)
	}
	p.client = c
	return nil
}

// Close releases resources blocking until
func (p *NatsSink) Close() {
	if p.client != nil {
		p.client.Close()
	}
}

// LastMessage gets the last message sent to NATS
func (p *NatsSink) LastMessage(ctx context.Context, partition int) (*Message, error) {
	type message struct {
		sequence uint64
		data     []byte
	}
	topic := TopicWithPartition(p.topic, partition)
	ch := make(chan message)
	sub, err := p.client.Subscribe(topic, func(m *stan.Msg) {
		ch <- message{
			sequence: m.Sequence,
			data:     m.Data,
		}
	}, stan.StartWithLastReceived())
	if err != nil {
		return nil, err
	}
	defer sub.Close()
	ctx, _ = context.WithTimeout(ctx, 100*time.Millisecond)
	var msg message
	select {
	case msg = <-ch:
	case <-ctx.Done():
		// no last message
		return nil, nil
	}
	event := eventstore.Event{}
	err = json.Unmarshal(msg.data, &event)
	if err != nil {
		return nil, err
	}

	return &Message{
		Event:       event,
		ResumeToken: strconv.FormatUint(msg.sequence, 10),
	}, nil
}

// Sink sends the event to pulsar
func (p *NatsSink) Sink(ctx context.Context, e eventstore.Event) error {
	b, err := json.Marshal(e)
	if err != nil {
		return nil
	}

	topic := PartitionTopic(e.AggregateID, p.topic, p.partitions)
	err = p.client.Publish(topic, b)
	if err != nil {
		return fmt.Errorf("Failed to send message: %w", err)
	}
	return nil
}
