package sink

import (
	"context"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/nats-io/stan.go"
	"github.com/quintans/eventstore"
	"github.com/quintans/eventstore/common"
	"github.com/quintans/faults"
	"github.com/sirupsen/logrus"
)

type NatsSink struct {
	topic      string
	client     stan.Conn
	partitions uint32
	codec      Codec
}

// NewNatsSink instantiate PulsarSink
func NewNatsSink(topic string, partitions uint32, stanClusterID, clientID string, options ...stan.Option) (*NatsSink, error) {
	p := &NatsSink{
		topic:      topic,
		partitions: partitions,
		codec:      JsonCodec{},
	}

	c, err := stan.Connect(stanClusterID, clientID, options...)
	if err != nil {
		return nil, faults.Errorf("Could not instantiate Nats connection: %w", err)
	}
	p.client = c
	return p, nil
}

func (p *NatsSink) SetCodec(codec Codec) {
	p.codec = codec
}

// Close releases resources blocking until
func (p *NatsSink) Close() {
	if p.client != nil {
		p.client.Close()
	}
}

// LastMessage gets the last message sent to NATS
func (p *NatsSink) LastMessage(ctx context.Context, partition uint32) (*eventstore.Event, error) {
	type message struct {
		sequence uint64
		data     []byte
	}
	topic := common.TopicWithPartition(p.topic, partition)
	ch := make(chan message)
	sub, err := p.client.Subscribe(topic, func(m *stan.Msg) {
		ch <- message{
			sequence: m.Sequence,
			data:     m.Data,
		}
	}, stan.StartWithLastReceived())
	if err != nil {
		return nil, faults.Wrap(err)
	}
	defer sub.Close()
	ctx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()

	var msg message
	select {
	case msg = <-ch:
	case <-ctx.Done():
		// no last message
		return nil, nil
	}
	event, err := p.codec.Decode(msg.data)
	if err != nil {
		return nil, err
	}

	return &event, nil
}

// Sink sends the event to pulsar
func (p *NatsSink) Sink(ctx context.Context, e eventstore.Event) error {
	b, err := p.codec.Encode(e)
	if err != nil {
		return err
	}

	topic := common.PartitionTopic(p.topic, e.AggregateIDHash, p.partitions)
	logrus.Debugf("publishing '%+v' to topic '%s'", e, topic)

	// TODO should be configurable
	bo := backoff.NewExponentialBackOff()
	bo.MaxElapsedTime = 10 * time.Second

	err = backoff.Retry(func() error {
		err := p.client.Publish(topic, b)
		if err != nil && p.client.NatsConn().IsClosed() {
			return backoff.Permanent(err)
		}
		return err
	}, bo)
	if err != nil {
		return faults.Errorf("Failed to send message: %w", err)
	}
	return nil
}
