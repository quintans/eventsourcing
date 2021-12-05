package nats

import (
	"context"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/nats-io/nats.go"
	"github.com/quintans/faults"

	"github.com/quintans/eventsourcing"
	"github.com/quintans/eventsourcing/common"
	"github.com/quintans/eventsourcing/log"
	"github.com/quintans/eventsourcing/sink"
)

type NatsSink struct {
	logger     log.Logger
	topic      string
	nc         *nats.Conn
	js         nats.JetStreamContext
	partitions uint32
	codec      sink.Codec
}

// NewSink instantiate nats sink
func NewSink(logger log.Logger, topic string, partitions uint32, url string, options ...nats.Option) (_ *NatsSink, err error) {
	defer faults.Catch(&err, "NewSink(topic=%s, partitions=%d)", topic, partitions)

	p := &NatsSink{
		logger:     logger,
		topic:      topic,
		partitions: partitions,
		codec:      sink.JsonCodec{},
	}

	nc, err := nats.Connect(url, options...)
	if err != nil {
		return nil, faults.Errorf("Could not instantiate Nats connection: %w", err)
	}
	p.nc = nc
	js, err := nc.JetStream()
	if err != nil {
		return nil, faults.Errorf("Could not instantiate Nats jetstream context: %w", err)
	}
	p.js = js

	if partitions == 0 {
		createStream(js, common.TopicWithPartition(topic, 0))
	} else {
		for p := uint32(1); p <= partitions; p++ {
			createStream(js, common.TopicWithPartition(topic, p))
		}
	}

	return p, nil
}

func (p *NatsSink) SetCodec(codec sink.Codec) {
	p.codec = codec
}

func (p *NatsSink) Close() {
	if p.nc != nil {
		p.nc.Close()
	}
}

// LastMessage gets the last message sent to NATS
func (p *NatsSink) LastMessage(ctx context.Context, partition uint32) (*eventsourcing.Event, error) {
	type message struct {
		sequence uint64
		data     []byte
	}
	topic := common.TopicWithPartition(p.topic, partition)
	ch := make(chan message)
	_, err := p.js.Subscribe(
		topic,
		func(m *nats.Msg) {
			ch <- message{
				sequence: sequence(m),
				data:     m.Data,
			}
		},
		nats.DeliverLast(),
	)
	if err != nil {
		return nil, faults.Wrap(err)
	}
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

// Sink sends the event to the message queue
func (p *NatsSink) Sink(ctx context.Context, e eventsourcing.Event) error {
	b, err := p.codec.Encode(e)
	if err != nil {
		return err
	}

	topic := common.PartitionTopic(p.topic, e.AggregateIDHash, p.partitions)
	p.logger.WithTags(log.Tags{
		"topic": topic,
	}).Debugf("publishing '%+v'", e)

	// TODO should be configurable
	bo := backoff.NewExponentialBackOff()
	bo.MaxElapsedTime = 10 * time.Second

	err = backoff.Retry(func() error {
		_, err := p.js.Publish(topic, b)
		if err != nil && p.nc.IsClosed() {
			return backoff.Permanent(err)
		}
		return err
	}, bo)
	if err != nil {
		return faults.Errorf("Failed to send message: %w", err)
	}
	return nil
}

func createStream(js nats.JetStreamContext, streamName string) error {
	// Check if the ORDERS stream already exists; if not, create it.
	stream, err := js.StreamInfo(streamName)
	if err != nil {
		return err
	}
	if stream == nil {
		_, err = js.AddStream(&nats.StreamConfig{
			Name: streamName,
		})
		if err != nil {
			return err
		}
	}
	return nil
}
