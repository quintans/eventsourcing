package nats

import (
	"context"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/nats-io/nats.go"
	"github.com/quintans/faults"

	"github.com/quintans/eventsourcing"
	"github.com/quintans/eventsourcing/log"
	"github.com/quintans/eventsourcing/sink"
	"github.com/quintans/eventsourcing/util"
)

var _ sink.Sinker = (*Sink)(nil)

type Sink struct {
	logger     log.Logger
	topic      string
	nc         *nats.Conn
	js         nats.JetStreamContext
	partitions uint32
	codec      sink.Codec
}

// NewSink instantiate nats sink
func NewSink(logger log.Logger, topic string, partitions uint32, url string, options ...nats.Option) (_ *Sink, err error) {
	defer faults.Catch(&err, "NewSink(topic=%s, partitions=%d)", topic, partitions)

	p := &Sink{
		logger:     logger,
		topic:      topic,
		partitions: partitions,
		codec:      sink.JSONCodec{},
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

	if partitions <= 1 {
		t, err := util.NewPartitionedTopic(topic, 0)
		if err != nil {
			return nil, faults.Wrap(err)
		}
		if err := createStream(logger, js, t); err != nil {
			return nil, faults.Wrap(err)
		}
	} else {
		for p := uint32(1); p <= partitions; p++ {
			t, err := util.NewPartitionedTopic(topic, 0)
			if err != nil {
				return nil, faults.Wrap(err)
			}
			if err := createStream(logger, js, t); err != nil {
				return nil, faults.Wrap(err)
			}
		}
	}

	return p, nil
}

func (p *Sink) SetCodec(codec sink.Codec) {
	p.codec = codec
}

func (p *Sink) Close() {
	if p.nc != nil {
		p.nc.Close()
	}
}

// LastMessage gets the last message sent to NATS
func (p *Sink) LastMessage(ctx context.Context, partition uint32) (*eventsourcing.Event, error) {
	type message struct {
		sequence uint64
		data     []byte
	}
	topic, err := util.NewPartitionedTopic(p.topic, partition)
	if err != nil {
		return nil, faults.Wrap(err)
	}
	ch := make(chan message)
	_, err = p.js.Subscribe(
		topic.String(),
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
	ctx, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
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

	return event, nil
}

func sequence(m *nats.Msg) uint64 {
	md, _ := m.Metadata()
	return md.Sequence.Stream
}

// Sink sends the event to the message queue
func (p *Sink) Sink(ctx context.Context, e *eventsourcing.Event) error {
	b, err := p.codec.Encode(e)
	if err != nil {
		return err
	}

	topic, err := util.PartitionTopic(p.topic, e.AggregateIDHash, p.partitions)
	if err != nil {
		return faults.Wrap(err)
	}
	p.logger.WithTags(log.Tags{
		"topic": topic,
	}).Debugf("publishing '%+v'", e)

	// TODO should be configurable
	bo := backoff.NewExponentialBackOff()
	bo.MaxElapsedTime = 10 * time.Second

	err = backoff.Retry(func() error {
		_, er := p.js.Publish(topic.String(), b)
		if er != nil && p.nc.IsClosed() {
			return backoff.Permanent(er)
		}
		return er
	}, bo)
	if err != nil {
		return faults.Errorf("failed to send message %+v on topic %s: %w", e, topic, err)
	}
	return nil
}

func createStream(logger log.Logger, js nats.JetStreamContext, streamName util.Topic) error {
	// Check if the ORDERS stream already exists; if not, create it.
	_, err := js.StreamInfo(streamName.String())
	if err == nil {
		logger.Infof("stream '%s' found", streamName)
		return nil
	}

	logger.Infof("stream '%s' not found. creating.", streamName)
	_, err = js.AddStream(&nats.StreamConfig{
		Name: streamName.String(),
	})
	return faults.Wrap(err)
}
