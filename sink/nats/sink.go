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

// NewSink instantiate NATS sink
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

func (s *Sink) SetCodec(codec sink.Codec) {
	s.codec = codec
}

func (s *Sink) Close() {
	if s.nc != nil {
		s.nc.Close()
	}
}

// LastMessage gets the last message sent to NATS
// It will return 0 if there is no last message
func (s *Sink) LastMessage(ctx context.Context, partition uint32) (uint64, *sink.Message, error) {
	type message struct {
		sequence uint64
		data     []byte
	}
	topic, err := util.NewPartitionedTopic(s.topic, partition)
	if err != nil {
		return 0, nil, faults.Wrap(err)
	}
	ch := make(chan message)
	_, err = s.js.Subscribe(
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
		return 0, nil, faults.Wrap(err)
	}
	ctx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()

	var msg message
	select {
	case msg = <-ch:
	case <-ctx.Done():
		// no last message
		return 0, nil, nil
	}
	event, err := s.codec.Decode(msg.data)
	if err != nil {
		return 0, nil, err
	}

	return msg.sequence, event, nil
}

func sequence(m *nats.Msg) uint64 {
	md, _ := m.Metadata()
	return md.Sequence.Stream
}

// Sink sends the event to the message queue
func (s *Sink) Sink(ctx context.Context, e *eventsourcing.Event, m sink.Meta) (sink.Data, error) {
	b, err := s.codec.Encode(e, m)
	if err != nil {
		return sink.Data{}, err
	}

	topic, err := util.PartitionTopic(s.topic, e.AggregateIDHash, s.partitions)
	if err != nil {
		return sink.Data{}, faults.Wrap(err)
	}
	s.logger.WithTags(log.Tags{
		"topic": topic,
	}).Debugf("publishing '%+v'", e)

	// TODO should be configurable
	bo := backoff.NewExponentialBackOff()
	bo.MaxElapsedTime = 10 * time.Second

	var sequence uint64
	err = backoff.Retry(func() error {
		if er := ctx.Err(); er != nil {
			return backoff.Permanent(er)
		}
		ack, er := s.js.Publish(topic.String(), b)
		if er != nil && s.nc.IsClosed() {
			return backoff.Permanent(er)
		}
		sequence = ack.Sequence
		return er
	}, bo)
	if err != nil {
		return sink.Data{}, faults.Errorf("failed to send message %+v on topic %s: %w", e, topic, err)
	}
	return sink.NewSinkData(topic.Partition(), sequence), nil
}

func createStream(logger log.Logger, js nats.JetStreamContext, streamName util.Topic) error {
	// Check if the ORDERS stream already exists; if not, create it.
	_, err := js.StreamInfo(streamName.String())
	if err == nil {
		logger.Infof("stream found '%s'", streamName)
		return nil
	}

	logger.Infof("stream not found '%s'. creating.", streamName)
	_, err = js.AddStream(&nats.StreamConfig{
		Name: streamName.String(),
	})
	return faults.Wrap(err)
}
