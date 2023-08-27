package nats

import (
	"context"
	"errors"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/nats-io/nats.go"
	"github.com/quintans/faults"

	"github.com/quintans/eventsourcing"
	"github.com/quintans/eventsourcing/encoding"
	"github.com/quintans/eventsourcing/log"
	"github.com/quintans/eventsourcing/sink"
	"github.com/quintans/eventsourcing/store"
	"github.com/quintans/eventsourcing/util"
)

var _ sink.Sinker = (*Sink)(nil)

const checkPointBuffer = 1_000

type Sink struct {
	kvStore    store.KVStore
	logger     log.Logger
	topic      string
	nc         *nats.Conn
	js         nats.JetStreamContext
	partitions uint32
	codec      sink.Codec

	mu                 util.Atomic
	checkPointCh       chan encoding.Base64
	checkPointChClosed bool
}

// NewSink instantiate NATS sink
func NewSink(kvStore store.KVStore, logger log.Logger, topic string, partitions uint32, url string, options ...nats.Option) (_ *Sink, err error) {
	defer faults.Catch(&err, "NewSink(topic=%s, partitions=%d)", topic, partitions)

	p := &Sink{
		kvStore:      kvStore,
		logger:       logger,
		topic:        topic,
		partitions:   partitions,
		codec:        sink.JSONCodec{},
		checkPointCh: make(chan encoding.Base64, checkPointBuffer),
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
		t, err := ComposeTopic(topic, 1)
		if err != nil {
			return nil, faults.Wrap(err)
		}
		if err := createStream(logger, js, t); err != nil {
			return nil, faults.Wrap(err)
		}
	} else {
		for p := uint32(1); p <= partitions; p++ {
			t, err := ComposeTopic(topic, p)
			if err != nil {
				return nil, faults.Wrap(err)
			}
			if err := createStream(logger, js, t); err != nil {
				return nil, faults.Wrap(err)
			}
		}
	}

	// saves into the resume db. It is fine if it sporadically fails. It will just pickup from there
	go func() {
		for token := range p.checkPointCh {
			ts := token.String()
			err := p.kvStore.Put(context.Background(), topic, ts)
			if err != nil {
				logger.WithError(err).WithTags(log.Tags{
					"topic":  topic,
					"resume": ts,
				}).Error("Failed to save sink resume key")
			}
		}
	}()

	return p, nil
}

func (s *Sink) SetCodec(codec sink.Codec) {
	s.codec = codec
}

func (s *Sink) Close() {
	if s.nc != nil {
		s.nc.Close()
	}
	s.mu.Lock(func() {
		close(s.checkPointCh)
		s.checkPointChClosed = true
	})
}

// ResumeToken gets the last saved resumed token
// It will return 0 if there is no last message
func (s *Sink) ResumeToken(ctx context.Context, partition uint32) (encoding.Base64, error) {
	topic, err := ComposeTopic(s.topic, partition)
	if err != nil {
		return nil, faults.Wrap(err)
	}
	resume, err := s.kvStore.Get(ctx, topic)
	if err != nil {
		if errors.Is(err, store.ErrResumeTokenNotFound) {
			return nil, nil
		}
		return nil, faults.Wrap(err)
	}

	dec, err := encoding.ParseBase64(resume)
	if err != nil {
		return nil, faults.Wrap(err)
	}
	return dec, nil
}

// Sink sends the event to the message queue
func (s *Sink) Sink(ctx context.Context, e *eventsourcing.Event, m sink.Meta) error {
	b, err := s.codec.Encode(e)
	if err != nil {
		return err
	}

	partition := util.CalcPartition(e.AggregateIDHash, s.partitions)
	topic, err := ComposeTopic(s.topic, partition)
	if err != nil {
		return faults.Wrap(err)
	}
	s.logger.WithTags(log.Tags{
		"topic": topic,
	}).Debugf("publishing '%+v'", e)

	// TODO should be configurable
	bo := backoff.NewExponentialBackOff()
	bo.MaxElapsedTime = 10 * time.Second

	err = backoff.Retry(func() error {
		if er := ctx.Err(); er != nil {
			return backoff.Permanent(er)
		}
		_, er := s.js.Publish(topic, b)
		if er != nil && s.nc.IsClosed() {
			return backoff.Permanent(er)
		}
		return er
	}, bo)
	if err != nil {
		return faults.Errorf("failed to send message %+v on topic %s: %w", e, topic, err)
	}
	s.mu.Lock(func() {
		if s.checkPointChClosed {
			return
		}
		s.checkPointCh <- m.ResumeToken
	})

	return nil
}

func createStream(logger log.Logger, js nats.JetStreamContext, topic string) error {
	// Check if the ORDERS stream already exists; if not, create it.
	_, err := js.StreamInfo(topic)
	if err == nil {
		logger.Infof("stream found '%s'", topic)
		return nil
	}

	logger.Infof("stream not found '%s'. creating.", topic)
	_, err = js.AddStream(&nats.StreamConfig{
		Name: topic,
	})
	return faults.Wrap(err)
}
