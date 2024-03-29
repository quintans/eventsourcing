package nats

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/nats-io/nats.go"
	"github.com/oklog/ulid/v2"
	"github.com/quintans/faults"

	"github.com/quintans/eventsourcing"
	"github.com/quintans/eventsourcing/encoding"
	"github.com/quintans/eventsourcing/log"
	"github.com/quintans/eventsourcing/sink"
	"github.com/quintans/eventsourcing/store"
	"github.com/quintans/eventsourcing/util"
)

var _ sink.Sinker[*ulid.ULID] = (*Sink[*ulid.ULID])(nil)

const checkPointBuffer = 1_000

type Sink[K eventsourcing.ID] struct {
	kvStore         store.KVStore
	logger          *slog.Logger
	topic           string
	nc              *nats.Conn
	js              nats.JetStreamContext
	totalPartitions uint32
	codec           sink.Codec[K]

	checkPointCh chan resume
}

type resume struct {
	key   string
	value encoding.Base64
}

// NewSink instantiate NATS sink
func NewSink[K eventsourcing.ID, PK eventsourcing.IDPt[K]](kvStore store.KVStore, logger *slog.Logger, topic string, totalPartitions uint32, url string, options ...nats.Option) (_ *Sink[K], err error) {
	defer faults.Catch(&err, "NewSink(topic=%s, totalPartitions=%d)", topic, totalPartitions)

	p := &Sink[K]{
		kvStore:         kvStore,
		logger:          logger.With("sink", "nats"),
		topic:           topic,
		totalPartitions: totalPartitions,
		codec:           sink.JSONCodec[K, PK]{},
		checkPointCh:    make(chan resume, checkPointBuffer),
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

	for p := uint32(1); p <= totalPartitions; p++ {
		t, err := ComposeTopic(topic, p)
		if err != nil {
			return nil, faults.Wrap(err)
		}
		if err := createStream(logger, js, t); err != nil {
			return nil, faults.Wrap(err)
		}
	}

	// saves into the resume db. It is fine if it sporadically fails. It will just pickup from there
	go func() {
		for cp := range p.checkPointCh {
			if cp.key == "" {
				return
			}

			ts := cp.value.String()
			err := p.kvStore.Put(context.Background(), cp.key, ts)
			if err != nil {
				logger.Error("Failed to save sink resume key",
					"topic", cp.key,
					"resume", ts,
					log.Err(err),
				)
			}
		}
	}()

	return p, nil
}

func (s *Sink[K]) SetCodec(codec sink.Codec[K]) {
	s.codec = codec
}

func (s *Sink[K]) Close() {
	if s.nc != nil {
		s.nc.Close()
	}
	s.checkPointCh <- resume{}
}

// ResumeTokens iterates over all the last saved resumed token per partition
// It will return 0 if there is no last message
func (s *Sink[K]) ResumeTokens(ctx context.Context, forEach func(resumeToken encoding.Base64) error) error {
	for p := uint32(1); p <= s.totalPartitions; p++ {
		topic, err := ComposeTopic(s.topic, p)
		if err != nil {
			return faults.Wrap(err)
		}
		resume, err := s.kvStore.Get(ctx, topic)
		if err != nil {
			if errors.Is(err, store.ErrResumeTokenNotFound) {
				continue
			}
			return faults.Wrap(err)
		}

		dec, err := encoding.ParseBase64(resume)
		if err != nil {
			return faults.Wrap(err)
		}
		err = forEach(dec)
		if err != nil {
			return faults.Wrap(err)
		}
	}

	return nil
}

// Sink sends the event to the message queue
func (s *Sink[K]) Sink(ctx context.Context, e *eventsourcing.Event[K], m sink.Meta) error {
	b, err := s.codec.Encode(e)
	if err != nil {
		return err
	}

	partition := util.CalcPartition(e.AggregateIDHash, s.totalPartitions)

	natsTopic, err := ComposeTopic(s.topic, partition)
	if err != nil {
		return faults.Wrap(err)
	}

	// TODO should be configurable
	bo := backoff.NewExponentialBackOff()
	bo.MaxElapsedTime = 10 * time.Second

	err = backoff.Retry(func() error {
		if er := ctx.Err(); er != nil {
			return backoff.Permanent(er)
		}
		_, er := s.js.Publish(natsTopic, b)
		if er != nil && s.nc.IsClosed() {
			return backoff.Permanent(er)
		}
		return er
	}, bo)
	if err != nil {
		return faults.Errorf("failed to send message %+v on topic %s: %w", e, natsTopic, err)
	}

	s.checkPointCh <- resume{key: natsTopic, value: m.ResumeToken}

	return nil
}

func createStream(logger *slog.Logger, js nats.JetStreamContext, topic string) error {
	// Check if the ORDERS stream already exists; if not, create it.
	_, err := js.StreamInfo(topic)
	if err == nil {
		logger.Info("stream found", "stream", topic)
		return nil
	}

	logger.Info("stream not found. creating.", "stream", topic)
	_, err = js.AddStream(&nats.StreamConfig{
		Name: topic,
	})
	return faults.Wrap(err)
}
