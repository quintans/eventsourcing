package nats

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/oklog/ulid/v2"
	"github.com/teris-io/shortid"

	"github.com/quintans/eventsourcing"
	"github.com/quintans/faults"

	"github.com/quintans/eventsourcing/log"
	"github.com/quintans/eventsourcing/projection"
	"github.com/quintans/eventsourcing/sink"
	snats "github.com/quintans/eventsourcing/sink/nats"
)

func NewSubscriberWithURL[K eventsourcing.ID, PK eventsourcing.IDPt[K]](
	ctx context.Context,
	logger *slog.Logger,
	url string,
	topic projection.ConsumerTopic,
) (*Subscriber[K], error) {
	nc, err := nats.Connect(url)
	if err != nil {
		return nil, faults.Errorf("instantiating NATS connection: %w", err)
	}
	subscriber, err := NewSubscriberWithConn[K, PK](
		logger,
		nc,
		topic,
	)
	if err != nil {
		return nil, faults.Wrap(err)
	}
	go func() {
		<-ctx.Done()
		subscriber.stopConsumer()
		nc.Close()
	}()

	return subscriber, nil
}

func NewSubscriberWithConn[K eventsourcing.ID, PK eventsourcing.IDPt[K]](
	logger *slog.Logger,
	nc *nats.Conn,
	topic projection.ConsumerTopic,
) (*Subscriber[K], error) {
	stream, err := nc.JetStream()
	if err != nil {
		return nil, faults.Wrap(err)
	}

	return NewSubscriber[K, PK](logger, stream, topic), nil
}

type SubOption[K eventsourcing.ID] func(*Subscriber[K])

func WithMsgCodec[K eventsourcing.ID](codec sink.Codec[K]) SubOption[K] {
	return func(r *Subscriber[K]) {
		r.codec = codec
	}
}

var _ projection.Consumer[*ulid.ULID] = (*Subscriber[*ulid.ULID])(nil)

type Subscriber[K eventsourcing.ID] struct {
	logger *slog.Logger
	js     nats.JetStreamContext
	topic  projection.ConsumerTopic
	codec  sink.Codec[K]

	mu            sync.RWMutex
	subscriptions []*nats.Subscription
}

func NewSubscriber[K eventsourcing.ID, PK eventsourcing.IDPt[K]](
	logger *slog.Logger,
	js nats.JetStreamContext,
	topic projection.ConsumerTopic,
	options ...SubOption[K],
) *Subscriber[K] {
	s := &Subscriber[K]{
		logger: logger,
		js:     js,
		topic:  topic,
		codec:  sink.JSONCodec[K, PK]{},
	}
	s.logger = logger.With(
		"subscriber", "nats",
		"id", "subscriber-"+shortid.MustGenerate(),
	)

	for _, o := range options {
		o(s)
	}

	return s
}

func (s *Subscriber[K]) Topic() string {
	return s.topic.Topic
}

func (s *Subscriber[K]) StartConsumer(ctx context.Context, startTime *time.Time, projName string, handler projection.ConsumerHandler[K], options ...projection.ConsumerOption[K]) (er error) {
	logger := s.logger.With("topic", s.topic.Topic)
	opts := projection.ConsumerOptions[K]{
		AckWait: 30 * time.Second,
	}
	for _, v := range options {
		v(&opts)
	}

	for _, part := range s.topic.Partitions {

		natsTopic, err := snats.ComposeTopic(s.topic.Topic, part)
		if err != nil {
			return faults.Wrap(err)
		}

		callback := func(m *nats.Msg) {
			evt, er := s.codec.Decode(m.Data)
			if er != nil {
				logger.Error("unable to unmarshal event", "event", string(m.Data), log.Err(er))
				_ = m.Nak()
				return
			}
			seq := sequence(m)
			if opts.Filter == nil || opts.Filter(evt) {
				er = handler(ctx, evt, part, seq)
				if er != nil {
					logger.Error("Error when handling event", "eventID", evt.ID, log.Err(er))
					_ = m.Nak()
					return
				}
			}

			if er := m.Ack(); er != nil {
				logger.Error("Failed to ACK", "sequence", seq, "event", evt, log.Err(er))
				return
			}
		}
		// no dots (.) allowed
		groupName := strings.ReplaceAll(fmt.Sprintf("%s:%s", s.topic.Topic, projName), ".", "_")

		natsOpts := []nats.SubOpt{
			nats.Durable(groupName),
			nats.MaxAckPending(1),
			nats.AckExplicit(),
			nats.AckWait(opts.AckWait),
		}
		if startTime != nil {
			logger.Info("Starting consumer from start time", "from", *startTime, "partition", part)
			natsOpts = append(natsOpts, nats.StartTime(*startTime))
		}

		sub, err := s.js.QueueSubscribe(natsTopic, groupName, callback, natsOpts...)
		if err != nil {
			return faults.Errorf("subscribing to %s: %w", natsTopic, err)
		}

		s.mu.Lock()
		s.subscriptions = append(s.subscriptions, sub)
		s.mu.Unlock()
	}

	go func() {
		<-ctx.Done()
		s.stopConsumer()
	}()
	return nil
}

func (s *Subscriber[K]) stopConsumer() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.subscriptions == nil {
		return
	}

	for _, sub := range s.subscriptions {
		err := sub.Unsubscribe()
		if err != nil {
			s.logger.Warn("Failed to unsubscribe", "topic", s.topic.Topic, log.Err(err))
		} else {
			s.logger.Info("Unsubscribed", "topic", s.topic.Topic)
		}
	}

	s.subscriptions = nil
}

func sequence(m *nats.Msg) uint64 {
	md, _ := m.Metadata()
	return md.Sequence.Stream
}
