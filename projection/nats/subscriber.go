package nats

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/teris-io/shortid"

	"github.com/quintans/faults"

	"github.com/quintans/eventsourcing/eventid"
	"github.com/quintans/eventsourcing/log"
	"github.com/quintans/eventsourcing/projection"
	"github.com/quintans/eventsourcing/sink"
	snats "github.com/quintans/eventsourcing/sink/nats"
	"github.com/quintans/eventsourcing/store"
)

const checkPointBuffer = 1_000

func NewSubscriberWithURL(
	ctx context.Context,
	logger log.Logger,
	url string,
	topic projection.ConsumerTopic,
	resumeStore store.KVStore,
) (*Subscriber, error) {
	nc, err := nats.Connect(url)
	if err != nil {
		return nil, faults.Errorf("instantiating NATS connection: %w", err)
	}
	subscriber, err := NewSubscriberWithConn(
		logger,
		nc,
		topic,
		resumeStore,
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

func NewSubscriberWithConn(
	logger log.Logger,
	nc *nats.Conn,
	topic projection.ConsumerTopic,
	resumeStore store.KVStore,
) (*Subscriber, error) {
	stream, err := nc.JetStream()
	if err != nil {
		return nil, faults.Wrap(err)
	}

	return NewSubscriber(logger, stream, topic, resumeStore), nil
}

type SubOption func(*Subscriber)

func WithMsgCodec(codec sink.Codec) SubOption {
	return func(r *Subscriber) {
		r.codec = codec
	}
}

var _ projection.Consumer = (*Subscriber)(nil)

type Subscriber struct {
	logger      log.Logger
	js          nats.JetStreamContext
	topic       projection.ConsumerTopic
	codec       sink.Codec
	resumeStore store.KVStore

	mu            sync.RWMutex
	subscriptions []*nats.Subscription
}

func NewSubscriber(
	logger log.Logger,
	js nats.JetStreamContext,
	topic projection.ConsumerTopic,
	resumeStore store.KVStore,
	options ...SubOption,
) *Subscriber {
	s := &Subscriber{
		logger:      logger,
		js:          js,
		topic:       topic,
		codec:       sink.JSONCodec{},
		resumeStore: resumeStore,
	}
	s.logger = logger.WithTags(log.Tags{
		"id": "subscriber-" + shortid.MustGenerate(),
	})

	for _, o := range options {
		o(s)
	}

	return s
}

func (s *Subscriber) TopicPartitions() (string, []uint32) {
	return s.topic.Topic, s.topic.Partitions
}

func (s *Subscriber) Positions(ctx context.Context) (map[uint32]projection.SubscriberPosition, error) {
	bms := map[uint32]projection.SubscriberPosition{}
	for _, p := range s.topic.Partitions {
		seq, eventID, err := s.lastBUSMessage(ctx, p)
		if err != nil {
			return nil, faults.Wrap(err)
		}
		bms[p] = projection.SubscriberPosition{
			EventID:  eventID,
			Sequence: seq,
		}
	}

	return bms, nil
}

// lastBUSMessage gets the last message sent to NATS
// It will return 0 if there is no last message
func (s *Subscriber) lastBUSMessage(ctx context.Context, partition uint32) (uint64, eventid.EventID, error) {
	type message struct {
		sequence uint64
		data     []byte
	}
	topic, err := snats.ComposeTopic(s.topic.Topic, partition)
	if err != nil {
		return 0, eventid.Zero, faults.Wrap(err)
	}
	ch := make(chan message)
	_, err = s.js.Subscribe(
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
		return 0, eventid.Zero, faults.Errorf("subscribing topic '%s': %w", topic, err)
	}
	ctx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()

	var msg message
	select {
	case msg = <-ch:
	case <-ctx.Done():
		// no last message
		return 0, eventid.Zero, nil
	}
	event, err := s.codec.Decode(msg.data)
	if err != nil {
		return 0, eventid.Zero, err
	}

	return msg.sequence, event.ID, nil
}

func (s *Subscriber) StartConsumer(ctx context.Context, projName string, handler projection.ConsumerHandler, options ...projection.ConsumerOption) (er error) {
	logger := s.logger.WithTags(log.Tags{"topic": s.topic.Topic})
	opts := projection.ConsumerOptions{
		AckWait: 30 * time.Second,
	}
	for _, v := range options {
		v(&opts)
	}

	for _, part := range s.topic.Partitions {
		resumeKey, err := projection.NewResumeKey(projName, s.topic.Topic, part)
		if err != nil {
			return faults.Wrap(err)
		}

		data, err := s.resumeStore.Get(ctx, resumeKey.String())
		if err != nil && !errors.Is(err, store.ErrResumeTokenNotFound) {
			return faults.Errorf("Could not retrieve resume token for '%s': %w", resumeKey, err)
		}

		token, err := projection.ParseToken(data)
		if err != nil {
			return faults.Wrap(err)
		}

		natsTopic, err := snats.ComposeTopic(s.topic.Topic, part)
		if err != nil {
			return faults.Wrap(err)
		}

		var startOption nats.SubOpt
		if token.IsEmpty() || token.Kind() == projection.CatchUpToken {
			logger.WithTags(log.Tags{"topic": natsTopic}).Info("Starting consuming all available events")
			startOption = nats.StartSequence(1)
		} else {
			logger.WithTags(log.Tags{"from": token.ConsumerSequence(), "topic": natsTopic}).Info("Starting consumer")
			startOption = nats.StartSequence(token.ConsumerSequence() + 1) // after seq
		}

		callback := func(m *nats.Msg) {
			evt, er := s.codec.Decode(m.Data)
			if er != nil {
				logger.WithError(er).Errorf("unable to unmarshal event '%s'", string(m.Data))
				_ = m.Nak()
				return
			}
			seq := sequence(m)
			if opts.Filter == nil || opts.Filter(evt) {
				logger.Debugf("Handling received event '%+v'", evt)
				er = handler(ctx, evt, part, seq)
				if er != nil {
					logger.WithError(er).Errorf("Error when handling event with ID '%s'", evt.ID)
					_ = m.Nak()
					return
				}
			}

			if er := m.Ack(); er != nil {
				logger.WithError(er).Errorf("Failed to ACK seq=%d, event=%+v", seq, evt)
				return
			}
		}
		// no dots (.) allowed
		groupName := strings.ReplaceAll(fmt.Sprintf("%s:%s", s.topic.Topic, projName), ".", "_")
		natsOpts := []nats.SubOpt{
			startOption,
			nats.Durable(groupName),
			nats.MaxAckPending(1),
			nats.AckExplicit(),
			nats.AckWait(opts.AckWait),
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

func (s *Subscriber) stopConsumer() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.subscriptions == nil {
		return
	}

	for _, sub := range s.subscriptions {
		err := sub.Unsubscribe()
		if err != nil {
			s.logger.WithError(err).Warnf("Failed to unsubscribe from '%s'", s.topic.Topic)
		} else {
			s.logger.Infof("Unsubscribed from '%s'", s.topic.Topic)
		}
	}

	s.subscriptions = nil
}

func sequence(m *nats.Msg) uint64 {
	md, _ := m.Metadata()
	return md.Sequence.Stream
}
