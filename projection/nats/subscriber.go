package nats

import (
	"context"
	"fmt"
	"log/slog"
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
)

func NewSubscriberWithURL(
	ctx context.Context,
	logger *slog.Logger,
	url string,
	topic projection.ConsumerTopic,
) (*Subscriber, error) {
	nc, err := nats.Connect(url)
	if err != nil {
		return nil, faults.Errorf("instantiating NATS connection: %w", err)
	}
	subscriber, err := NewSubscriberWithConn(
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

func NewSubscriberWithConn(
	logger *slog.Logger,
	nc *nats.Conn,
	topic projection.ConsumerTopic,
) (*Subscriber, error) {
	stream, err := nc.JetStream()
	if err != nil {
		return nil, faults.Wrap(err)
	}

	return NewSubscriber(logger, stream, topic), nil
}

type SubOption func(*Subscriber)

func WithMsgCodec(codec sink.Codec) SubOption {
	return func(r *Subscriber) {
		r.codec = codec
	}
}

var _ projection.Consumer = (*Subscriber)(nil)

type Subscriber struct {
	logger *slog.Logger
	js     nats.JetStreamContext
	topic  projection.ConsumerTopic
	codec  sink.Codec

	mu            sync.RWMutex
	subscriptions []*nats.Subscription
}

func NewSubscriber(
	logger *slog.Logger,
	js nats.JetStreamContext,
	topic projection.ConsumerTopic,
	options ...SubOption,
) *Subscriber {
	s := &Subscriber{
		logger: logger,
		js:     js,
		topic:  topic,
		codec:  sink.JSONCodec{},
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
			Position: seq,
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

func (s *Subscriber) StartConsumer(ctx context.Context, subPos map[uint32]projection.SubscriberPosition, projName string, handler projection.ConsumerHandler, options ...projection.ConsumerOption) (er error) {
	logger := s.logger.With("topic", s.topic.Topic)
	opts := projection.ConsumerOptions{
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

		if subPos != nil {
			pos := subPos[uint32(part)]
			var startOption nats.SubOpt
			if pos.Position == 0 {
				logger.Info("Starting consuming all available events", "topic", natsOpts, "partition", part)
				startOption = nats.StartSequence(1)
			} else {
				logger.Info("Starting consumer from an offset", "from", pos.Position+1, "topic", natsOpts, "partition", part)
				startOption = nats.StartSequence(pos.Position + 1) // after seq
			}

			natsOpts = append(natsOpts, startOption)
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
