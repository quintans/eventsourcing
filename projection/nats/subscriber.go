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

	"github.com/quintans/eventsourcing/log"
	"github.com/quintans/eventsourcing/projection"
	"github.com/quintans/eventsourcing/sink"
	"github.com/quintans/eventsourcing/util"
)

func NewSubscriberWithURL(
	ctx context.Context,
	logger log.Logger,
	url string,
	topic string,
) (*Subscriber, error) {
	nc, err := nats.Connect(url)
	if err != nil {
		return nil, faults.Errorf("Could not instantiate NATS connection: %w", err)
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
	logger log.Logger,
	nc *nats.Conn,
	topic string,
) (*Subscriber, error) {
	stream, err := nc.JetStream()
	if err != nil {
		return nil, faults.Wrap(err)
	}

	t, err := util.NewTopic(topic)
	if err != nil {
		return nil, faults.Wrap(err)
	}

	return NewSubscriber(logger, stream, t), nil
}

type SubOption func(*Subscriber)

func WithMsgCodec(codec sink.Codec) SubOption {
	return func(r *Subscriber) {
		r.messageCodec = codec
	}
}

var _ projection.Consumer = (*Subscriber)(nil)

type Subscriber struct {
	logger       log.Logger
	jetStream    nats.JetStreamContext
	topic        util.Topic
	messageCodec sink.Codec

	mu           sync.RWMutex
	subscription *nats.Subscription
}

func NewSubscriber(
	logger log.Logger,
	jetStream nats.JetStreamContext,
	topic util.Topic,
	options ...SubOption,
) *Subscriber {
	s := &Subscriber{
		logger:       logger,
		jetStream:    jetStream,
		topic:        topic,
		messageCodec: sink.JSONCodec{},
	}
	s.logger = logger.WithTags(log.Tags{
		"id": "subscriber-" + shortid.MustGenerate(),
	})

	for _, o := range options {
		o(s)
	}

	return s
}

func (s *Subscriber) Topic() util.Topic {
	return s.topic
}

func (s *Subscriber) RetrieveLastSequence(ctx context.Context) (uint64, error) {
	ch := make(chan uint64)
	// this will position the stream at the last position+1
	sub, err := s.jetStream.Subscribe(
		s.topic.String(),
		func(m *nats.Msg) {
			ch <- sequence(m)
			m.Ack()
		},
		nats.DeliverLast(),
		nats.MaxDeliver(2),
	)
	if err != nil {
		return 0, faults.Wrap(err)
	}
	defer sub.Unsubscribe()
	var resume uint64
	// will wait until a message is available
	select {
	case resume = <-ch:
	case <-ctx.Done():
		return 0, faults.New("failed to get subscription to last event ID")
	}
	return resume, nil
}

func (s *Subscriber) StartConsumer(ctx context.Context, proj projection.Projection, options ...projection.ConsumerOption) error {
	logger := s.logger.WithTags(log.Tags{"topic": s.topic.String()})
	opts := projection.ConsumerOptions{
		AckWait: 30 * time.Second,
	}
	for _, v := range options {
		v(&opts)
	}

	var startOption nats.SubOpt
	token, err := proj.StreamResumeToken(ctx, s.topic)
	if err != nil && !errors.Is(err, projection.ErrResumeTokenNotFound) {
		return faults.Errorf("Could not retrieve resume token for '%s': %w", s.topic, err)
	}
	if token.IsEmpty() {
		logger.WithTags(log.Tags{"topic": s.topic}).Info("Starting consuming all available events", s.topic)
		// startOption = nats.DeliverAll()
		startOption = nats.StartSequence(1)
	} else {
		logger.WithTags(log.Tags{"from": token.Sequence(), "topic": s.topic}).Info("Starting consumer")
		startOption = nats.StartSequence(token.Sequence() + 1) // after seq
	}

	callback := func(m *nats.Msg) {
		evt, er := s.messageCodec.Decode(m.Data)
		if er != nil {
			logger.WithError(er).Errorf("unable to unmarshal event '%s'", string(m.Data))
			m.Nak()
			return
		}
		seq := sequence(m)
		if opts.Filter == nil || opts.Filter(evt) {
			logger.Debugf("Handling received event '%+v'", evt)
			er = proj.Handler(
				ctx,
				projection.Meta{
					Topic:     s.topic.Root(),
					Partition: s.topic.Partition(),
					Token:     projection.NewToken(projection.ConsumerToken, seq),
				},
				evt,
			)
			if er != nil {
				logger.WithError(er).Errorf("Error when handling event with ID '%s'", evt.ID)
				m.Nak()
				return
			}
		}

		if er := m.Ack(); er != nil {
			logger.WithError(er).Errorf("failed to ACK seq=%d, event=%+v", seq, evt)
			return
		}
	}
	// nod dots (.) allowed
	groupName := strings.ReplaceAll(fmt.Sprintf("%s:%s", s.topic, proj.Name()), ".", "_")
	natsOpts := []nats.SubOpt{
		startOption,
		nats.Durable(groupName),
		nats.MaxAckPending(1),
		nats.AckExplicit(),
		nats.AckWait(opts.AckWait),
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.subscription, err = s.jetStream.QueueSubscribe(s.topic.String(), groupName, callback, natsOpts...)
	if err != nil {
		return faults.Errorf("failed to subscribe to %s: %w", s.topic, err)
	}

	go func() {
		select {
		case <-ctx.Done():
			s.stopConsumer()
		}
	}()
	return nil
}

func (s *Subscriber) stopConsumer() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.subscription == nil {
		return
	}

	err := s.subscription.Unsubscribe()
	if err != nil {
		s.logger.WithError(err).Warnf("Failed to unsubscribe from '%s'", s.topic)
	} else {
		s.logger.Infof("Unsubscribed from '%s'", s.topic)
	}

	s.subscription = nil
}

func sequence(m *nats.Msg) uint64 {
	md, _ := m.Metadata()
	return md.Sequence.Stream
}
