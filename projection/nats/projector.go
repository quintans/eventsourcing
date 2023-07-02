package nats

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/teris-io/shortid"

	"github.com/quintans/faults"

	"github.com/quintans/eventsourcing/log"
	"github.com/quintans/eventsourcing/projection"
	"github.com/quintans/eventsourcing/sink"
	"github.com/quintans/eventsourcing/worker"
)

func NewProjector(
	ctx context.Context,
	logger log.Logger,
	url string,
	lockerFactory projection.LockerFactory,
	catchUpLockerFactory projection.WaitLockerFactory,
	resumeStore projection.ResumeStore,
	resumeKey projection.ResumeKey,
	projectionName string,
	topic string,
	esRepo projection.Repository,
	handler projection.MessageHandlerFunc,
	options projection.ProjectorOptions,
) (*worker.RunWorker, error) {
	nc, err := nats.Connect(url)
	if err != nil {
		return nil, faults.Errorf("Could not instantiate NATS connection: %w", err)
	}
	w, err := NewProjectorWithConn(
		ctx,
		logger,
		nc,
		lockerFactory,
		catchUpLockerFactory,
		resumeStore,
		resumeKey,
		projectionName,
		topic,
		esRepo,
		handler,
		options,
	)
	if err != nil {
		return nil, err
	}
	go func() {
		<-ctx.Done()
		nc.Close()
	}()

	return w, nil
}

func NewProjectorWithConn(
	ctx context.Context,
	logger log.Logger,
	nc *nats.Conn,
	lockerFactory projection.LockerFactory,
	catchUpLockerFactory projection.WaitLockerFactory,
	resumeStore projection.ResumeStore,
	resumeKey projection.ResumeKey,
	projectionName string,
	topic string,
	esRepo projection.Repository,
	handler projection.MessageHandlerFunc,
	options projection.ProjectorOptions,
) (*worker.RunWorker, error) {
	stream, err := nc.JetStream()
	if err != nil {
		return nil, faults.Wrap(err)
	}

	logger = logger.WithTags(log.Tags{
		"projection": projectionName,
	})
	subscriber := NewSubscriber(logger, stream, resumeStore, resumeKey)
	return projection.NewProjector(
		ctx,
		logger,
		lockerFactory,
		catchUpLockerFactory,
		resumeStore,
		subscriber,
		esRepo,
		handler,
		options,
	), nil
}

type SubOption func(*Subscriber)

func WithMsgCodec(codec sink.Codec) SubOption {
	return func(r *Subscriber) {
		r.messageCodec = codec
	}
}

var _ projection.Subscriber = (*Subscriber)(nil)

type Subscriber struct {
	logger       log.Logger
	jetStream    nats.JetStreamContext
	resumeStore  projection.ResumeStore
	resumeKey    projection.ResumeKey
	messageCodec sink.Codec

	mu           sync.RWMutex
	done         chan struct{}
	subscription *nats.Subscription
}

func NewSubscriber(
	logger log.Logger,
	jetStream nats.JetStreamContext,
	resumeStore projection.ResumeStore,
	resumeKey projection.ResumeKey,
	options ...SubOption,
) *Subscriber {
	s := &Subscriber{
		logger:       logger,
		jetStream:    jetStream,
		resumeStore:  resumeStore,
		resumeKey:    resumeKey,
		messageCodec: sink.JSONCodec{},
	}
	s.logger = logger.WithTags(log.Tags{
		"id": shortid.MustGenerate(),
	})

	for _, o := range options {
		o(s)
	}

	return s
}

func (s *Subscriber) ResumeKey() projection.ResumeKey {
	return s.resumeKey
}

func (s *Subscriber) RetrieveLastSequence(ctx context.Context) (uint64, error) {
	ch := make(chan uint64)
	// this will position the stream at the last position+1
	sub, err := s.jetStream.Subscribe(
		s.resumeKey.Topic().String(),
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

func (s *Subscriber) SaveLastSequence(ctx context.Context, token uint64) error {
	return s.resumeStore.SetStreamResumeToken(ctx, s.resumeKey, projection.NewToken(projection.ConsumerToken, token))
}

func (s *Subscriber) StartConsumer(ctx context.Context, handler projection.MessageHandlerFunc, options ...projection.ConsumerOption) error {
	logger := s.logger.WithTags(log.Tags{"topic": s.resumeKey.Topic()})
	opts := projection.ConsumerOptions{
		AckWait: 30 * time.Second,
	}
	for _, v := range options {
		v(&opts)
	}

	var startOption nats.SubOpt
	token, err := s.resumeStore.GetStreamResumeToken(ctx, s.resumeKey)
	if err != nil && !errors.Is(err, projection.ErrResumeTokenNotFound) {
		return faults.Errorf("Could not retrieve resume token for '%s': %w", s.resumeKey, err)
	}
	if token.IsEmpty() {
		logger.Infof("Starting consuming all available [token key: '%s']", s.resumeKey)
		// startOption = nats.DeliverAll()
		startOption = nats.StartSequence(1)
	} else {
		logger.Infof("Starting consuming from '%s' [token key: '%s']", token, s.resumeKey)
		startOption = nats.StartSequence(token.Value() + 1) // after seq
	}

	callback := func(m *nats.Msg) {
		evt, er := s.messageCodec.Decode(m.Data)
		if er != nil {
			logger.WithError(er).Errorf("unable to unmarshal event '%s'", string(m.Data))
			m.Nak()
			return
		}
		if opts.Filter == nil || opts.Filter(evt) {
			logger.Debugf("Handling received event '%+v'", evt)
			er = handler(ctx, projection.Meta{Sequence: sequence(m)}, evt)
			if er != nil {
				logger.WithError(er).Errorf("Error when handling event with ID '%s'", evt.ID)
				m.Nak()
				return
			}
		}

		seq := sequence(m)
		if er := m.Ack(); er != nil {
			logger.WithError(er).Errorf("failed to ACK seq=%d, event=%+v", seq, evt)
			return
		}
	}
	groupName := s.resumeKey.String()
	natsOpts := []nats.SubOpt{
		startOption,
		nats.Durable(groupName),
		nats.MaxAckPending(1),
		nats.AckExplicit(),
		nats.AckWait(opts.AckWait),
	}
	s.subscription, err = s.jetStream.QueueSubscribe(s.resumeKey.Topic().String(), groupName, callback, natsOpts...)
	if err != nil {
		return faults.Errorf("failed to subscribe to %s: %w", s.resumeKey.Topic(), err)
	}

	done := make(chan struct{})
	s.mu.Lock()
	s.done = done
	s.mu.Unlock()
	go func() {
		select {
		case <-done:
		case <-ctx.Done():
			s.StopConsumer(context.Background())
		}
	}()
	return nil
}

func (s *Subscriber) StopConsumer(ctx context.Context) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.done == nil {
		return
	}

	err := s.subscription.Unsubscribe()
	if err != nil {
		s.logger.WithError(err).Warnf("Failed to unsubscribe from '%s'", s.resumeKey.Topic())
	} else {
		s.logger.Infof("Unsubscribed from '%s'", s.resumeKey.Topic())
	}

	s.subscription = nil
	close(s.done)
	s.done = nil
}

func sequence(m *nats.Msg) uint64 {
	md, _ := m.Metadata()
	return md.Sequence.Stream
}
