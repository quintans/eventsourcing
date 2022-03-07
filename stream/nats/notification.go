package nats

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/quintans/faults"
	"github.com/teris-io/shortid"

	"github.com/quintans/eventsourcing/log"
	"github.com/quintans/eventsourcing/projection"
	"github.com/quintans/eventsourcing/sink"
)

type SubOption func(*Subscriber)

func WithMsgCodec(codec sink.Codec) SubOption {
	return func(r *Subscriber) {
		r.messageCodec = codec
	}
}

var _ projection.Subscriber = (*ResumeableSubscriber)(nil)

type ResumeableSubscriber struct {
	logger       log.Logger
	jetStream    nats.JetStreamContext
	resumeStore  projection.ResumeStore
	resumeKey    projection.ResumeKey
	messageCodec sink.Codec

	mu           sync.Mutex
	done         chan struct{}
	subscription *nats.Subscription
}

func NewResumeableSubscriber(
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
		messageCodec: sink.JsonCodec{},
	}

	for _, o := range options {
		o(s)
	}

	return s
}

func (s *ResumeableSubscriber) RetrieveLastResume(ctx context.Context) (projection.Resume, error) {
	ch := make(chan projection.Resume)
	// this will position the stream at the last position+1
	sub, err := s.jetStream.Subscribe(
		s.resumeKey.Topic().String(),
		func(m *nats.Msg) {
			evt, err := s.messageCodec.Decode(m.Data)
			if err != nil {
				s.logger.WithError(err).Errorf("unable to unmarshal event '%s'", string(m.Data))
				m.Nak()
				return
			}

			seq := sequence(m)
			token := strconv.FormatUint(seq, 10)

			ch <- projection.Resume{
				Topic:   s.resumeKey.Topic(),
				EventID: evt.ID,
				Token:   token,
			}
			m.Ack()
		},
		nats.DeliverLast(),
		nats.MaxDeliver(2),
	)
	if err != nil {
		return projection.Resume{}, faults.Wrap(err)
	}
	defer sub.Unsubscribe()
	ctx, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
	defer cancel()
	var resume projection.Resume
	select {
	case resume = <-ch:
	case <-ctx.Done():
		return projection.Resume{}, faults.New("failed to move subscription to last position")
	}
	return resume, nil
}

func (s *ResumeableSubscriber) RecordLastResume(ctx context.Context, token string) error {
	return s.resumeStore.SetStreamResumeToken(ctx, s.resumeKey, token)
}

func (s *ResumeableSubscriber) StartConsumer(ctx context.Context, handler projection.EventHandlerFunc, options ...projection.ConsumerOption) error {
	logger := s.logger.WithTags(log.Tags{"topic": s.resumeKey.Topic()})
	opts := projection.ConsumerOptions{
		AckWait: 30 * time.Second,
	}
	for _, v := range options {
		v(&opts)
	}

	var token string
	var startOption nats.SubOpt
	saveResume := make(chan uint64, 100)
	var err error
	token, err = s.resumeStore.GetStreamResumeToken(ctx, s.resumeKey)
	if err != nil && !errors.Is(err, projection.ErrResumeTokenNotFound) {
		return faults.Errorf("Could not retrieve resume token for '%s': %w", s.resumeKey, err)
	}
	if token == "" {
		logger.Infof("Starting consuming all available [token key: '%s']", s.resumeKey)
		// startOption = nats.DeliverAll()
		startOption = nats.StartSequence(1)
	} else {
		logger.Infof("Starting consuming from '%s' [token key: '%s']", token, s.resumeKey)
		seq, err := strconv.ParseUint(token, 10, 64)
		if err != nil {
			return faults.Errorf("unable to parse resume token '%s': %w", token, err)
		}
		startOption = nats.StartSequence(seq + 1) // after seq
	}

	callback := func(m *nats.Msg) {
		evt, err := s.messageCodec.Decode(m.Data)
		if err != nil {
			logger.WithError(err).Errorf("unable to unmarshal event '%s'", string(m.Data))
			m.Nak()
			return
		}
		if opts.Filter == nil || opts.Filter(evt) {
			logger.Debugf("Handling received event '%+v'", evt)
			err = handler(ctx, evt)
			if err != nil {
				logger.WithError(err).Errorf("Error when handling event with ID '%s'", evt.ID)
				m.Nak()
				return
			}
		}

		seq := sequence(m)
		if err := m.Ack(); err != nil {
			logger.WithError(err).Errorf("failed to ACK seq=%d, event=%+v", seq, evt)
			return
		}

		saveResume <- seq
	}
	natsOpts := []nats.SubOpt{
		startOption,
		nats.MaxAckPending(1),
		nats.AckExplicit(),
		nats.AckWait(opts.AckWait),
	}
	s.subscription, err = s.jetStream.Subscribe(s.resumeKey.Topic().String(), callback, natsOpts...)
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
			s.StopConsumer(context.Background(), true)
		}
	}()

	go func() {
		// should we really care if they are store in sequence?
		for {
			select {
			case <-ctx.Done():
				return

			case seq := <-saveResume:
				err := s.resumeStore.SetStreamResumeToken(ctx, s.resumeKey, strconv.FormatUint(seq, 10))
				if err != nil {
					logger.WithError(err).Errorf("Failed to set the resume token for %s", s.resumeKey)
				}
			}
		}
	}()

	return nil
}

func (s *ResumeableSubscriber) StopConsumer(ctx context.Context, _ bool) {
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
		messageCodec: sink.JsonCodec{},
	}
	s.logger = logger.WithTags(log.Tags{
		"id": shortid.MustGenerate(),
	})

	for _, o := range options {
		o(s)
	}

	return s
}

func sequence(m *nats.Msg) uint64 {
	md, _ := m.Metadata()
	return md.Sequence.Stream
}

func (s *Subscriber) RetrieveLastResume(ctx context.Context) (projection.Resume, error) {
	ch := make(chan projection.Resume)
	// this will position the stream at the last position+1
	sub, err := s.jetStream.Subscribe(
		s.resumeKey.Topic().String(),
		func(m *nats.Msg) {
			evt, err := s.messageCodec.Decode(m.Data)
			if err != nil {
				s.logger.WithError(err).Errorf("unable to unmarshal event '%s'", string(m.Data))
				m.Nak()
				return
			}

			seq := sequence(m)
			token := strconv.FormatUint(seq, 10)

			ch <- projection.Resume{
				Topic:   s.resumeKey.Topic(),
				EventID: evt.ID,
				Token:   token,
			}
			m.Ack()
		},
		nats.DeliverLast(),
		nats.MaxDeliver(2),
	)
	if err != nil {
		return projection.Resume{}, faults.Wrap(err)
	}
	defer sub.Unsubscribe()
	ctx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	var resume projection.Resume
	select {
	case resume = <-ch:
	case <-ctx.Done():
		return projection.Resume{}, faults.New("failed to get subscription to last event ID")
	}
	return resume, nil
}

func (s *Subscriber) RecordLastResume(ctx context.Context, token string) error {
	return s.resumeStore.SetStreamResumeToken(ctx, s.resumeKey, token)
}

func (s *Subscriber) StartConsumer(ctx context.Context, handler projection.EventHandlerFunc, options ...projection.ConsumerOption) error {
	logger := s.logger.WithTags(log.Tags{"topic": s.resumeKey.Topic()})
	opts := projection.ConsumerOptions{
		AckWait: 30 * time.Second,
	}
	for _, v := range options {
		v(&opts)
	}

	var token string
	var startOption nats.SubOpt
	var err error
	token, err = s.resumeStore.GetStreamResumeToken(ctx, s.resumeKey)
	if err != nil && !errors.Is(err, projection.ErrResumeTokenNotFound) {
		return faults.Errorf("Could not retrieve resume token for '%s': %w", s.resumeKey, err)
	}
	if token == "" {
		logger.Infof("Starting consuming all available [token key: '%s']", s.resumeKey)
		// startOption = nats.DeliverAll()
		startOption = nats.StartSequence(1)
	} else {
		logger.Infof("Starting consuming from '%s' [token key: '%s']", token, s.resumeKey)
		seq, err := strconv.ParseUint(token, 10, 64)
		if err != nil {
			return faults.Errorf("unable to parse resume token '%s': %w", token, err)
		}
		startOption = nats.StartSequence(seq + 1) // after seq
	}

	callback := func(m *nats.Msg) {
		evt, err := s.messageCodec.Decode(m.Data)
		if err != nil {
			logger.WithError(err).Errorf("unable to unmarshal event '%s'", string(m.Data))
			m.Nak()
			return
		}
		if opts.Filter == nil || opts.Filter(evt) {
			logger.Debugf("Handling received event '%+v'", evt)
			err = handler(ctx, evt)
			if err != nil {
				logger.WithError(err).Errorf("Error when handling event with ID '%s'", evt.ID)
				m.Nak()
				return
			}
		}

		seq := sequence(m)
		if err := m.Ack(); err != nil {
			logger.WithError(err).Errorf("failed to ACK seq=%d, event=%+v", seq, evt)
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
			s.StopConsumer(context.Background(), false)
		}
	}()
	return nil
}

func (s *Subscriber) StopConsumer(ctx context.Context, hard bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.done == nil {
		return
	}
	if hard {
		err := s.subscription.Unsubscribe()
		if err != nil {
			s.logger.WithError(err).Warnf("Failed to unsubscribe from '%s'", s.resumeKey.Topic())
		} else {
			s.logger.Infof("Unsubscribed from '%s'", s.resumeKey.Topic())
		}
	}
	s.subscription = nil
	close(s.done)
	s.done = nil
}

type Option func(*ReactorSubscriber)

func WithMessageCodec(codec sink.Codec) Option {
	return func(r *ReactorSubscriber) {
		r.messageCodec = codec
	}
}

var _ projection.Consumer = (*ReactorSubscriber)(nil)

type ReactorSubscriber struct {
	logger       log.Logger
	jetStream    nats.JetStreamContext
	resumeKey    projection.ResumeKey
	messageCodec sink.Codec

	mu   sync.RWMutex
	done chan struct{}
}

func NewReactorSubscriber(
	logger log.Logger,
	stream nats.JetStreamContext,
	resumeKey projection.ResumeKey,
	options ...Option,
) *ReactorSubscriber {
	s := &ReactorSubscriber{
		logger:       logger,
		jetStream:    stream,
		resumeKey:    resumeKey,
		messageCodec: sink.JsonCodec{},
	}

	for _, o := range options {
		o(s)
	}

	return s
}

func (s *ReactorSubscriber) StartConsumer(ctx context.Context, handler projection.EventHandlerFunc, options ...projection.ConsumerOption) error {
	logger := s.logger.WithTags(log.Tags{"topic": s.resumeKey.Topic()})
	opts := projection.ConsumerOptions{
		AckWait: 30 * time.Second,
	}
	for _, v := range options {
		v(&opts)
	}

	groupName := s.resumeKey.String()
	_, err := s.jetStream.QueueSubscribe(
		s.resumeKey.Topic().String(),
		groupName,
		func(m *nats.Msg) {
			evt, err := s.messageCodec.Decode(m.Data)
			if err != nil {
				logger.WithError(err).Errorf("unable to unmarshal event '%s'", string(m.Data))
				m.Nak()
				return
			}
			if opts.Filter == nil || opts.Filter(evt) {
				logger.Debugf("Handling received event '%+v'", evt)
				err = handler(ctx, evt)
				if err != nil {
					logger.WithError(err).Errorf("Error when handling event with ID '%s'", evt.ID)
					m.Nak()
					return
				}
			}

			if err := m.Ack(); err != nil {
				logger.WithError(err).Errorf("failed to ACK seq=%d, event=%+v", sequence(m), evt)
				return
			}
		},
		nats.DeliverAll(),
		nats.Durable(groupName),
		nats.MaxAckPending(1),
		nats.AckExplicit(),
		nats.AckWait(opts.AckWait),
	)
	if err != nil {
		return faults.Errorf("failed to subscribe topic=%s, queue=%s, durable name=%s: %w", s.resumeKey.Topic(), groupName, groupName, err)
	}

	done := make(chan struct{})
	s.mu.Lock()
	s.done = done
	s.mu.Unlock()
	go func() {
		select {
		case <-done:
		case <-ctx.Done():
			s.StopConsumer(context.Background(), false)
		}
	}()

	return nil
}

func (s *ReactorSubscriber) StopConsumer(ctx context.Context, hard bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.done == nil {
		return
	}
	close(s.done)
	s.done = nil
}

type NotificationOption func(*NotificationListener)

func WithNotificationMessageCodec(codec sink.Codec) NotificationOption {
	return func(r *NotificationListener) {
		r.messageCodec = codec
	}
}

func WithCancelTimeout(timeout time.Duration) NotificationOption {
	return func(r *NotificationListener) {
		r.cancelTimeout = timeout
	}
}

type NotificationListener struct {
	logger        log.Logger
	queue         *nats.Conn
	managerTopic  string
	messageCodec  sink.Codec
	cancelID      string
	cancelTimeout time.Duration
}

func NewNotificationListener(
	ctx context.Context,
	logger log.Logger,
	url string,
	managerTopic string,
	options ...NotificationOption,
) (*NotificationListener, error) {

	nc, err := nats.Connect(url)
	if err != nil {
		return nil, faults.Errorf("Could not instantiate Nats connection: %w", err)
	}

	go func() {
		<-ctx.Done()
		nc.Close()
	}()

	return NewNotificationListenerWithConn(ctx, logger, nc, managerTopic, options...)
}

func NewNotificationListenerWithConn(
	ctx context.Context,
	logger log.Logger,
	conn *nats.Conn,
	managerTopic string,
	options ...NotificationOption,
) (*NotificationListener, error) {
	cancelID := shortid.MustGenerate()
	logger = logger.WithTags(log.Tags{
		"id":    cancelID,
		"topic": managerTopic,
	})
	s := &NotificationListener{
		logger:        logger,
		queue:         conn,
		managerTopic:  managerTopic,
		messageCodec:  sink.JsonCodec{},
		cancelID:      cancelID,
		cancelTimeout: 5 * time.Second,
	}

	for _, o := range options {
		o(s)
	}

	return s, nil
}

func (s NotificationListener) ListenCancel(ctx context.Context, canceller projection.Canceller) error {
	s.logger.Info("listening for cancel")
	sub, err := s.queue.Subscribe(
		s.managerTopic,
		func(msg *nats.Msg) {
			n := projection.Notification{}
			err := json.Unmarshal(msg.Data, &n)
			if err != nil {
				s.logger.Errorf("unable to unmarshal: %+v", faults.Wrap(err))
				return
			}

			switch n.Action {
			case projection.Release:
				canceller.Cancel(context.Background(), true)
				err = msg.Respond([]byte(s.cancelID))
				if err != nil {
					s.logger.Errorf("unable to publish cancel reply to '%s': %+v", s.managerTopic, faults.Wrap(err))
				}
			default:
				s.logger.WithTags(log.Tags{"notification": n}).Error("Unknown notification")
			}
		})
	if err != nil {
		return faults.Wrap(err)
	}

	go func() {
		<-ctx.Done()
		s.logger.Info("unsubscribing cancel listener")
		sub.Unsubscribe()
	}()

	return nil
}

func (s NotificationListener) PublishCancel(ctx context.Context, projectionName string, listenerCount int) error {
	logger := s.logger.WithTags(log.Tags{"projection": projectionName})
	logger.Info("Stopping projection")

	payload, err := json.Marshal(projection.Notification{
		Projection: projectionName,
		Action:     projection.Release,
	})
	if err != nil {
		return faults.Wrap(err)
	}

	replyTo := s.managerTopic + "-reply"
	sub, err := s.queue.SubscribeSync(replyTo)
	if err != nil {
		return faults.Wrap(err)
	}
	defer sub.Unsubscribe()

	err = s.queue.Flush()
	if err != nil {
		return faults.Wrap(err)
	}

	// Send the request
	err = s.queue.PublishRequest(s.managerTopic, replyTo, []byte(payload))
	if err != nil {
		return faults.Wrap(err)
	}

	// Wait for all responses
	max := s.cancelTimeout
	start := time.Now()
	replies := map[string]struct{}{}
	since := time.Since(start)
	for since < max {
		remainingTimeout := max - since
		msg, err := sub.NextMsg(remainingTimeout)
		if err != nil {
			return faults.Wrap(err)
		}
		m := string(msg.Data)
		logger.Infof("stop projection acknowledgement. CancelID='%s'", m)
		replies[m] = struct{}{}

		if len(replies) == listenerCount {
			return nil
		}
	}

	return projection.ErrCancelProjectionTimeout
}