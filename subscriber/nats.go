package subscriber

import (
	"context"
	"encoding/json"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/stan.go"
	"github.com/quintans/faults"

	"github.com/quintans/eventsourcing/log"
	"github.com/quintans/eventsourcing/projection"
	"github.com/quintans/eventsourcing/sink"
)

type Option func(*NatsSubscriberWithResumer)

func WithMessageCodec(codec sink.Codec) Option {
	return func(r *NatsSubscriberWithResumer) {
		r.messageCodec = codec
	}
}

var _ projection.Subscriber = (*NatsSubscriberWithResumer)(nil)

type NatsSubscriberWithResumer struct {
	logger       log.Logger
	stream       stan.Conn
	resumerStore projection.StreamResumer
	resumeKey    projection.StreamResume
	messageCodec sink.Codec
}

func NewNatsSubscriberWithResumer(
	ctx context.Context,
	logger log.Logger,
	stream stan.Conn,
	resumerStore projection.StreamResumer,
	resumeKey projection.StreamResume,
	options ...Option,
) (*NatsSubscriberWithResumer, error) {
	go func() {
		<-ctx.Done()
		stream.Close()
	}()

	s := &NatsSubscriberWithResumer{
		logger:       logger,
		stream:       stream,
		resumerStore: resumerStore,
		resumeKey:    resumeKey,
		messageCodec: sink.JsonCodec{},
	}

	for _, o := range options {
		o(s)
	}

	return s, nil
}

func (s NatsSubscriberWithResumer) GetStream() stan.Conn {
	return s.stream
}

func (s NatsSubscriberWithResumer) MoveToLastPosition(ctx context.Context) error {
	ch := make(chan uint64)
	// this will position the stream at the last position+1
	sub, err := s.stream.Subscribe(
		s.resumeKey.Topic(),
		func(m *stan.Msg) {
			ch <- m.Sequence
		},
		stan.StartWithLastReceived(),
	)
	if err != nil {
		return faults.Wrap(err)
	}
	defer sub.Close()
	ctx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()
	var sequence uint64
	select {
	case sequence = <-ch:
	case <-ctx.Done():
		return nil
	}
	token := strconv.FormatUint(sequence, 10)
	return s.resumerStore.SetStreamResumeToken(ctx, s.resumeKey, token)
}

func (s NatsSubscriberWithResumer) StartConsumer(ctx context.Context, handler projection.EventHandlerFunc, options ...projection.ConsumerOption) (chan struct{}, error) {
	logger := s.logger.WithTags(log.Tags{"topic": s.resumeKey.Topic()})
	opts := projection.ConsumerOptions{
		AckWait: 30 * time.Second,
	}
	for _, v := range options {
		v(&opts)
	}
	var token string
	var start stan.SubscriptionOption
	saveResume := make(chan uint64, 100)
	var err error
	token, err = s.resumerStore.GetStreamResumeToken(ctx, s.resumeKey)
	if err != nil {
		return nil, faults.Errorf("Could not retrieve resume token for %s: %w", s.resumeKey, err)
	}
	if token == "" {
		logger.Infof("Starting consuming all available [token key: %s]", s.resumeKey)
		start = stan.DeliverAllAvailable()
	} else {
		logger.Infof("Starting consuming from %s [token key: %s]", token, s.resumeKey)
		seq, err := strconv.ParseUint(token, 10, 64)
		if err != nil {
			return nil, faults.Errorf("unable to parse resume token %s: %w", token, err)
		}
		start = stan.StartAtSequence(seq)
	}

	sub, err := s.stream.Subscribe(
		s.resumeKey.Topic(),
		func(m *stan.Msg) {
			evt, err := s.messageCodec.Decode(m.Data)
			if err != nil {
				logger.WithError(err).Errorf("unable to unmarshal event '%s'", string(m.Data))
				return
			}
			if opts.Filter(evt) {
				logger.Debugf("Handling received event '%+v'", evt)
				err = handler(ctx, evt)
				if err != nil {
					logger.WithError(err).Errorf("Error when handling event with ID '%s'", evt.ID)
					return
				}
			}

			if err := m.Ack(); err != nil {
				logger.WithError(err).Errorf("failed to ACK msg: %d", m.Sequence)
				return
			}

			saveResume <- m.Sequence
		},
		start,
		stan.MaxInflight(1),
		stan.SetManualAckMode(),
		stan.AckWait(opts.AckWait),
	)
	if err != nil {
		return nil, faults.Wrap(err)
	}

	stopped := make(chan struct{})
	go func() {
		<-ctx.Done()
		sub.Close()
		close(stopped)
		close(saveResume)
	}()

	go func() {
		// should we really care if they are store in sequence?
		for seq := range saveResume {
			err := s.resumerStore.SetStreamResumeToken(ctx, s.resumeKey, strconv.FormatUint(seq, 10))
			if err != nil {
				logger.WithError(err).Errorf("Failed to set the resume token for %s", s.resumeKey)
			}
		}
	}()

	return stopped, nil
}

var _ projection.Subscriber = (*NatsSubscriber)(nil)

type NatsSubscriber struct {
	logger       log.Logger
	stream       stan.Conn
	resumeKey    projection.StreamResume
	messageCodec sink.Codec
}

func NewNatsSubscriber(
	ctx context.Context,
	logger log.Logger,
	stream stan.Conn,
	resumeKey projection.StreamResume,
	options ...Option,
) (*NatsSubscriberWithResumer, error) {
	go func() {
		<-ctx.Done()
		stream.Close()
	}()

	s := &NatsSubscriberWithResumer{
		logger:       logger,
		stream:       stream,
		resumeKey:    resumeKey,
		messageCodec: sink.JsonCodec{},
	}

	for _, o := range options {
		o(s)
	}

	return s, nil
}

func (s NatsSubscriber) GetStream() stan.Conn {
	return s.stream
}

func (s NatsSubscriber) MoveToLastPosition(ctx context.Context) error {
	ch := make(chan uint64)
	groupName := s.resumeKey.String()
	// this will position the stream at the last position+1
	sub, err := s.stream.QueueSubscribe(
		s.resumeKey.Topic(),
		groupName,
		func(m *stan.Msg) {
			ch <- m.Sequence
		},
		stan.DurableName(groupName),
		stan.StartWithLastReceived(),
	)
	if err != nil {
		return faults.Wrap(err)
	}
	defer sub.Close()
	ctx, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
	defer cancel()
	select {
	case <-ch:
	case <-ctx.Done():
		return faults.New("failed to move subscription to last position")
	}
	return nil
}

func (s NatsSubscriber) StartConsumer(ctx context.Context, handler projection.EventHandlerFunc, options ...projection.ConsumerOption) (chan struct{}, error) {
	logger := s.logger.WithTags(log.Tags{"topic": s.resumeKey.Topic()})
	opts := projection.ConsumerOptions{
		AckWait: 30 * time.Second,
	}
	for _, v := range options {
		v(&opts)
	}

	groupName := s.resumeKey.String()
	sub, err := s.stream.QueueSubscribe(
		s.resumeKey.Topic(),
		groupName,
		func(m *stan.Msg) {
			evt, err := s.messageCodec.Decode(m.Data)
			if err != nil {
				logger.WithError(err).Errorf("unable to unmarshal event '%s'", string(m.Data))
				return
			}
			if opts.Filter(evt) {
				logger.Debugf("Handling received event '%+v'", evt)
				err = handler(ctx, evt)
				if err != nil {
					logger.WithError(err).Errorf("Error when handling event with ID '%s'", evt.ID)
					return
				}
			}

			if err := m.Ack(); err != nil {
				logger.WithError(err).Errorf("failed to ACK msg: %d", m.Sequence)
				return
			}
		},
		stan.DurableName(groupName),
		stan.MaxInflight(1),
		stan.SetManualAckMode(),
		stan.AckWait(opts.AckWait),
	)
	if err != nil {
		return nil, faults.Wrap(err)
	}

	stopped := make(chan struct{})
	go func() {
		<-ctx.Done()
		sub.Close()
		close(stopped)
	}()

	return stopped, nil
}

type ProjectionOption func(*NatsProjectionSubscriber)

func WithProjectionMessageCodec(codec sink.Codec) ProjectionOption {
	return func(r *NatsProjectionSubscriber) {
		r.messageCodec = codec
	}
}

func WithCancelTimeout(timeout time.Duration) ProjectionOption {
	return func(r *NatsProjectionSubscriber) {
		r.cancelTimeout = timeout
	}
}

type NatsProjectionSubscriber struct {
	logger        log.Logger
	queue         *nats.Conn
	managerTopic  string
	messageCodec  sink.Codec
	cancelID      string
	cancelTimeout time.Duration
}

func NewNatsProjectionSubscriber(
	ctx context.Context,
	logger log.Logger,
	queue *nats.Conn,
	managerTopic string,
	options ...ProjectionOption,
) (*NatsProjectionSubscriber, error) {
	s := &NatsProjectionSubscriber{
		logger:        logger,
		queue:         queue,
		managerTopic:  managerTopic,
		messageCodec:  sink.JsonCodec{},
		cancelID:      uuid.New().String(),
		cancelTimeout: 5 * time.Second,
	}

	for _, o := range options {
		o(s)
	}

	return s, nil
}

func (s NatsProjectionSubscriber) GetQueue() *nats.Conn {
	return s.queue
}

func (s NatsProjectionSubscriber) ListenCancel(ctx context.Context, canceller projection.Canceller) error {
	logger := s.logger.WithTags(log.Tags{"topic": s.managerTopic})
	sub, err := s.queue.Subscribe(
		s.managerTopic,
		func(msg *nats.Msg) {
			n := projection.Notification{}
			err := json.Unmarshal(msg.Data, &n)
			if err != nil {
				logger.Errorf("unable to unmarshal: %+v", faults.Wrap(err))
				return
			}
			if n.Projection != canceller.Name() {
				return
			}

			switch n.Action {
			case projection.Release:
				canceller.Cancel()
				err = msg.Respond([]byte(s.cancelID))
				if err != nil {
					logger.Errorf("unable to publish cancel reply to '%s': %+v", s.managerTopic, faults.Wrap(err))
				}
			default:
				logger.WithTags(log.Tags{"notification": n}).Error("Unknown notification")
			}
		})
	if err != nil {
		return faults.Wrap(err)
	}

	go func() {
		<-ctx.Done()
		sub.Unsubscribe()
	}()

	return nil
}

func (s NatsProjectionSubscriber) PublishCancel(ctx context.Context, projectionName string, listenerCount int) error {
	s.logger.WithTags(log.Tags{"projection": projectionName}).Info("Cancelling projection")

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
		replies[string(msg.Data)] = struct{}{}

		if len(replies) == listenerCount {
			return nil
		}
	}

	return projection.ErrCancelProjectionTimeout
}
