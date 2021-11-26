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

type Option func(*NatsSubscriber)

func WithMessageCodec(codec sink.Codec) Option {
	return func(r *NatsSubscriber) {
		r.messageCodec = codec
	}
}

type NatsSubscriber struct {
	logger       log.Logger
	stream       stan.Conn
	topicResumer projection.StreamResumer
	messageCodec sink.Codec
}

func NewNatsSubscriber(
	ctx context.Context,
	logger log.Logger,
	addresses string,
	stanClusterID,
	clientID string,
	topicResumer projection.StreamResumer,
	options ...Option,
) (*NatsSubscriber, error) {
	stream, err := stan.Connect(stanClusterID, clientID, stan.NatsURL(addresses))
	if err != nil {
		return nil, faults.Errorf("Could not instantiate NATS stream connection: %w", err)
	}

	go func() {
		<-ctx.Done()
		stream.Close()
	}()

	s := &NatsSubscriber{
		logger:       logger,
		stream:       stream,
		topicResumer: topicResumer,
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

func (s NatsSubscriber) GetResumeToken(ctx context.Context, topic string) (string, error) {
	ch := make(chan uint64)
	sub, err := s.stream.Subscribe(topic, func(m *stan.Msg) {
		ch <- m.Sequence
	}, stan.StartWithLastReceived())
	if err != nil {
		return "", faults.Wrap(err)
	}
	defer sub.Close()
	ctx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()
	var sequence uint64
	select {
	case sequence = <-ch:
	case <-ctx.Done():
		return "", nil
	}
	return strconv.FormatUint(sequence, 10), nil
}

func (s NatsSubscriber) StartConsumer(ctx context.Context, resume projection.StreamResume, handler projection.EventHandlerFunc, options ...projection.ConsumerOption) (chan struct{}, error) {
	logger := s.logger.WithTags(log.Tags{"topic": resume.Topic})
	opts := projection.ConsumerOptions{}
	for _, v := range options {
		v(&opts)
	}
	var token string
	// if no token is found start form the last one
	start := stan.StartWithLastReceived()
	var key string
	saveResume := make(chan uint64, 100)
	if resume.Stream != "" {
		key = resume.String()
		var err error
		token, err = s.topicResumer.GetStreamResumeToken(ctx, key)
		if err != nil {
			return nil, faults.Errorf("Could not retrieve resume token for %s: %w", key, err)
		}
		if token == "" {
			logger.Infof("Starting consuming all available [token key: %s]", key)
			start = stan.DeliverAllAvailable()
		} else {
			logger.Infof("Starting consuming from %s [token key: %s]", token, key)
			seq, err := strconv.ParseUint(token, 10, 64)
			if err != nil {
				return nil, faults.Errorf("unable to parse resume token %s: %w", token, err)
			}
			start = stan.StartAtSequence(seq)
		}
	} else {
		logger.Info("Starting consuming from the last received")
	}

	sub, err := s.stream.Subscribe(resume.Topic, func(m *stan.Msg) {
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
		stan.AckWait(30*time.Second), // TODO: make it configurable
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
		for seq := range saveResume {
			err := s.topicResumer.SetStreamResumeToken(ctx, key, strconv.FormatUint(seq, 10))
			if err != nil {
				logger.WithError(err).Errorf("Failed to set the resume token for %s", key)
			}
		}
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
	addresses string,
	managerTopic string,
	options ...ProjectionOption,
) (*NatsProjectionSubscriber, error) {
	nc, err := nats.Connect(addresses)
	if err != nil {
		return nil, faults.Errorf("Could not instantiate NATS client: %w", err)
	}

	go func() {
		<-ctx.Done()
		nc.Close()
	}()

	s := &NatsProjectionSubscriber{
		logger:        logger,
		queue:         nc,
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
	sub, err := s.queue.Subscribe(s.managerTopic, func(msg *nats.Msg) {
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
