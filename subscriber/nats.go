package subscriber

import (
	"context"
	"encoding/json"
	"strconv"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/stan.go"
	"github.com/quintans/eventstore/projection"
	"github.com/quintans/eventstore/sink"
	"github.com/quintans/faults"
	log "github.com/sirupsen/logrus"
)

type Option func(*NatsSubscriber)

func WithMessageCodec(codec sink.Codec) Option {
	return func(r *NatsSubscriber) {
		r.messageCodec = codec
	}
}

type NatsSubscriber struct {
	stream       stan.Conn
	topicResumer projection.StreamResumer
	messageCodec sink.Codec
}

func NewNatsSubscriber(
	ctx context.Context,
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
	ctx, _ = context.WithTimeout(ctx, 100*time.Millisecond)
	var sequence uint64
	select {
	case sequence = <-ch:
	case <-ctx.Done():
		return "", nil
	}
	return strconv.FormatUint(sequence, 10), nil
}

func (s NatsSubscriber) StartConsumer(ctx context.Context, resume projection.StreamResume, handler projection.EventHandlerFunc, options ...projection.ConsumerOption) (chan struct{}, error) {
	logger := log.WithField("topic", resume.Topic)
	opts := projection.ConsumerOptions{}
	for _, v := range options {
		v(&opts)
	}
	var token string
	// if no token is found start form the last one
	start := stan.StartWithLastReceived()
	var key string
	var resumers chan uint64
	if resume.Stream != "" {
		key = resume.String()
		resumers = make(chan uint64, 100)
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
		if !opts.Filter(evt) {
			// ignore
			if err = m.Ack(); err != nil {
				logger.WithError(err).Errorf("failed to ACK ignored msg: %d", m.Sequence)
				return
			}
			if resumers != nil {
				resumers <- m.Sequence
			}
			return
		}
		logger.Debugf("Handling received event '%+v'", evt)
		err = handler(ctx, evt)
		if err != nil {
			logger.WithError(err).Errorf("Error when handling event with ID '%s'", evt.ID)
			return
		}
		if err := m.Ack(); err != nil {
			logger.WithError(err).Errorf("failed to ACK msg: %d", m.Sequence)
			return
		}

		if resumers != nil {
			resumers <- m.Sequence
		}
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
		close(resumers)
		sub.Close()
		close(stopped)
	}()

	if resumers != nil {
		go func() {
			for seq := range resumers {
				err := s.topicResumer.SetStreamResumeToken(ctx, key, strconv.FormatUint(seq, 10))
				if err != nil {
					logger.WithError(err).Errorf("Failed to set the resume token for %s", key)
				}
			}
		}()
	}

	return stopped, nil
}

type ProjectionOption func(*NatsProjectionSubscriber)

func WithProjectionMessageCodec(codec sink.Codec) ProjectionOption {
	return func(r *NatsProjectionSubscriber) {
		r.messageCodec = codec
	}
}

type NatsProjectionSubscriber struct {
	queue        *nats.Conn
	managerTopic string
	messageCodec sink.Codec
}

func NewNatsProjectionSubscriber(
	ctx context.Context,
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
		queue:        nc,
		managerTopic: managerTopic,
		messageCodec: sink.JsonCodec{},
	}

	for _, o := range options {
		o(s)
	}

	return s, nil
}

func (s NatsProjectionSubscriber) GetQueue() *nats.Conn {
	return s.queue
}

func (s NatsProjectionSubscriber) ListenCancelProjection(ctx context.Context, canceller projection.Canceller) error {
	logger := log.WithField("topic", s.managerTopic)
	sub, err := s.queue.Subscribe(s.managerTopic, func(msg *nats.Msg) {
		n := projection.Notification{}
		err := json.Unmarshal(msg.Data, &n)
		if err != nil {
			logger.Errorf("Unable to unmarshal %v", faults.Wrap(err))
			return
		}
		if n.Projection != canceller.Name() {
			return
		}

		switch n.Action {
		case projection.Release:
			canceller.Cancel()
		default:
			logger.WithField("notification", n).Error("Unknown notification")
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

func (s NatsProjectionSubscriber) CancelProjection(ctx context.Context, projectionName string, listenerCount int) error {
	log.WithField("projection", projectionName).Info("Cancelling projection")

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
	err = s.queue.Flush()
	if err != nil {
		return faults.Wrap(err)
	}

	// Send the request
	err = s.queue.PublishRequest(s.managerTopic, replyTo, []byte(payload))
	if err != nil {
		return faults.Wrap(err)
	}

	// Wait for a single response
	max := time.Second
	start := time.Now()
	count := 0
	for time.Now().Sub(start) < max {
		_, err = sub.NextMsg(1 * time.Second)
		if err != nil {
			break
		}

		count++
		if count >= listenerCount {
			break
		}
	}
	sub.Unsubscribe()

	return nil
}
