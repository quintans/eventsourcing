package subscriber

import (
	"context"
	"encoding/json"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/stan.go"
	"github.com/quintans/eventstore/common"
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
	queue        *nats.Conn
	stream       stan.Conn
	topic        string
	managerTopic string
	messageCodec sink.Codec
}

func NewNatsSubscriber(
	ctx context.Context,
	addresses string,
	stanClusterID,
	clientIDPrefix string,
	topic,
	managerTopic string,
	options ...Option,
) (*NatsSubscriber, error) {
	nc, err := nats.Connect(addresses)
	if err != nil {
		return nil, faults.Errorf("Could not instantiate NATS client: %w", err)
	}

	// since we are not using durable subscriptions we randomize the client
	clientID := clientIDPrefix + "-" + uuid.New().String()

	stream, err := stan.Connect(stanClusterID, clientID, stan.NatsURL(addresses))
	if err != nil {
		return nil, faults.Errorf("Could not instantiate NATS stream connection: %w", err)
	}

	go func() {
		<-ctx.Done()
		nc.Close()
		stream.Close()
	}()

	s := &NatsSubscriber{
		queue:        nc,
		stream:       stream,
		topic:        topic,
		managerTopic: managerTopic,
		messageCodec: sink.JsonCodec{},
	}

	for _, o := range options {
		o(s)
	}

	return s, nil
}

func (s NatsSubscriber) GetQueue() *nats.Conn {
	return s.queue
}

func (s NatsSubscriber) GetStream() stan.Conn {
	return s.stream
}

func (s NatsSubscriber) GetResumeToken(ctx context.Context, partition uint32) (string, error) {
	ch := make(chan uint64)
	topic := common.TopicWithPartition(s.topic, partition)
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

func (s NatsSubscriber) StartConsumer(ctx context.Context, partition uint32, resumeToken string, projection projection.Projection, aggregateTypes []string) (chan struct{}, error) {
	logger := log.WithField("partition", partition)
	start := stan.DeliverAllAvailable()
	var seq uint64
	if resumeToken != "" {
		var err error
		seq, err = strconv.ParseUint(resumeToken, 10, 64)
		if err != nil {
			return nil, faults.Errorf("unable to parse resume token %s: %w", resumeToken, err)
		}
		start = stan.StartAtSequence(seq)
	}
	topic := common.TopicWithPartition(s.topic, partition)
	logger = logger.WithField("topic", topic)
	sub, err := s.stream.Subscribe(topic, func(m *stan.Msg) {
		if seq >= m.Sequence {
			// ignore seq
			return
		}
		e, err := s.messageCodec.Decode(m.Data)
		if err != nil {
			logger.WithError(err).Errorf("unable to unmarshal event '%s'", string(m.Data))
		}
		if !in(e.AggregateType, aggregateTypes...) {
			// ignore
			return
		}
		logger.Debugf("Handling received event '%+v'", e)
		err = projection.Handler(ctx, e)
		if err != nil {
			logger.WithError(err).Errorf("Error when handling event with ID '%s'", e.ID)
		}
	}, start, stan.MaxInflight(1))
	if err != nil {
		return nil, faults.Wrap(err)
	}

	stopped := make(chan struct{})
	go func() {
		<-ctx.Done()
		sub.Close()
		close(stopped)
		logger.Infof("Stoping handling events for %s", projection.GetName())
	}()

	return stopped, nil
}

func in(test string, values ...string) bool {
	for _, v := range values {
		if v == test {
			return true
		}
	}
	return false
}

func (s NatsSubscriber) ListenCancelProjection(ctx context.Context, canceller projection.Canceller) error {
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

func (s NatsSubscriber) CancelProjection(ctx context.Context, projectionName string, partitions int) error {
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
	max := 500 * time.Millisecond
	start := time.Now()
	count := 0
	for time.Now().Sub(start) < max {
		_, err = sub.NextMsg(1 * time.Second)
		if err != nil {
			break
		}

		count++
		if count >= partitions {
			break
		}
	}
	sub.Unsubscribe()

	return nil
}
