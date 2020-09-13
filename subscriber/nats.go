package subscriber

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/stan.go"
	"github.com/quintans/eventstore"
	"github.com/quintans/eventstore/projection"
	"github.com/quintans/eventstore/sink"
	log "github.com/sirupsen/logrus"
)

type NatsSubscriber struct {
	queue        *nats.Conn
	stream       stan.Conn
	topic        string
	managerTopic string
}

func NewNatsSubscriber(ctx context.Context, addresses string, stanClusterID, clientID string, topic, managerTopic string) (*NatsSubscriber, error) {
	nc, err := nats.Connect(addresses)
	if err != nil {
		return nil, fmt.Errorf("Could not instantiate NATS client: %w", err)
	}

	stream, err := stan.Connect(stanClusterID, clientID, stan.NatsURL(addresses))
	if err != nil {
		return nil, fmt.Errorf("Could not instantiate NATS stream connection: %w", err)
	}

	go func() {
		<-ctx.Done()
		nc.Close()
		stream.Close()
	}()

	return &NatsSubscriber{
		queue:        nc,
		stream:       stream,
		topic:        topic,
		managerTopic: managerTopic,
	}, nil
}

func (s NatsSubscriber) GetQueue() *nats.Conn {
	return s.queue
}

func (s NatsSubscriber) GetStream() stan.Conn {
	return s.stream
}

func (s NatsSubscriber) GetResumeToken(ctx context.Context, partition int) (string, error) {
	ch := make(chan uint64)
	topic := sink.TopicWithPartition(s.topic, partition)
	sub, err := s.stream.Subscribe(topic, func(m *stan.Msg) {
		ch <- m.Sequence
	}, stan.StartWithLastReceived())
	if err != nil {
		return "", err
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

func (s NatsSubscriber) StartConsumer(ctx context.Context, partition int, resumeToken string, projection projection.Projection) (chan struct{}, error) {
	start := stan.DeliverAllAvailable()
	var seq uint64
	if resumeToken != "" {
		var err error
		seq, err = strconv.ParseUint(resumeToken, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("Unable to parse resume token %s: %w", resumeToken, err)
		}
		start = stan.StartAtSequence(seq)
	}
	topic := sink.TopicWithPartition(s.topic, partition)
	sub, err := s.stream.Subscribe(topic, func(m *stan.Msg) {
		if seq >= m.Sequence {
			// ignore seq
			return
		}
		e := eventstore.Event{}
		err := json.Unmarshal(m.Data, &e)
		if err != nil {
			log.WithError(err).Errorf("Unable to unmarshal event '%s'", string(m.Data))
		}
		if !in(e.AggregateType, projection.GetAggregateTypes()...) {
			// ignore
			return
		}
		err = projection.Handler(ctx, e)
		if err != nil {
			log.WithError(err).Errorf("Error when handling event with ID '%s'", e.ID)
		}
	}, start, stan.MaxInflight(1))
	if err != nil {
		return nil, err
	}

	stopped := make(chan struct{})
	go func() {
		<-ctx.Done()
		sub.Close()
		close(stopped)
		log.Infof("Stoping handling events for %s", projection.GetName())
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

func (s NatsSubscriber) StartNotifier(ctx context.Context, freezer projection.Freezer) error {
	sub, err := s.queue.Subscribe(s.managerTopic, func(msg *nats.Msg) {
		n := projection.Notification{}
		err := json.Unmarshal(msg.Data, &n)
		if err != nil {
			log.Errorf("Unable to unmarshal %v", err)
			return
		}
		if n.Projection != freezer.Name() {
			return
		}

		switch n.Action {
		case projection.Freeze:
			if freezer.Freeze() {
				err := msg.Respond([]byte("..."))
				if err != nil {
					log.WithError(err).Error("Unable to respond to notification")
				}
			}
		case projection.Unfreeze:
			freezer.Unfreeze()
		default:
			log.WithField("notification", n).Error("Unknown notification")
		}

	})
	if err != nil {
		return err
	}

	go func() {
		<-ctx.Done()
		sub.Unsubscribe()
	}()

	return nil
}

func (s NatsSubscriber) FreezeProjection(ctx context.Context, projectionName string) error {
	log.WithField("projection", projectionName).Info("Freezing projection")
	payload, err := json.Marshal(projection.Notification{
		Projection: projectionName,
		Action:     projection.Freeze,
	})
	if err != nil {
		return err
	}
	_, err = s.queue.Request(s.managerTopic, payload, 500*time.Millisecond)
	return err
}

func (s NatsSubscriber) UnfreezeProjection(ctx context.Context, projectionName string) error {
	log.WithField("projection", projectionName).Info("Unfreezing projection")
	payload, err := json.Marshal(projection.Notification{
		Projection: projectionName,
		Action:     projection.Unfreeze,
	})
	if err != nil {
		return err
	}
	return s.queue.Publish(s.managerTopic, payload)
}
