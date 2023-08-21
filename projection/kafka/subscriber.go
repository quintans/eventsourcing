package kafka

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/IBM/sarama"
	"github.com/teris-io/shortid"

	"github.com/quintans/faults"

	"github.com/quintans/eventsourcing/log"
	"github.com/quintans/eventsourcing/projection"
	"github.com/quintans/eventsourcing/sink"
	"github.com/quintans/eventsourcing/util"
)

func NewSubscriberWithBrokers(
	ctx context.Context,
	logger log.Logger,
	brokers []string,
	topic string,
) (*Subscriber, error) {
	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true
	client, err := sarama.NewClient(brokers, config)
	if err != nil {
		return nil, faults.Errorf("instantiating Kafka client: %w", err)
	}
	subscriber, err := NewSubscriberWithClient(
		logger,
		client,
		topic,
	)
	if err != nil {
		return nil, faults.Wrap(err)
	}
	go func() {
		<-ctx.Done()
		client.Close()
	}()

	return subscriber, nil
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
	client       sarama.Client
	topic        string
	partition    []uint32
	messageCodec sink.Codec
}

func NewSubscriberWithClient(
	logger log.Logger,
	client sarama.Client,
	topic string,
	options ...SubOption,
) (*Subscriber, error) {
	if topic == "" {
		return nil, faults.New("empty topic provided")
	}

	partitions, err := client.Partitions(topic)
	if err != nil {
		return nil, faults.Errorf("getting partitions: %w", err)
	}

	s := &Subscriber{
		logger:       logger,
		client:       client,
		topic:        topic,
		partition:    partitionsUint32(partitions),
		messageCodec: sink.JSONCodec{},
	}
	s.logger = logger.WithTags(log.Tags{
		"id": "subscriber-" + shortid.MustGenerate(),
	})

	for _, o := range options {
		o(s)
	}

	return s, nil
}

func partitionsUint32(p []int32) []uint32 {
	out := make([]uint32, len(p))
	for k, v := range p {
		out[k] = uint32(v)
	}
	return out
}

func (s *Subscriber) TopicPartitions() (string, []uint32) {
	return s.topic, s.partition
}

func (s *Subscriber) StartConsumer(ctx context.Context, proj projection.Projection, options ...projection.ConsumerOption) error {
	opts := projection.ConsumerOptions{
		AckWait: 30 * time.Second,
	}
	for _, v := range options {
		v(&opts)
	}

	groupName := fmt.Sprintf("%s:%s", s.topic, proj.Name())
	group, err := sarama.NewConsumerGroupFromClient(groupName, s.client)
	if err != nil {
		return faults.Errorf("creating consumer group '%s': %w", groupName, err)
	}
	defer group.Close()

	logger := s.logger.WithTags(log.Tags{"topic": s.topic})
	gh := groupHandler{
		logger: logger,
		sub:    s,
		opts:   opts,
		proj:   proj,
	}

	for {
		err = group.Consume(ctx, []string{s.topic}, gh)
		if ctx.Err() != nil {
			return nil
		}
		if errors.Is(err, sarama.ErrClosedConsumerGroup) {
			continue
		}
		if err != nil {
			return faults.Errorf("consuming topic '%s' for group '%s': %w", s.topic, groupName, err)
		}
	}
}

type groupHandler struct {
	logger log.Logger
	sub    *Subscriber
	opts   projection.ConsumerOptions
	proj   projection.Projection
}

func (h groupHandler) Setup(sess sarama.ConsumerGroupSession) error {
	claims := sess.Claims()

	for t, parts := range claims {
		for _, part := range parts {
			topic, err := util.NewPartitionedTopic(t, uint32(part))
			if err != nil {
				return err
			}

			resume, err := projection.NewResume(topic, h.proj.Name())
			if err != nil {
				return faults.Wrap(err)
			}

			token, err := h.proj.GetStreamResumeToken(context.Background(), resume)
			if err != nil && !errors.Is(err, projection.ErrResumeTokenNotFound) {
				return faults.Errorf("Could not retrieve resume token for '%s': %w", h.sub.topic, err)
			}

			var startOffset int64
			if token.IsEmpty() {
				h.logger.WithTags(log.Tags{"topic": h.sub.topic}).Info("Starting consuming all available events", topic)
				startOffset = sarama.OffsetOldest
			} else {
				h.logger.WithTags(log.Tags{"from": token.Sequence(), "topic": topic}).Info("Starting consumer")
				startOffset = int64(token.Sequence() + 1) // after seq
			}

			sess.ResetOffset(t, part, startOffset, "start")
		}
	}

	return nil
}

func (groupHandler) Cleanup(_ sarama.ConsumerGroupSession) error {
	return nil
}

func (h groupHandler) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		evt, er := h.sub.messageCodec.Decode(msg.Value)
		if er != nil {
			return faults.Errorf("unmarshal event '%s': %w", string(msg.Value), er)
		}
		if h.opts.Filter == nil || h.opts.Filter(evt) {
			h.logger.Debugf("Handling received event '%+v'", evt)
			er = h.proj.Handle(
				context.Background(),
				projection.MetaData{
					Topic:     msg.Topic,
					Partition: uint32(msg.Partition),
					Token:     projection.NewToken(projection.ConsumerToken, uint64(msg.Offset)),
				},
				evt,
			)
			if er != nil {
				return faults.Errorf("handling event with ID '%s': %w", evt.ID, er)
			}
		}
	}

	return nil
}
