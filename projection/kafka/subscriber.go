package kafka

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/IBM/sarama"
	"github.com/teris-io/shortid"

	"github.com/quintans/faults"

	"github.com/quintans/eventsourcing/eventid"
	"github.com/quintans/eventsourcing/log"
	"github.com/quintans/eventsourcing/projection"
	"github.com/quintans/eventsourcing/sink"
	"github.com/quintans/eventsourcing/store"
	"github.com/quintans/eventsourcing/util"
)

func NewSubscriberWithBrokers(
	ctx context.Context,
	logger log.Logger,
	brokers []string,
	topic string,
	resumeStore store.KVStore,
) (*Subscriber, error) {
	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true
	config.Consumer.Offsets.Initial = sarama.OffsetOldest
	client, err := sarama.NewClient(brokers, config)
	if err != nil {
		return nil, faults.Errorf("instantiating Kafka client: %w", err)
	}
	subscriber, err := NewSubscriberWithClient(
		logger,
		client,
		brokers,
		topic,
		resumeStore,
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
		r.codec = codec
	}
}

var _ projection.Consumer = (*Subscriber)(nil)

type Subscriber struct {
	logger      log.Logger
	client      sarama.Client
	brokers     []string
	topic       string
	partitions  []uint32
	codec       sink.Codec
	resumeStore store.KVStore
}

func NewSubscriberWithClient(
	logger log.Logger,
	client sarama.Client,
	brokers []string,
	topic string,
	resumeStore store.KVStore,
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
		resumeStore: resumeStore,
		logger: logger.WithTags(log.Tags{
			"subscriber": "kafka",
			"id":         "subscriber-" + shortid.MustGenerate(),
		}),
		client:     client,
		brokers:    brokers,
		topic:      topic,
		partitions: util.NormalizePartitions(partitions),
		codec:      sink.JSONCodec{},
	}

	for _, o := range options {
		o(s)
	}

	return s, nil
}

func (s *Subscriber) TopicPartitions() (topic string, partitions []uint32) {
	return s.topic, s.partitions
}

func (s *Subscriber) Positions(ctx context.Context) (map[uint32]projection.SubscriberPosition, error) {
	bms := map[uint32]projection.SubscriberPosition{}
	for _, p := range s.partitions {
		seq, eventID, err := s.lastBUSMessage(ctx, int32(p-1))
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

func (s *Subscriber) lastBUSMessage(_ context.Context, partition int32) (_ uint64, _ eventid.EventID, e error) {
	defer faults.Catch(&e, "lastBUSMessage(partition=%d)", partition)
	// get last nextOffset
	nextOffset, err := s.client.GetOffset(s.topic, partition, sarama.OffsetNewest)
	if err != nil {
		return 0, eventid.Zero, faults.Errorf("getting offset: %w", err)
	}
	if nextOffset == 0 {
		return 0, eventid.Zero, nil
	}

	consumer, err := sarama.NewConsumerFromClient(s.client)
	if err != nil {
		return 0, eventid.Zero, faults.Errorf("initializing consumer client: %w", err)
	}
	defer consumer.Close()

	pc, err := consumer.ConsumePartition(s.topic, partition, nextOffset-1)
	if err != nil {
		if errors.Is(err, sarama.ErrOffsetOutOfRange) {
			return 0, eventid.Zero, nil
		}
		return 0, eventid.Zero, faults.Errorf("getting last message: %w", err)
	}

	select {
	case msg := <-pc.Messages():
		message, err := s.codec.Decode(msg.Value)
		if err != nil {
			return 0, eventid.Zero, faults.Errorf("decoding last message: %w", err)
		}

		return uint64(msg.Offset), message.ID, nil
	case <-time.After(500 * time.Millisecond):
		return 0, eventid.Zero, nil
	}
}

func groupName(projName, topic string) string {
	return fmt.Sprintf("%s_%s", projName, topic)
}

func (s *Subscriber) StartConsumer(ctx context.Context, projName string, handler projection.ConsumerHandler, options ...projection.ConsumerOption) error {
	opts := projection.ConsumerOptions{}
	for _, v := range options {
		v(&opts)
	}

	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true
	config.Consumer.Offsets.Initial = sarama.OffsetOldest

	groupName := groupName(projName, s.topic)
	group, err := sarama.NewConsumerGroup(s.brokers, groupName, config)
	if err != nil {
		return faults.Errorf("creating consumer group '%s': %w", groupName, err)
	}

	logger := s.logger.WithTags(log.Tags{
		"topic":          s.topic,
		"consumer_group": groupName,
	})
	gh := groupHandler{
		resumeStore: s.resumeStore,
		logger:      logger,
		sub:         s,
		opts:        opts,
		projName:    projName,
		handler:     handler,
		ready:       make(chan bool),
	}

	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			err = group.Consume(ctx, []string{s.topic}, &gh)
			if err != nil {
				if errors.Is(err, sarama.ErrClosedConsumerGroup) {
					return
				}
				logger.WithError(err).Fatal("Error from consumer")
			}
			if ctx.Err() != nil {
				return
			}
			gh.ready = make(chan bool)
			s.logger.Info("===> REBALANCING")
		}
	}()

	<-gh.ready // Await till the consumer has been set up

	logger.Infof("Consumer group up and running!...")

	go func() {
		<-ctx.Done()
		logger.Infof("Terminating: context cancelled")

		wg.Wait()
		if err = group.Close(); err != nil {
			logger.WithError(err).Fatal("Error closing consumer group")
		}
	}()

	return nil
}

type groupHandler struct {
	resumeStore store.KVRStore
	logger      log.Logger
	sub         *Subscriber
	opts        projection.ConsumerOptions
	projName    string
	handler     projection.ConsumerHandler
	ready       chan bool
}

func (h *groupHandler) Setup(sess sarama.ConsumerGroupSession) (er error) {
	defer func() {
		if er != nil {
			fmt.Println("===> ERROR:", er)
		}
	}()

	claims := sess.Claims()

	for topic, parts := range claims {
		for _, part := range parts {
			resume, err := projection.NewResumeKey(h.projName, topic, uint32(part+1))
			if err != nil {
				return faults.Wrap(err)
			}

			data, err := h.resumeStore.Get(context.Background(), resume.String())
			if err != nil && !errors.Is(err, store.ErrResumeTokenNotFound) {
				return faults.Errorf("Could not retrieve resume token for '%s': %w", h.sub.topic, err)
			}

			token, err := projection.ParseToken(data)
			if err != nil {
				return faults.Wrap(err)
			}

			var startOffset int64
			if token.IsEmpty() {
				h.logger.WithTags(log.Tags{"topic": h.sub.topic}).Info("Starting consuming all available events")
				startOffset = sarama.OffsetOldest
			} else {
				h.logger.WithTags(log.Tags{"from": token.ConsumerSequence(), "topic": topic}).Info("Starting consumer from an offset")
				startOffset = int64(token.ConsumerSequence())
			}

			fmt.Printf("===> resetting partition=%d to offset=%d", part, startOffset)
			sess.ResetOffset(topic, part, startOffset, "start")
		}
	}

	close(h.ready)

	return nil
}

func (*groupHandler) Cleanup(_ sarama.ConsumerGroupSession) error {
	return nil
}

func (h *groupHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	h.logger.Info("===> listening to messages")
	for {
		select {
		case msg, ok := <-claim.Messages():
			if !ok {
				h.logger.Debug("message channel was closed")
				return nil
			}

			fmt.Println("===> got message: offset =", msg.Offset)
			evt, er := h.sub.codec.Decode(msg.Value)
			if er != nil {
				return faults.Errorf("unmarshal event '%s': %w", string(msg.Value), er)
			}
			if h.opts.Filter == nil || h.opts.Filter(evt) {
				h.logger.Debugf("Handling received event '%+v'", evt)
				er = h.handler(
					context.Background(),
					evt,
					uint32(msg.Partition),
					uint64(msg.Offset),
				)
				if er != nil {
					return faults.Errorf("handling event with ID '%s': %w", evt.ID, er)
				}
			}

			fmt.Printf("===> Message claimed: value = %s, timestamp = %v, topic = %s\n", string(msg.Value), msg.Timestamp, msg.Topic)
			session.MarkMessage(msg, "")

		case <-session.Context().Done():
			fmt.Println("===> BYE BYE")
			return nil
		}
	}
}
