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

	gn := groupName(projName, s.topic)
	client, err := sarama.NewConsumerGroupFromClient(gn, s.client)
	if err != nil {
		return faults.Errorf("creating consumer group '%s': %w", groupName, err)
	}
	logger := s.logger.WithTags(log.Tags{
		"topic":          s.topic,
		"consumer_group": gn,
	})

	consumer := Consumer{
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
			// `Consume` should be called inside an infinite loop, when a
			// server-side rebalance happens, the consumer session will need to be
			// recreated to get the new claims
			if err := client.Consume(ctx, []string{s.topic}, &consumer); err != nil {
				if errors.Is(err, sarama.ErrClosedConsumerGroup) {
					return
				}
				logger.WithError(err).Error("Error from consumer")
				return
			}
			// check if context was cancelled, signaling that the consumer should stop
			if ctx.Err() != nil {
				return
			}

			consumer.ready = make(chan bool)
		}
	}()

	<-consumer.ready // Await till the consumer has been set up
	logger.Infof("Consumer group up and running!...")

	go func() {
		<-ctx.Done()
		logger.Infof("Terminating: context cancelled")

		wg.Wait()
		if err = client.Close(); err != nil {
			logger.WithError(err).Error("Error closing consumer group")
		}
	}()

	return nil
}

// Consumer represents a Sarama consumer group consumer
type Consumer struct {
	resumeStore store.KVRStore
	logger      log.Logger
	sub         *Subscriber
	opts        projection.ConsumerOptions
	projName    string
	handler     projection.ConsumerHandler
	ready       chan bool
}

// Setup is run at the beginning of a new session, before ConsumeClaim
func (c *Consumer) Setup(sess sarama.ConsumerGroupSession) (er error) {
	defer func() {
		if er != nil {
			fmt.Println("===> ERROR:", er)
		}
	}()

	claims := sess.Claims()

	for topic, parts := range claims {
		for _, part := range parts {
			resume, err := projection.NewResumeKey(c.projName, topic, uint32(part+1))
			if err != nil {
				return faults.Wrap(err)
			}

			data, err := c.resumeStore.Get(context.Background(), resume.String())
			if err != nil && !errors.Is(err, store.ErrResumeTokenNotFound) {
				return faults.Errorf("Could not retrieve resume token for '%s': %w", c.sub.topic, err)
			}

			token, err := projection.ParseToken(data)
			if err != nil {
				return faults.Wrap(err)
			}

			var startOffset int64
			if token.IsEmpty() {
				c.logger.WithTags(log.Tags{"topic": c.sub.topic}).Info("Starting consuming all available events")
				startOffset = 0
			} else {
				c.logger.WithTags(log.Tags{"from": token.ConsumerSequence(), "topic": topic}).Info("Starting consumer from an offset")
				startOffset = int64(token.ConsumerSequence())
			}

			fmt.Printf("===> resetting to topic=%s, partition=%d, offset=%d\n", topic, part, startOffset)
			sess.ResetOffset(topic, part, startOffset, "start")
		}
	}

	// Mark the consumer as ready
	close(c.ready)
	return nil
}

// Cleanup is run at the end of a session, once all ConsumeClaim goroutines have exited
func (c *Consumer) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

// ConsumeClaim must start a consumer loop of ConsumerGroupClaim's Messages().
// Once the Messages() channel is closed, the Handler must finish its processing
// loop and exit.
func (c *Consumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	// NOTE:
	// Do not move the code below to a goroutine.
	// The `ConsumeClaim` itself is called within a goroutine, see:
	// https://github.com/IBM/sarama/blob/main/consumer_group.go#L27-L29
	for {
		select {
		case msg, ok := <-claim.Messages():
			if !ok {
				c.logger.Debug("message channel was closed")
				return nil
			}
			fmt.Printf(">>====> Message claimed: value = %s, timestamp = %v, topic = %s, offset=%d", string(msg.Value), msg.Timestamp, msg.Topic, msg.Offset)

			evt, er := c.sub.codec.Decode(msg.Value)
			if er != nil {
				return faults.Errorf("unmarshal event '%s': %w", string(msg.Value), er)
			}
			if c.opts.Filter == nil || c.opts.Filter(evt) {
				c.logger.Debugf("Handling received event '%+v'", evt)
				er = c.handler(
					context.Background(),
					evt,
					uint32(msg.Partition),
					uint64(msg.Offset),
				)
				if er != nil {
					return faults.Errorf("handling event with ID '%s': %w", evt.ID, er)
				}
			}

			session.MarkMessage(msg, "")
		// Should return when `session.Context()` is done.
		// If not, will raise `ErrRebalanceInProgress` or `read tcp <ip>:<port>: i/o timeout` when kafka rebalance. see:
		// https://github.com/IBM/sarama/issues/1192
		case <-session.Context().Done():
			fmt.Println("===> BYE BYE")
			return nil
		}
	}
}
