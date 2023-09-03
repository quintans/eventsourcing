package kafka

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/IBM/sarama"
	"github.com/cenkalti/backoff"
	"github.com/teris-io/shortid"

	"github.com/quintans/eventsourcing"
	"github.com/quintans/faults"

	"github.com/quintans/eventsourcing/eventid"
	"github.com/quintans/eventsourcing/log"
	"github.com/quintans/eventsourcing/projection"
	"github.com/quintans/eventsourcing/sink"
	"github.com/quintans/eventsourcing/util"
)

func NewSubscriberWithBrokers[K eventsourcing.ID](
	ctx context.Context,
	logger *slog.Logger,
	brokers []string,
	topic string,
	config *sarama.Config,
) (*Subscriber[K], error) {
	if config == nil {
		config = sarama.NewConfig()
	}
	config.Consumer.Return.Errors = true

	client, err := sarama.NewClient(brokers, config)
	if err != nil {
		return nil, faults.Errorf("instantiating Kafka client: %w", err)
	}
	subscriber, err := NewSubscriberWithClient[K](
		logger,
		client,
		topic,
	)
	if err != nil {
		return nil, faults.Wrap(err)
	}
	go func() {
		<-ctx.Done()
		subscriber.Shutdown()
		client.Close()
	}()

	return subscriber, nil
}

type SubOption[K eventsourcing.ID] func(*Subscriber[K])

func WithMsgCodec[K eventsourcing.ID](codec sink.Codec[K]) SubOption[K] {
	return func(r *Subscriber[K]) {
		r.codec = codec
	}
}

// var _ projection.Consumer = (*Subscriber)(nil)

type Subscriber[K eventsourcing.ID] struct {
	logger     *slog.Logger
	client     sarama.Client
	topic      string
	partitions []uint32
	codec      sink.Codec[K]

	mu            sync.Mutex
	shutdownHooks []func()
}

func NewSubscriberWithClient[K eventsourcing.ID](
	logger *slog.Logger,
	client sarama.Client,
	topic string,
	options ...SubOption[K],
) (*Subscriber[K], error) {
	if topic == "" {
		return nil, faults.New("empty topic provided")
	}

	partitions, err := client.Partitions(topic)
	if err != nil {
		return nil, faults.Errorf("getting partitions: %w", err)
	}
	s := &Subscriber[K]{
		logger: logger.With(
			"subscriber", "kafka",
			"id", "subscriber-"+shortid.MustGenerate(),
		),
		client:     client,
		topic:      topic,
		partitions: util.NormalizePartitions(partitions),
		codec:      sink.JSONCodec[K]{},
	}

	for _, o := range options {
		o(s)
	}

	return s, nil
}

func (s *Subscriber[K]) Shutdown() {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, fn := range s.shutdownHooks {
		fn()
	}
	s.shutdownHooks = nil
}

func (s *Subscriber[K]) AddShutdownHook(hook func()) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.shutdownHooks = append(s.shutdownHooks, hook)
}

func (s *Subscriber[K]) TopicPartitions() (topic string, partitions []uint32) {
	return s.topic, s.partitions
}

func (s *Subscriber[K]) Positions(ctx context.Context) (map[uint32]projection.SubscriberPosition, error) {
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

func (s *Subscriber[K]) lastBUSMessage(_ context.Context, partition int32) (_ uint64, _ eventid.EventID, e error) {
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

func groupID(projName, topic string) string {
	return fmt.Sprintf("%s_%s", projName, topic)
}

func (s *Subscriber[K]) StartConsumer(ctx context.Context, subPos map[uint32]projection.SubscriberPosition, projName string, handler projection.ConsumerHandler[K], options ...projection.ConsumerOption[K]) error {
	opts := projection.ConsumerOptions[K]{}
	for _, v := range options {
		v(&opts)
	}

	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true

	s.client.Closed()

	gID := groupID(projName, s.topic)
	group, err := sarama.NewConsumerGroupFromClient(gID, s.client)
	if err != nil {
		return faults.Errorf("creating consumer group '%s': %w", gID, err)
	}
	logger := s.logger.With(
		"topic", s.topic,
		"consumer_group", gID,
	)

	consumer := Consumer[K]{
		logger:   logger,
		sub:      s,
		opts:     opts,
		projName: projName,
		topic:    s.topic,
		handler:  handler,
		subPos:   subPos,
		ready:    make(chan bool),
	}

	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			// `Consume` should be called inside an infinite loop, when a
			// server-side rebalance happens, the consumer session will need to be
			// recreated to get the new claims
			if err := group.Consume(ctx, []string{s.topic}, &consumer); err != nil {
				if errors.Is(err, sarama.ErrClosedConsumerGroup) {
					return
				}
				logger.Error("Error from consumer", log.Err(err))
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
	logger.Info("Consumer group up and running!...")

	s.AddShutdownHook(func() {
		if err = group.Close(); err != nil {
			logger.Error("Error closing consumer group", log.Err(err))
		}
	})

	go func() {
		<-ctx.Done()
		logger.Info("Terminating: context cancelled")

		wg.Wait()
		s.Shutdown()
	}()

	return nil
}

// Consumer represents a Sarama consumer group consumer
type Consumer[K eventsourcing.ID] struct {
	logger   *slog.Logger
	sub      *Subscriber[K]
	opts     projection.ConsumerOptions[K]
	projName string
	topic    string
	handler  projection.ConsumerHandler[K]
	subPos   map[uint32]projection.SubscriberPosition
	ready    chan bool
}

// Setup is run at the beginning of a new session, before ConsumeClaim
func (c *Consumer[K]) Setup(session sarama.ConsumerGroupSession) (er error) {
	// Mark the consumer as ready
	defer close(c.ready)

	if c.subPos == nil {
		return nil
	}

	for part, pos := range c.subPos {
		var startOffset int64
		if pos.Position == 0 {
			c.logger.Info("Starting consuming all available events",
				"topic", c.sub.topic,
				"partition", part-1,
			)
			startOffset = 0
		} else {
			c.logger.Info("Starting consumer from an offset",
				"from", pos.Position+1,
				"topic", c.topic,
				"partition", part-1,
			)
			startOffset = int64(pos.Position + 1)
		}

		// both instructions are needed to cover both scenarios
		session.MarkOffset(c.topic, int32(part-1), startOffset, "")  // only moves to point AFTER the last consumed message of this group is
		session.ResetOffset(c.topic, int32(part-1), startOffset, "") // only moves to point BEFORE the last consumed message of this group is
	}

	return nil
}

// Cleanup is run at the end of a session, once all ConsumeClaim goroutines have exited
func (c *Consumer[K]) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

// ConsumeClaim must start a consumer loop of ConsumerGroupClaim's Messages().
// Once the Messages() channel is closed, the Handler must finish its processing
// loop and exit.
func (c *Consumer[K]) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
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

			evt, er := c.sub.codec.Decode(msg.Value)
			if er != nil {
				return faults.Errorf("unmarshal event '%s': %w", string(msg.Value), er)
			}
			if c.opts.Filter == nil || c.opts.Filter(evt) {
				// TODO should be configurable
				bo := backoff.NewExponentialBackOff()
				bo.MaxElapsedTime = 10 * time.Second

				err := backoff.Retry(func() error {
					er := c.handler(
						session.Context(),
						evt,
						uint32(msg.Partition+1),
						uint64(msg.Offset+1),
					)
					if session.Context().Err() != nil {
						return backoff.Permanent(er)
					}
					return er
				}, bo)
				if err != nil {
					return faults.Errorf("handling event with ID '%s': %w", evt.ID, er)
				}
			}

			session.MarkMessage(msg, "")
		// Should return when `session.Context()` is done.
		// If not, will raise `ErrRebalanceInProgress` or `read tcp <ip>:<port>: i/o timeout` when kafka rebalance. see:
		// https://github.com/IBM/sarama/issues/1192
		case <-session.Context().Done():
			return nil
		}
	}
}
