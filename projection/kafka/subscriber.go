package kafka

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"maps"
	"sync"
	"time"

	"github.com/IBM/sarama"
	"github.com/cenkalti/backoff"
	"github.com/oklog/ulid/v2"
	"github.com/teris-io/shortid"

	"github.com/quintans/eventsourcing"
	"github.com/quintans/faults"

	"github.com/quintans/eventsourcing/log"
	"github.com/quintans/eventsourcing/projection"
	"github.com/quintans/eventsourcing/sink"
)

func NewSubscriberWithBrokers[K eventsourcing.ID, PK eventsourcing.IDPt[K]](
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
	subscriber, err := NewSubscriberWithClient[K, PK](
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

var _ projection.Consumer[*ulid.ULID] = (*Subscriber[*ulid.ULID])(nil)

type Subscriber[K eventsourcing.ID] struct {
	logger     *slog.Logger
	client     sarama.Client
	topic      string
	partitions []int32
	codec      sink.Codec[K]

	mu            sync.Mutex
	shutdownHooks []func()
}

func NewSubscriberWithClient[K eventsourcing.ID, PK eventsourcing.IDPt[K]](
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
		partitions: partitions,
		codec:      sink.JSONCodec[K, PK]{},
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

func (s *Subscriber[K]) Topic() (topic string) {
	return s.topic
}

func (s *Subscriber[K]) positions(startTime time.Time) (map[int32]int64, error) {
	bms := map[int32]int64{}
	for _, p := range s.partitions {
		seq, err := s.client.GetOffset(s.topic, p, startTime.UnixMilli())
		if err != nil {
			return nil, faults.Wrap(err)
		}
		bms[p] = seq
	}

	return bms, nil
}

func groupID(projName, topic string) string {
	return fmt.Sprintf("%s_%s", projName, topic)
}

func (s *Subscriber[K]) StartConsumer(ctx context.Context, startTime *time.Time, projName string, handler projection.ConsumerHandler[K], options ...projection.ConsumerOption[K]) error {
	opts := projection.ConsumerOptions[K]{}
	for _, v := range options {
		v(&opts)
	}

	gID := groupID(projName, s.topic)
	group, err := sarama.NewConsumerGroupFromClient(gID, s.client)
	if err != nil {
		return faults.Errorf("creating consumer group '%s': %w", gID, err)
	}
	logger := s.logger.With(
		"topic", s.topic,
		"consumer_group", gID,
	)

	var subPos map[int32]int64
	if startTime != nil {
		subPos, err = s.positions(*startTime)
		if err != nil {
			return faults.Errorf("getting partition positions '%s': %w", gID, err)
		}
	}

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
				if errors.Is(err, sarama.ErrClosedConsumerGroup) || errors.Is(err, io.EOF) {
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

	go func() {
		<-ctx.Done()
		logger.Info("Terminating: context cancelled")

		wg.Wait()
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
	ready    chan bool

	mu     sync.RWMutex
	subPos map[int32]int64
}

func (c *Consumer[K]) SubPos() map[int32]int64 {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return maps.Clone(c.subPos)
}

func (c *Consumer[K]) ClearSubPos() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.subPos = nil
}

// Setup is run at the beginning of a new session, before ConsumeClaim
func (c *Consumer[K]) Setup(session sarama.ConsumerGroupSession) (er error) {
	// Mark the consumer as ready
	defer close(c.ready)

	subPos := c.SubPos()
	if subPos == nil {
		return nil
	}

	// resetting ALL partitions
	for part, pos := range subPos {
		c.logger.Info("Starting consumer from an offset",
			"from", pos,
			"topic", c.topic,
			"partition", part,
		)

		// both instructions are needed to cover both scenarios
		session.MarkOffset(c.topic, part, pos, "")  // only moves to point AFTER the last consumed message of this group is
		session.ResetOffset(c.topic, part, pos, "") // only moves to point BEFORE the last consumed message of this group is
	}
	// We need to clear the map, otherwise it will be used again in the next session
	c.ClearSubPos()

	return nil
}

// Cleanup is run at the end of a session, once all ConsumeClaim goroutines have exited
func (c *Consumer[K]) Cleanup(session sarama.ConsumerGroupSession) error {
	c.logger.Info("Shutting down consumer")
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
					return faults.Errorf("handling event with ID '%s': %w", evt.ID, err)
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
