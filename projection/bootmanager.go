package projection

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/quintans/faults"

	"github.com/quintans/eventsourcing"
	"github.com/quintans/eventsourcing/lock"
	"github.com/quintans/eventsourcing/log"
	"github.com/quintans/eventsourcing/worker"
)

// Canceller is the interface for cancelling a running projection
type Canceller interface {
	Name() string
	Cancel(ctx context.Context, hard bool)
}

var ErrCancelProjectionTimeout = errors.New("cancel projection timeout")

// CancelPublisher represents the interface that a message queue needs to implement to able to cancel distributed projections execution
type CancelPublisher interface {
	PublishCancel(ctx context.Context, targetName string, members int) error
}

type CancelListener interface {
	ListenCancel(ctx context.Context, restarter Canceller) error
}

// ResumeKey is used to retrieve the last event id to replay messages directly from the event store.
type ResumeKey struct {
	// topic identifies the topic. eg: account.3
	topic string
	// stream identifies a stream for a topic.
	// The same topic can be consumed by different projections and/or reactors.
	stream string
}

func NewStreamResume(topic, stream string) (ResumeKey, error) {
	if topic == "" {
		faults.New("topic cannot be empty")
	}
	if stream == "" {
		faults.New("stream cannot be empty")
	}

	return ResumeKey{
		topic:  topic,
		stream: stream,
	}, nil
}

func (ts ResumeKey) Topic() string {
	return ts.topic
}

func (ts ResumeKey) String() string {
	return ts.topic + ":" + ts.stream
}

type ConsumerOptions struct {
	Filter  func(e eventsourcing.Event) bool
	AckWait time.Duration
}

type ConsumerOption func(*ConsumerOptions)

func WithFilter(filter func(e eventsourcing.Event) bool) ConsumerOption {
	return func(o *ConsumerOptions) {
		o.Filter = filter
	}
}

func WithAckWait(ackWait time.Duration) ConsumerOption {
	return func(o *ConsumerOptions) {
		o.AckWait = ackWait
	}
}

type Consumer interface {
	StartConsumer(ctx context.Context, handler EventHandlerFunc, options ...ConsumerOption) error
	StopConsumer(ctx context.Context, hard bool)
}

type Subscriber interface {
	Consumer
	RecordLastResume(ctx context.Context) error
}

type ResumeStore interface {
	GetStreamResumeToken(ctx context.Context, key ResumeKey) (string, error)
	SetStreamResumeToken(ctx context.Context, key ResumeKey, token string) error
}

type EventHandlerFunc func(ctx context.Context, e eventsourcing.Event) error

var _ Canceller = (*StartStopBalancer)(nil)

type StartStopBalancer struct {
	logger         log.Logger
	restartLock    lock.WaitForUnlocker
	cancelListener CancelListener
	balancer       worker.Balancer

	done chan struct{}
	mu   sync.RWMutex
}

// NewStartStopBalancer creates an instance that manages the lifecycle of a balancer that has the capability of being stopped and restarted on demand.
func NewStartStopBalancer(
	logger log.Logger,
	restartLock lock.WaitForUnlocker,
	cancelListener CancelListener,
	balancer worker.Balancer,
) *StartStopBalancer {
	mc := &StartStopBalancer{
		logger: logger.WithTags(log.Tags{
			"balancer": balancer.Name(),
		}),
		restartLock:    restartLock,
		cancelListener: cancelListener,
		balancer:       balancer,
	}

	return mc
}

// Name returns the name of this balancer
func (b *StartStopBalancer) Name() string {
	return b.balancer.Name()
}

// Run action to be executed on boot
func (b *StartStopBalancer) Run(ctx context.Context) error {
	for {
		b.logger.Info("Waiting for Unlock")
		b.restartLock.WaitForUnlock(ctx)
		b.mu.Lock()
		b.done = make(chan struct{})
		b.mu.Unlock()
		b.balancer.Start(ctx)

		err := b.startAndWait(ctx, b.done)
		if err != nil {
			b.Cancel(context.Background(), false)
			return err
		}

		select {
		case <-ctx.Done():
			b.Cancel(context.Background(), false)
			return nil
		default:
		}
	}
}

func (b *StartStopBalancer) startAndWait(ctx context.Context, done chan struct{}) error {
	err := b.cancelListener.ListenCancel(ctx, b)
	if err != nil {
		return err
	}

	// wait for the closing subscriber
	<-done

	return nil
}

func (b *StartStopBalancer) Cancel(ctx context.Context, hard bool) {
	b.mu.Lock()
	if b.done != nil {
		b.balancer.Stop(ctx, hard)
		close(b.done)
		b.done = nil
	}
	b.mu.Unlock()
}
