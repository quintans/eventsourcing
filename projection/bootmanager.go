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
	Cancel()
}

var ErrCancelProjectionTimeout = errors.New("cancel projection timeout")

// CancelPublisher represents the interface that a message queue needs to implement to able to cancel distributed projections execution
type CancelPublisher interface {
	PublishCancel(ctx context.Context, targetName string, members int) error
}

type CancelListener interface {
	ListenCancel(ctx context.Context, restarter Canceller) error
}

type StreamResume struct {
	// topic identifies the topic. eg: account.3
	topic string
	// stream identifies a stream for a topic.
	// The same topic can be consumed by different projections and/or reactors.
	stream string
}

func NewStreamResume(topic, stream string) (StreamResume, error) {
	if topic == "" {
		faults.New("topic cannot be empty")
	}
	if stream == "" {
		faults.New("stream cannot be empty")
	}

	return StreamResume{
		topic:  topic,
		stream: stream,
	}, nil
}

func (ts StreamResume) Topic() string {
	return ts.topic
}

func (ts StreamResume) String() string {
	return ts.topic + "." + ts.stream
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
	StartConsumer(ctx context.Context, handler EventHandlerFunc, options ...ConsumerOption) (chan struct{}, error)
}

type Subscriber interface {
	Consumer
	MoveToLastPosition(ctx context.Context) error
}

type StreamResumer interface {
	GetStreamResumeToken(ctx context.Context, key StreamResume) (string, error)
	SetStreamResumeToken(ctx context.Context, key StreamResume, token string) error
}

type EventHandlerFunc func(ctx context.Context, e eventsourcing.Event) error

type StartStopBalancer struct {
	logger         log.Logger
	restartLock    lock.WaitForUnlocker
	cancelListener CancelListener
	balancer       worker.Balancer

	cancel context.CancelFunc
	done   <-chan struct{}
	mu     sync.RWMutex
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
		ctx2, cancel := context.WithCancel(ctx)
		b.mu.Lock()
		b.cancel = cancel
		b.mu.Unlock()

		err := b.startAndWait(ctx2)
		if err != nil {
			cancel()
			return err
		}

		select {
		case <-ctx.Done():
			return nil
		default:
		}
	}
}

func (b *StartStopBalancer) startAndWait(ctx context.Context) error {
	b.mu.Lock()
	b.done = b.balancer.Start(ctx)
	b.mu.Unlock()

	err := b.cancelListener.ListenCancel(ctx, b)
	if err != nil {
		return err
	}

	// wait for the closing subscriber
	<-b.done

	return nil
}

func (b *StartStopBalancer) Cancel() {
	b.mu.Lock()
	if b.cancel != nil {
		b.cancel()
	}
	b.mu.Unlock()
}
