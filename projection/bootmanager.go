package projection

import (
	"context"
	"errors"
	"sync"

	"github.com/quintans/faults"

	"github.com/quintans/eventsourcing"
	"github.com/quintans/eventsourcing/lock"
	"github.com/quintans/eventsourcing/log"
)

// Canceller is the interface for cancelling a running projection
type Canceller interface {
	Name() string
	Cancel()
}

var ErrCancelProjectionTimeout = errors.New("cancel projection timeout")

// Notifier represents the interface that a message queue needs to implement to able to cancel distributed projections execution
type Notifier interface {
	ListenCancelProjection(ctx context.Context, restarter Canceller) error
	CancelProjection(ctx context.Context, projectionName string, partitions int) error
}

type StreamResume struct {
	Topic  string
	Stream string
}

func (ts StreamResume) String() string {
	return ts.Topic + "." + ts.Stream
}

type ConsumerOptions struct {
	Filter func(e eventsourcing.Event) bool
}

type ConsumerOption func(*ConsumerOptions)

func WithFilter(filter func(e eventsourcing.Event) bool) ConsumerOption {
	return func(o *ConsumerOptions) {
		o.Filter = filter
	}
}

type Subscriber interface {
	StartConsumer(ctx context.Context, resume StreamResume, handler EventHandlerFunc, options ...ConsumerOption) (chan struct{}, error)
	GetResumeToken(ctx context.Context, topic string) (string, error)
}

type StreamResumer interface {
	GetStreamResumeToken(ctx context.Context, key string) (string, error)
	SetStreamResumeToken(ctx context.Context, key string, token string) error
}

type EventHandlerFunc func(ctx context.Context, e eventsourcing.Event) error

type ProjectionPartition struct {
	logger      log.Logger
	handler     EventHandlerFunc
	restartLock lock.Locker
	notifier    Notifier
	resume      StreamResume
	filter      func(e eventsourcing.Event) bool
	subscriber  Subscriber

	cancel context.CancelFunc
	done   chan struct{}
	mu     sync.RWMutex
}

// NewProjectionPartition creates an instance that manages the lifecycle of a projection that has the capability of being stopped and restarted on demand.
func NewProjectionPartition(
	logger log.Logger,
	restartLock lock.Locker,
	notifier Notifier,
	subscriber Subscriber,
	resume StreamResume,
	filter func(e eventsourcing.Event) bool,
	handler EventHandlerFunc,
) *ProjectionPartition {
	mc := &ProjectionPartition{
		logger:      logger,
		restartLock: restartLock,
		handler:     handler,
		notifier:    notifier,
		resume:      resume,
		filter:      filter,
		subscriber:  subscriber,
	}

	return mc
}

// Name returns the name of this projection
func (m *ProjectionPartition) Name() string {
	return m.resume.Stream
}

// Run action to be executed on boot
func (m *ProjectionPartition) Run(ctx context.Context) error {
	logger := m.logger.WithTags(log.Tags{
		"projection": m.resume.Stream,
	})
	for {
		logger.Info("Waiting for Unlock")
		m.restartLock.WaitForUnlock(ctx)
		ctx2, cancel := context.WithCancel(ctx)

		err := m.bootAndListen(ctx2)
		if err != nil {
			cancel()
			return err
		}

		m.mu.Lock()
		m.cancel = cancel
		m.mu.Unlock()

		select {
		case <-ctx.Done():
			return nil
		default:
		}
	}
}

func (m *ProjectionPartition) bootAndListen(ctx context.Context) error {
	err := m.boot(ctx)
	if err != nil {
		return err
	}

	err = m.notifier.ListenCancelProjection(ctx, m)
	if err != nil {
		return err
	}

	<-ctx.Done()

	return nil
}

func (m *ProjectionPartition) boot(ctx context.Context) error {
	// start consuming events from the last available position
	options := []ConsumerOption{}
	if m.filter != nil {
		options = append(options, WithFilter(m.filter))
	}
	done, err := m.subscriber.StartConsumer(
		ctx,
		m.resume,
		m.handler,
		options...,
	)
	if err != nil {
		return faults.Errorf("Unable to start consumer projection %s: %w", m.resume.Stream, err)
	}

	m.mu.Lock()
	m.done = done
	m.mu.Unlock()

	return nil
}

func (m *ProjectionPartition) Cancel() {
	m.mu.Lock()
	if m.cancel != nil {
		m.cancel()
	}
	// wait for the closing subscriber
	<-m.done

	m.mu.Unlock()
}
