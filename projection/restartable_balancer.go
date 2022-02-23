package projection

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/quintans/faults"

	"github.com/quintans/eventsourcing"
	"github.com/quintans/eventsourcing/eventid"
	"github.com/quintans/eventsourcing/lock"
	"github.com/quintans/eventsourcing/log"
	"github.com/quintans/eventsourcing/util"
	"github.com/quintans/eventsourcing/worker"
)

// Canceller is the interface for cancelling a running projection
type Canceller interface {
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
	// topic identifies the topic. eg: account#3
	topic util.Topic
	// stream identifies a stream for a topic.
	// The same topic can be consumed by different projections and/or reactors.
	stream string
}

func NewStreamResume(topic util.Topic, stream string) (ResumeKey, error) {
	if stream == "" {
		faults.New("stream cannot be empty")
	}

	return ResumeKey{
		topic:  topic,
		stream: stream,
	}, nil
}

func (ts ResumeKey) Topic() util.Topic {
	return ts.topic
}

func (ts ResumeKey) String() string {
	return ts.topic.String() + ":" + ts.stream
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
	RetrieveLastResume(ctx context.Context) (Resume, error)
	RecordLastResume(ctx context.Context, token string) error
}

type Resume struct {
	Topic   util.Topic
	EventID eventid.EventID
	Token   string
}

var ErrResumeTokenNotFound = errors.New("resume token not found")

type ResumeStore interface {
	// GetStreamResumeToken retrives the resume token for the resume key.
	// If the a resume key is not found it return ErrResumeTokenNotFound as an error
	GetStreamResumeToken(ctx context.Context, key ResumeKey) (string, error)
	SetStreamResumeToken(ctx context.Context, key ResumeKey, token string) error
}

type EventHandlerFunc func(ctx context.Context, e eventsourcing.Event) error

var _ Canceller = (*RestartableProjection)(nil)

type RestartableProjection struct {
	logger         log.Logger
	name           string
	restartLock    lock.WaitForUnlocker
	cancelListener CancelListener
	workers        []worker.Worker

	done chan struct{}
	mu   sync.RWMutex
}

// NewRestartableProjection creates an instance that manages the lifecycle of a balancer that has the capability of being stopped and restarted on demand.
func NewRestartableProjection(
	logger log.Logger,
	name string,
	restartLock lock.WaitForUnlocker,
	cancelListener CancelListener,
	workers []worker.Worker,
) *RestartableProjection {
	mc := &RestartableProjection{
		logger: logger.WithTags(log.Tags{
			"projection": name,
		}),
		name:           name,
		restartLock:    restartLock,
		cancelListener: cancelListener,
		workers:        workers,
	}

	return mc
}

// Name returns the name of this balancer
func (b *RestartableProjection) Name() string {
	return b.name
}

// Start runs the action to be executed on boot
func (b *RestartableProjection) Start(ctx context.Context) error {
	for {
		b.logger.Info("Waiting for Unlock")
		b.restartLock.WaitForUnlock(ctx)
		b.mu.Lock()
		b.done = make(chan struct{})
		b.mu.Unlock()

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

func (b *RestartableProjection) startAndWait(ctx context.Context, done chan struct{}) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	for _, v := range b.workers {
		v.Start(ctx)
	}

	err := b.cancelListener.ListenCancel(ctx, b)
	if err != nil {
		return err
	}

	// wait for the closing subscriber
	<-done

	return nil
}

func (b *RestartableProjection) Cancel(ctx context.Context, hard bool) {
	b.mu.Lock()
	if b.done != nil {
		for _, w := range b.workers {
			if hard {
				w.Pause(ctx, true)
			} else {
				w.Stop(ctx)
			}
		}
		close(b.done)
		b.done = nil
	}
	b.mu.Unlock()
}
