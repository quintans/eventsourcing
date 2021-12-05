package nats

import (
	"context"

	"github.com/nats-io/nats.go"
	"github.com/quintans/faults"

	"github.com/quintans/eventsourcing/lock"
	"github.com/quintans/eventsourcing/log"
	"github.com/quintans/eventsourcing/projection"
	"github.com/quintans/eventsourcing/worker"
)

func NewProjection(
	ctx context.Context,
	logger log.Logger,
	url string,
	locker lock.Locker,
	unlocker lock.WaitForUnlocker,
	memberlist worker.Memberlister,
	projectionName string,
	topic string,
	partitions uint32,
	handler projection.EventHandlerFunc,
) (*projection.NotifierLockRebuilder, *projection.StartStopBalancer, error) {
	nc, err := nats.Connect(url)
	if err != nil {
		return nil, nil, faults.Errorf("Could not instantiate NATS connection: %w", err)
	}
	go func() {
		<-ctx.Done()
		nc.Close()
	}()

	return NewProjectionWithConn(ctx, logger, nc, locker, unlocker, memberlist, projectionName, topic, partitions, handler)
}

func NewProjectionWithConn(
	ctx context.Context,
	logger log.Logger,
	pubsub *nats.Conn,
	locker lock.Locker,
	unlocker lock.WaitForUnlocker,
	memberlist worker.Memberlister,
	projectionName string,
	topic string,
	partitions uint32,
	handler projection.EventHandlerFunc,
) (*projection.NotifierLockRebuilder, *projection.StartStopBalancer, error) {
	stream, err := pubsub.JetStream()
	if err != nil {
		return nil, nil, faults.Wrap(err)
	}
	var subscribers []projection.Subscriber
	consumerFactory := func(ctx context.Context, resume projection.StreamResume) (projection.Consumer, error) {
		sub, err := NewSubscriber(ctx, logger, stream, resume)
		if err != nil {
			return nil, faults.Wrap(err)
		}
		subscribers = append(subscribers, sub)
		return sub, nil
	}

	// create workers according to the topic that we want to listen
	workers, err := projection.UnmanagedWorkers(ctx, logger, projectionName, topic, partitions, consumerFactory, handler)
	if err != nil {
		return nil, nil, faults.Wrap(err)
	}

	balancer := worker.NewNoBalancer(logger, projectionName, memberlist, workers)

	natsCanceller, err := NewNotificationListenerWithConn(ctx, logger, pubsub, projectionName+"_notifications")
	if err != nil {
		return nil, nil, faults.Errorf("Error creating NATS canceller subscriber: %w", err)
	}
	startStop := projection.NewStartStopBalancer(logger, unlocker, natsCanceller, balancer)

	rebuilder := projection.NewNotifierLockRestarter(
		logger,
		locker,
		natsCanceller,
		subscribers,
		memberlist,
	)

	return rebuilder, startStop, nil
}

// NewReactorWithConn creates workers that listen to events coming through the event bus,
// forwarding them to an handler. This is the same approache used for projections
// but where we don't care about replays, usually for the write side of things.
// The number of workers will be equal to the number of partitions.
func NewReactor(
	ctx context.Context,
	logger log.Logger,
	url string,
	projectionName string,
	topic string,
	partitions uint32,
	handler projection.EventHandlerFunc,
) (<-chan struct{}, error) {
	nc, err := nats.Connect(url)
	if err != nil {
		return nil, faults.Errorf("Could not instantiate NATS connection: %w", err)
	}
	js, err := nc.JetStream()
	if err != nil {
		nc.Close()
		return nil, faults.Errorf("Could not instantiate Nats jetstream context: %w", err)
	}

	reactDone, err := NewReactorWithConn(ctx, logger, js, projectionName, topic, partitions, handler)
	if err != nil {
		return nil, faults.Wrap(err)
	}
	done := make(chan struct{})
	go func() {
		<-ctx.Done()
		nc.Close()
		<-reactDone
		close(done)
	}()

	return done, nil
}

func NewReactorWithConn(
	ctx context.Context,
	logger log.Logger,
	stream nats.JetStreamContext,
	projectionName string,
	topic string,
	partitions uint32,
	handler projection.EventHandlerFunc,
) (<-chan struct{}, error) {
	consumerFactory := func(ctx context.Context, resumeKey projection.StreamResume) (projection.Consumer, error) {
		return NewSubscriber(ctx, logger, stream, resumeKey)
	}

	workers, err := projection.UnmanagedWorkers(ctx, logger, projectionName, topic, partitions, consumerFactory, handler)
	if err != nil {
		return nil, faults.Errorf("Error creating unmanaged workers: %w", err)
	}
	done := make(chan struct{})
	go func() {
		<-ctx.Done()
		for _, w := range workers {
			w.Stop(context.Background())
		}
		close(done)
	}()

	return done, nil
}
