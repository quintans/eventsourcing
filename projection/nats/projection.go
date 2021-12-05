package nats

import (
	"context"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/stan.go"
	"github.com/quintans/faults"

	"github.com/quintans/eventsourcing/lock"
	"github.com/quintans/eventsourcing/log"
	"github.com/quintans/eventsourcing/projection"
	"github.com/quintans/eventsourcing/subscriber"
	"github.com/quintans/eventsourcing/worker"
)

func NewProjection(
	ctx context.Context,
	logger log.Logger,
	stream stan.Conn,
	pubsub *nats.Conn,
	locker lock.Locker,
	unlocker lock.WaitForUnlocker,
	memberlist worker.Memberlister,
	projectionName string,
	topic string,
	partitions uint32,
	handler projection.EventHandlerFunc,
) (*projection.NotifierLockRebuilder, *projection.StartStopBalancer, error) {
	var subscribers []projection.Subscriber
	consumerFactory := func(ctx context.Context, resume projection.StreamResume) (projection.Consumer, error) {
		sub, err := subscriber.NewNatsSubscriber(ctx, logger, stream, resume)
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

	natsCanceller, err := subscriber.NewNatsProjectionSubscriber(ctx, logger, pubsub, projectionName+"_notifications")
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

// NewReactor creates workers that listen to events coming through the event bus,
// forwarding them to an handler. This is the same approache used for projections
// but where we don't care about replays, usually for the write side of things.
// The number of workers will be equal to the number of partitions.
func NewReactor(
	ctx context.Context,
	logger log.Logger,
	stream stan.Conn,
	projectionName string,
	topic string,
	partitions uint32,
	handler projection.EventHandlerFunc,
) (<-chan struct{}, error) {

	consumerFactory := func(ctx context.Context, resumeKey projection.StreamResume) (projection.Consumer, error) {
		return subscriber.NewNatsSubscriber(ctx, logger, stream, resumeKey)
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
