package projection

import (
	"context"
	"log/slog"

	"github.com/quintans/eventsourcing"
	"github.com/quintans/faults"

	"github.com/quintans/eventsourcing/store"
	"github.com/quintans/eventsourcing/worker"
)

type SubscriberFactory[K eventsourcing.ID] func(context.Context, ResumeKey) Consumer[K]

// PartitionedWorkers creates workers that will always run because a balancer locker is not provided.
// This assumes that the balancing will be done by the message broker.
func PartitionedWorkers[K eventsourcing.ID](
	ctx context.Context,
	logger *slog.Logger,
	lockerFactory LockerFactory,
	subscriberFactory SubscriberFactory[K],
	topic string, partitions uint32,
	esRepo EventsRepository[K],
	projection Projection[K],
	splits int,
	splitIds []int,
	resumeStore store.KVStore,
) ([]*worker.RunWorker, error) {
	return PartitionedCompetingWorkers(
		ctx,
		logger,
		lockerFactory,
		subscriberFactory,
		topic, partitions,
		esRepo,
		projection,
		splits,
		resumeStore,
	)
}

// PartitionedCompetingWorkers creates workers that will run depending if a lock was acquired or not.
//
// If a locker is provided it is possible to balance workers between the several server instances using a [worker.Balancer]
func PartitionedCompetingWorkers[K eventsourcing.ID](
	ctx context.Context,
	logger *slog.Logger,
	lockerFactory LockerFactory,
	subscriberFactory SubscriberFactory[K],
	topic string, partitions uint32,
	esRepo EventsRepository[K],
	projection Projection[K],
	splits int,
	resumeStore store.KVStore,
) ([]*worker.RunWorker, error) {
	if partitions <= 1 {
		w, err := createProjector(
			ctx,
			logger,
			lockerFactory,
			subscriberFactory,
			topic,
			0,
			esRepo,
			projection,
			resumeStore,
			splits,
		)
		if err != nil {
			return nil, faults.Wrap(err)
		}
		return []*worker.RunWorker{w}, nil
	}

	var wrks []*worker.RunWorker
	for x := uint32(0); x < partitions; x++ {
		w, err := createProjector(
			ctx,
			logger,
			lockerFactory,
			subscriberFactory,
			topic,
			x+1,
			esRepo,
			projection,
			resumeStore,
			splits,
		)
		if err != nil {
			return nil, faults.Wrap(err)
		}
		wrks = append(wrks, w)
	}

	return wrks, nil
}

// CreateWorker creates a worker that will run if acquires the lock
func createProjector[K eventsourcing.ID](
	ctx context.Context,
	logger *slog.Logger,
	lockerFactory LockerFactory,
	subscriberFactory SubscriberFactory[K],
	topic string,
	partition uint32,
	esRepo EventsRepository[K],
	projection Projection[K],
	resumeStore store.KVStore,
	splits int,
) (*worker.RunWorker, error) {
	sr, err := NewResumeKey(projection.Name(), topic, partition)
	if err != nil {
		return nil, faults.Wrap(err)
	}

	return Project(
		logger,
		lockerFactory,
		esRepo,
		subscriberFactory(ctx, sr),
		projection,
		resumeStore,
		splits,
	), nil
}
