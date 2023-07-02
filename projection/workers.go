package projection

import (
	"context"
	"fmt"

	"github.com/quintans/faults"

	"github.com/quintans/eventsourcing/lock"
	"github.com/quintans/eventsourcing/log"
	"github.com/quintans/eventsourcing/player"
	"github.com/quintans/eventsourcing/util"
	"github.com/quintans/eventsourcing/worker"
)

type TaskerFactory func(partitionLow, partitionHi uint32) worker.Tasker

// PartitionedEventForwarderWorkers create workers responsible to forward events to their managed topic partition
// each worker is responsible to forward a range of partitions
func PartitionedEventForwarderWorkers(ctx context.Context, logger log.Logger, name string, lockerFactory LockerFactory, taskerFactory TaskerFactory, partitionSlots []worker.PartitionSlot) []worker.Worker {
	workers := make([]worker.Worker, len(partitionSlots))
	for i, v := range partitionSlots {
		slotsName := fmt.Sprintf("%d-%d", v.From, v.To)
		workers[i] = worker.NewRunWorker(
			logger,
			name+"-worker-"+slotsName,
			name,
			lockerFactory(name+"-lock-"+slotsName),
			taskerFactory(v.From, v.To),
		)
	}

	return workers
}

// PartitionedEventForwarderWorker creates a single worker responsible of forwarding
func PartitionedEventForwarderWorker(ctx context.Context, logger log.Logger, name string, lockerFactory LockerFactory, feeder worker.Tasker) worker.Worker {
	return worker.NewRunWorker(
		logger,
		name+"-worker",
		name,
		lockerFactory(name+"-lock"),
		feeder,
	)
}

type SubscriberFactory func(context.Context, ResumeKey) Subscriber

// PartitionedWorkers creates workers that will always run because a balancer locker is not provided.
// This assumes that the balancing will be done by the message broker.
func PartitionedWorkers(
	ctx context.Context,
	logger log.Logger,
	catchUpLockerFactory WaitLockerFactory,
	resumeStore ResumeStore,
	subscriberFactory SubscriberFactory,
	projectionName string, topic string, partitions uint32,
	esRepo player.Repository,
	handler player.MessageHandlerFunc,
	options ProjectorOptions,
) ([]worker.Worker, error) {
	workerLockerFactory := func(lockName string) lock.Locker {
		return nil
	}
	return PartitionedCompetingWorkers(
		ctx,
		logger,
		workerLockerFactory,
		catchUpLockerFactory,
		resumeStore,
		subscriberFactory,
		projectionName, topic, partitions,
		esRepo,
		handler,
		options,
	)
}

// PartitionedCompetingWorkers creates workers that will run depending if a lock was acquired or not.
//
// If a locker is provided it is possible to balance workers between the several server instances using a [worker.Balancer]
func PartitionedCompetingWorkers(
	ctx context.Context,
	logger log.Logger,
	workerLockerFactory LockerFactory,
	catchUpLockerFactory WaitLockerFactory,
	resumeStore ResumeStore,
	subscriberFactory SubscriberFactory,
	projectionName string, topic string, partitions uint32,
	esRepo player.Repository,
	handler player.MessageHandlerFunc,
	options ProjectorOptions,
) ([]worker.Worker, error) {
	if partitions <= 1 {
		w, err := createProjector(
			ctx,
			logger,
			workerLockerFactory,
			catchUpLockerFactory,
			resumeStore,
			subscriberFactory,
			projectionName,
			topic,
			0,
			esRepo,
			handler,
			options,
		)
		if err != nil {
			return nil, faults.Wrap(err)
		}
		return []worker.Worker{w}, nil
	}
	workers := make([]worker.Worker, partitions)
	for x := uint32(0); x < partitions; x++ {
		var err error
		workers[x], err = createProjector(
			ctx,
			logger,
			workerLockerFactory,
			catchUpLockerFactory,
			resumeStore,
			subscriberFactory,
			projectionName,
			topic,
			x+1,
			esRepo,
			handler,
			options,
		)
		if err != nil {
			return nil, faults.Wrap(err)
		}
	}

	return workers, nil
}

// CreateWorker creates a worker that will run if acquires the lock
func createProjector(
	ctx context.Context,
	logger log.Logger,
	workerLockerFactory LockerFactory,
	catchUpLockerFactory WaitLockerFactory,
	resumeStore ResumeStore,
	subscriberFactory SubscriberFactory,
	projectionName string,
	topic string,
	partition uint32,
	esRepo player.Repository,
	handler player.MessageHandlerFunc,
	options ProjectorOptions,
) (worker.Worker, error) {
	t, err := util.NewPartitionedTopic(topic, partition)
	if err != nil {
		return nil, faults.Wrap(err)
	}
	sr, err := NewStreamResume(t, projectionName)
	if err != nil {
		return nil, faults.Wrap(err)
	}

	return NewProjector(
		ctx,
		logger,
		workerLockerFactory,
		catchUpLockerFactory,
		resumeStore,
		subscriberFactory(ctx, sr),
		esRepo,
		handler,
		options,
	), nil
}
