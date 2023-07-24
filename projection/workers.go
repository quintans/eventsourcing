package projection

import (
	"context"
	"fmt"

	"github.com/quintans/faults"

	"github.com/quintans/eventsourcing/lock"
	"github.com/quintans/eventsourcing/log"
	"github.com/quintans/eventsourcing/util"
	"github.com/quintans/eventsourcing/worker"
)

type TaskerFactory func(partitionLow, partitionHi uint32) worker.Task

// PartitionedEventForwarderWorkers create workers responsible to forward events to their managed topic partition
// each worker is responsible to forward a range of partitions
func PartitionedEventForwarderWorkers(logger log.Logger, name string, lockerFactory LockerFactory, taskerFactory TaskerFactory, partitionSlots []worker.PartitionSlot) []worker.Worker {
	workers := make([]worker.Worker, len(partitionSlots))
	for i, v := range partitionSlots {
		slotsName := fmt.Sprintf("%d-%d", v.From, v.To)

		var locker lock.Locker
		if lockerFactory != nil {
			locker = lockerFactory(name + "-lock-" + slotsName)
		}

		workers[i] = worker.NewRunWorker(
			logger,
			name+"-worker-"+slotsName,
			name,
			locker,
			taskerFactory(v.From, v.To),
		)
	}

	return workers
}

// PartitionedEventForwarderWorker creates a single worker responsible of forwarding
func PartitionedEventForwarderWorker(logger log.Logger, name string, lockerFactory LockerFactory, task worker.Task) worker.Worker {
	var locker lock.Locker
	if lockerFactory != nil {
		locker = lockerFactory(name + "-lock")
	}

	return worker.NewRunWorker(
		logger,
		name+"-worker",
		name,
		locker,
		task,
	)
}

type SubscriberFactory func(context.Context, ResumeKey) Subscriber

// PartitionedWorkers creates workers that will always run because a balancer locker is not provided.
// This assumes that the balancing will be done by the message broker.
func PartitionedWorkers(
	ctx context.Context,
	logger log.Logger,
	lockerFactory LockerFactory,
	subscriberFactory SubscriberFactory,
	topic string, partitions uint32,
	esRepo EventsRepository,
	projection Projection,
) error {
	return PartitionedCompetingWorkers(
		ctx,
		logger,
		lockerFactory,
		subscriberFactory,
		topic, partitions,
		esRepo,
		projection,
	)
}

// PartitionedCompetingWorkers creates workers that will run depending if a lock was acquired or not.
//
// If a locker is provided it is possible to balance workers between the several server instances using a [worker.Balancer]
func PartitionedCompetingWorkers(
	ctx context.Context,
	logger log.Logger,
	lockerFactory LockerFactory,
	subscriberFactory SubscriberFactory,
	topic string, partitions uint32,
	esRepo EventsRepository,
	projection Projection,
) error {
	if partitions <= 1 {
		err := createProjector(
			ctx,
			logger,
			lockerFactory,
			subscriberFactory,
			topic,
			0,
			esRepo,
			projection,
		)
		if err != nil {
			return faults.Wrap(err)
		}
		return nil
	}
	for x := uint32(0); x < partitions; x++ {
		err := createProjector(
			ctx,
			logger,
			lockerFactory,
			subscriberFactory,
			topic,
			x+1,
			esRepo,
			projection,
		)
		if err != nil {
			return faults.Wrap(err)
		}
	}

	return nil
}

// CreateWorker creates a worker that will run if acquires the lock
func createProjector(
	ctx context.Context,
	logger log.Logger,
	lockerFactory LockerFactory,
	subscriberFactory SubscriberFactory,
	topic string,
	partition uint32,
	esRepo EventsRepository,
	projection Projection,
) error {
	t, err := util.NewPartitionedTopic(topic, partition)
	if err != nil {
		return faults.Wrap(err)
	}
	sr, err := NewStreamResume(t, projection.Name())
	if err != nil {
		return faults.Wrap(err)
	}

	Project(
		ctx,
		logger,
		lockerFactory,
		esRepo,
		subscriberFactory(ctx, sr),
		projection,
	)

	return nil
}
