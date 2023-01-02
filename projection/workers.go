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

type LockerFactory func(lockName string) lock.Locker

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

type ConsumerFactory func(context.Context, ResumeKey) (Consumer, error)

// SoloWorkers creates workers that will always run because a locker is not provided.
//
// Since a locker is not provided it is not possible to balance workers between the several server instances using a [worker.Balancer]
func SoloWorkers(ctx context.Context, logger log.Logger, streamName, topic string, partitions uint32, consumerFactory ConsumerFactory, handler EventHandlerFunc) ([]worker.Worker, error) {
	lockerFactory := func(lockName string) lock.Locker {
		return nil
	}
	return PartitionedCompetingWorkers(ctx, logger, streamName, topic, partitions, lockerFactory, consumerFactory, handler)
}

// PartitionedCompetingWorkers creates workers that will run depending if a lock was acquired or not.
//
// If a locker is provided it is possible to balance workers between the several server instances using a [worker.Balancer]
func PartitionedCompetingWorkers(ctx context.Context, logger log.Logger, streamName, topic string, partitions uint32, lockerFactory LockerFactory, consumerFactory ConsumerFactory, handler EventHandlerFunc) ([]worker.Worker, error) {
	if partitions <= 1 {
		w, err := CreateWorker(ctx, logger, streamName, util.NewTopic(topic), lockerFactory, consumerFactory, handler)
		if err != nil {
			return nil, faults.Wrap(err)
		}
		return []worker.Worker{w}, nil
	}
	workers := make([]worker.Worker, partitions)
	for x := uint32(0); x < partitions; x++ {
		var err error
		workers[x], err = CreateWorker(ctx, logger, streamName, util.NewPartitionedTopic(topic, x+1), lockerFactory, consumerFactory, handler)
		if err != nil {
			return nil, faults.Wrap(err)
		}
	}

	return workers, nil
}

// ManagedWorker creates a single managed worker
func ManagedWorker(ctx context.Context, logger log.Logger, streamName, topic string, lockerFactory LockerFactory, consumerFactory ConsumerFactory, handler EventHandlerFunc) (worker.Worker, error) {
	return CreateWorker(ctx, logger, streamName, util.NewTopic(topic), lockerFactory, consumerFactory, handler)
}

// CreateWorker creates a worker that will run if acquires the lock
func CreateWorker(ctx context.Context, logger log.Logger, streamName string, topic util.Topic, lockerFactory LockerFactory, consumerFactory ConsumerFactory, handler EventHandlerFunc) (worker.Worker, error) {
	sr, err := NewStreamResume(topic, streamName)
	if err != nil {
		return nil, faults.Wrap(err)
	}
	consumer, err := consumerFactory(ctx, sr)
	if err != nil {
		return nil, faults.Wrap(err)
	}
	name := streamName + "-lock-" + topic.String()
	wrk := worker.NewRunWorker(
		logger,
		name,
		streamName,
		lockerFactory(name),
		worker.NewTask(
			func(ctx context.Context) error {
				err := consumer.StartConsumer(ctx, handler)
				return faults.Wrapf(err, "Unable to start consumer for %s-%s", streamName, topic)
			},
			func(ctx context.Context, hard bool) {
				consumer.StopConsumer(ctx, hard)
			},
		),
	)
	return wrk, nil
}
