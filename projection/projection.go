package projection

import (
	"context"
	"fmt"

	"github.com/quintans/faults"

	"github.com/quintans/eventsourcing/common"
	"github.com/quintans/eventsourcing/lock"
	"github.com/quintans/eventsourcing/log"
	"github.com/quintans/eventsourcing/worker"
)

type LockerFactory func(lockName string) lock.Locker

type TaskerFactory func(partitionLow, partitionHi uint32) worker.Tasker

// EventForwarderWorkers create workers responsible to forward events to their managed topic partition
// each worker is responsible to forward a range of partitions
func EventForwarderWorkers(ctx context.Context, logger log.Logger, name string, lockerFactory LockerFactory, taskerFactory TaskerFactory, partitionSlots []worker.PartitionSlot) []worker.Worker {
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

// EventForwarderWorker creates a single worker responsible of forwarding
func EventForwarderWorker(ctx context.Context, logger log.Logger, name string, lockerFactory LockerFactory, feeder worker.Tasker) worker.Worker {
	return worker.NewRunWorker(
		logger,
		name+"-worker",
		name,
		lockerFactory(name+"-lock"),
		feeder,
	)
}

type ConsumerFactory func(context.Context, ResumeKey) (Consumer, error)

// UnmanagedWorkers creates workers that will always run regardless if a lock was acquired or not.
// There will be no balancing of workers between the several server instances
func UnmanagedWorkers(ctx context.Context, logger log.Logger, streamName string, topic string, partitions uint32, consumerFactory ConsumerFactory, handler EventHandlerFunc) ([]worker.Worker, error) {
	lockerFactory := func(lockName string) lock.Locker {
		return nil
	}
	return ManagedWorkers(ctx, logger, streamName, topic, partitions, lockerFactory, consumerFactory, handler)
}

// ManagedWorkers creates workers that will run depending if a lock was acquired or not.
// They will be balanced of over the several server instances
func ManagedWorkers(ctx context.Context, logger log.Logger, streamName string, topic string, partitions uint32, lockerFactory LockerFactory, consumerFactory ConsumerFactory, handler EventHandlerFunc) ([]worker.Worker, error) {
	if partitions <= 1 {
		w, err := createWorker(ctx, logger, streamName, topic, 0, lockerFactory, consumerFactory, handler)
		if err != nil {
			return nil, faults.Wrap(err)
		}
		return []worker.Worker{w}, nil
	}
	workers := make([]worker.Worker, partitions)
	for x := uint32(0); x < partitions; x++ {
		var err error
		workers[x], err = createWorker(ctx, logger, streamName, topic, x+1, lockerFactory, consumerFactory, handler)
		if err != nil {
			return nil, faults.Wrap(err)
		}
	}

	return workers, nil
}

// ManagedWorker creates a single managed worker
func ManagedWorker(ctx context.Context, logger log.Logger, streamName string, topic string, lockerFactory LockerFactory, consumerFactory ConsumerFactory, handler EventHandlerFunc) (worker.Worker, error) {
	return createWorker(ctx, logger, streamName, topic, 0, lockerFactory, consumerFactory, handler)
}

func createWorker(ctx context.Context, logger log.Logger, streamName string, topic string, partition uint32, lockerFactory LockerFactory, consumerFactory ConsumerFactory, handler EventHandlerFunc) (worker.Worker, error) {
	topicWithPartition := common.TopicWithPartition(topic, partition)
	sr, err := NewStreamResume(topicWithPartition, streamName)
	if err != nil {
		return nil, faults.Wrap(err)
	}
	consumer, err := consumerFactory(ctx, sr)
	if err != nil {
		return nil, faults.Wrap(err)
	}
	name := streamName + "-lock-" + topicWithPartition
	worker := worker.NewRunWorker(
		logger,
		name,
		streamName,
		lockerFactory(name),
		worker.NewTask(
			func(ctx context.Context) error {
				err := consumer.StartConsumer(ctx, handler)
				return faults.Wrapf(err, "Unable to start consumer for %s-%s", streamName, topicWithPartition)
			},
			func(ctx context.Context, hard bool) {
				consumer.StopConsumer(ctx, hard)
			},
		),
	)
	return worker, nil
}
