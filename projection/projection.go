package projection

import (
	"context"
	"fmt"
	"strconv"
	"sync"

	"github.com/quintans/faults"

	"github.com/quintans/eventsourcing/common"
	"github.com/quintans/eventsourcing/lock"
	"github.com/quintans/eventsourcing/log"
	"github.com/quintans/eventsourcing/sink"
	"github.com/quintans/eventsourcing/store"
	"github.com/quintans/eventsourcing/worker"
)

type LockerFactory func(lockName string) lock.Locker

type FeederFactory func(partitionLow, partitionHi uint32) store.Feeder

// EventForwarderWorkers create workers responsible to forward events to their managed topic partition
// each worker is responsible to forward a range of partitions
func EventForwarderWorkers(ctx context.Context, logger log.Logger, name string, lockerFactory LockerFactory, feederFactory FeederFactory, sinker sink.Sinker, partitionSlots []worker.PartitionSlot) []worker.Worker {
	workers := make([]worker.Worker, len(partitionSlots))
	for i, v := range partitionSlots {
		// feed provider
		feeder := feederFactory(v.From, v.To)

		slotsName := fmt.Sprintf("%d-%d", v.From, v.To)
		workers[i] = worker.NewRunWorker(
			logger,
			name+"-worker-"+slotsName,
			lockerFactory(name+"-lock-"+slotsName),
			store.NewForwarder(
				logger,
				name+"-"+slotsName,
				feeder,
				sinker,
			))
	}

	return workers
}

// EventForwarderWorker creates a single worker responsible of forwarding
func EventForwarderWorker(ctx context.Context, logger log.Logger, name string, lockerFactory LockerFactory, feeder store.Feeder, sinker sink.Sinker) worker.Worker {
	return worker.NewRunWorker(
		logger,
		name+"-worker",
		lockerFactory(name+"-lock"),
		store.NewForwarder(
			logger,
			name,
			feeder,
			sinker,
		))
}

type ConsumerFactory func(context.Context, StreamResume) (Consumer, error)

type dummyLocker struct {
	done chan struct{}
	mu   sync.RWMutex
}

func (d *dummyLocker) Lock(context.Context) (<-chan struct{}, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.done != nil {
		return d.done, nil
	}
	d.done = make(chan struct{})
	return d.done, nil
}

func (d *dummyLocker) Unlock(context.Context) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.done != nil {
		close(d.done)
	}
	return nil
}

func UnmanagedWorkers(ctx context.Context, logger log.Logger, streamName string, topic string, partitions uint32, consumerFactory ConsumerFactory, handler EventHandlerFunc) ([]worker.Worker, error) {
	lockerFactory := func(lockName string) lock.Locker {
		return &dummyLocker{}
	}
	return ManagedWorkers(ctx, logger, streamName, topic, partitions, lockerFactory, consumerFactory, handler)
}

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

func ManagedWorker(ctx context.Context, logger log.Logger, streamName string, topic string, lockerFactory LockerFactory, consumerFactory ConsumerFactory, handler EventHandlerFunc) (worker.Worker, error) {
	return createWorker(ctx, logger, streamName, topic, 0, lockerFactory, consumerFactory, handler)
}

func createWorker(ctx context.Context, logger log.Logger, streamName string, topic string, partition uint32, lockerFactory LockerFactory, consumerFactory ConsumerFactory, handler EventHandlerFunc) (worker.Worker, error) {
	sr, err := NewStreamResume(common.TopicWithPartition(topic, 0), streamName)
	if err != nil {
		return nil, faults.Wrap(err)
	}
	consumer, err := consumerFactory(ctx, sr)
	if err != nil {
		return nil, faults.Wrap(err)
	}
	name := streamName + "-lock-" + strconv.Itoa(int(partition))
	worker := worker.NewRunWorker(
		logger,
		name,
		lockerFactory(name),
		worker.NewTask(func(ctx context.Context) (<-chan struct{}, error) {
			done, err := consumer.StartConsumer(ctx, handler)
			if err != nil {
				return nil, faults.Errorf("Unable to start consumer for %s-%d: %w", streamName, partition, err)
			}

			return done, nil
		}),
	)
	return worker, nil
}
