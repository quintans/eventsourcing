package projection

import (
	"context"
	"fmt"
	"strconv"

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

type Consumer interface {
	StartConsumer(ctx context.Context, resume StreamResume, handler EventHandlerFunc, options ...ConsumerOption) (chan struct{}, error)
}

func ReactorConsumerWorkers(ctx context.Context, logger log.Logger, streamName string, lockerFactory LockerFactory, topic string, partitions uint32, consumer Consumer, handler EventHandlerFunc) ([]worker.Worker, []StreamResume) {
	if partitions <= 1 {
		w, r := reactorConsumerWorker(ctx, logger, streamName, lockerFactory, topic, 0, consumer, handler)
		return []worker.Worker{w}, []StreamResume{r}
	}
	workers := make([]worker.Worker, partitions)
	resumes := make([]StreamResume, partitions)
	for x := uint32(0); x < partitions; x++ {
		w, r := reactorConsumerWorker(ctx, logger, streamName, lockerFactory, topic, x+1, consumer, handler)
		resumes[x] = r
		workers[x] = w
	}

	return workers, resumes
}

func ReactorConsumerWorker(ctx context.Context, logger log.Logger, streamName string, lockerFactory LockerFactory, topic string, consumer Consumer, handler EventHandlerFunc) (worker.Worker, StreamResume) {
	return reactorConsumerWorker(ctx, logger, streamName, lockerFactory, topic, 0, consumer, handler)
}

func reactorConsumerWorker(ctx context.Context, logger log.Logger, streamName string, lockerFactory LockerFactory, topic string, partition uint32, consumer Consumer, handler EventHandlerFunc) (worker.Worker, StreamResume) {
	name := streamName + "-lock-" + strconv.Itoa(int(partition))
	resume := StreamResume{
		Topic:  common.TopicWithPartition(topic, partition),
		Stream: streamName,
	}
	worker := worker.NewRunWorker(
		logger,
		name,
		lockerFactory(name),
		worker.NewTask(func(ctx context.Context) (<-chan struct{}, error) {
			done, err := consumer.StartConsumer(
				ctx,
				resume,
				handler,
			)
			if err != nil {
				return nil, faults.Errorf("Unable to start consumer for %s-%d: %w", streamName, partition, err)
			}

			return done, nil
		}),
	)
	return worker, resume
}
