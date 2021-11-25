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

type Consumer interface {
	StartConsumer(ctx context.Context, resume StreamResume, handler EventHandlerFunc, options ...ConsumerOption) (chan struct{}, error)
}

func ReactorConsumerWorkers(ctx context.Context, logger log.Logger, streamName string, lockerFactory LockerFactory, topic string, partitions uint32, consumer Consumer, handler EventHandlerFunc) ([]worker.Worker, []StreamResume) {
	workers := make([]worker.Worker, partitions)
	resumes := make([]StreamResume, partitions)
	for i := uint32(0); i < partitions; i++ {
		x := i
		name := streamName + "-lock-" + strconv.Itoa(int(x))
		resume := StreamResume{
			Topic:  common.TopicWithPartition(topic, x+1),
			Stream: streamName,
		}
		resumes[x] = resume
		workers[x] = worker.NewRunWorker(
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
					return nil, faults.Errorf("Unable to start consumer for %s: %w", name, err)
				}

				return done, nil
			}),
		)
	}

	return workers, resumes
}
