package worker

import (
	"fmt"
	"log/slog"

	"github.com/quintans/eventsourcing/lock"
)

type LockerFactory func(lockName string) lock.Locker

type TaskerFactory func(partitionLow, partitionHi uint32) Task

// PartitionedEventForwarderWorkers create workers responsible to forward events to their managed topic partition
// each worker is responsible to forward a range of partitions
func PartitionedEventForwarders(logger *slog.Logger, name string, lockerFactory LockerFactory, taskerFactory TaskerFactory, partitionSlots []PartitionSlot) []Worker {
	workers := make([]Worker, len(partitionSlots))
	for i, v := range partitionSlots {
		slotsName := fmt.Sprintf("%d-%d", v.From, v.To)

		var locker lock.Locker
		if lockerFactory != nil {
			locker = lockerFactory(name + "-lock-" + slotsName)
		}

		workers[i] = NewRun(
			logger,
			name+"-"+slotsName,
			name,
			locker,
			taskerFactory(v.From, v.To),
		)
	}

	return workers
}

// EventForwarderWorker creates a single worker responsible of forwarding
func EventForwarder(logger *slog.Logger, name string, lockerFactory LockerFactory, task Task) Worker {
	var locker lock.Locker
	if lockerFactory != nil {
		locker = lockerFactory(name + "-lock")
	}

	return NewRun(
		logger,
		name,
		name,
		locker,
		task,
	)
}
