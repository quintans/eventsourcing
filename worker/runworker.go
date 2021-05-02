package worker

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"

	"github.com/quintans/faults"

	"github.com/quintans/eventsourcing/common"
	"github.com/quintans/eventsourcing/lock"
	"github.com/quintans/eventsourcing/log"
	"github.com/quintans/eventsourcing/projection"
	"github.com/quintans/eventsourcing/sink"
	"github.com/quintans/eventsourcing/store"
)

// Tasker is the interface for tasks that need to be balanced among a set of workers
type Tasker interface {
	Run(context.Context) error
	Cancel()
}

// BootMonitor is responsible for refreshing the lease
type RunWorker struct {
	logger log.Logger
	name   string
	locker lock.Locker
	runner Tasker
	cancel context.CancelFunc
	mu     sync.RWMutex
}

func NewRunWorker(logger log.Logger, name string, locker lock.Locker, runner Tasker) *RunWorker {
	return &RunWorker{
		logger: logger,
		name:   name,
		locker: locker,
		runner: runner,
	}
}

func (w *RunWorker) Name() string {
	return w.name
}

func (w *RunWorker) Stop(ctx context.Context) {
	w.logger.Infof("Stopping worker %s", w.name)

	w.mu.Lock()
	if w.cancel != nil {
		w.locker.Unlock(ctx)
		w.cancel()
		w.cancel = nil
	}
	w.mu.Unlock()
}

func (w *RunWorker) IsRunning() bool {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.cancel != nil
}

func (w *RunWorker) Start(ctx context.Context) bool {
	release, _ := w.locker.Lock(ctx)
	if release != nil {
		go func() {
			ctx, cancel := context.WithCancel(ctx)
			select {
			case <-release:
			case <-ctx.Done():
				w.locker.Unlock(ctx)
			}
			cancel()
		}()
		go w.start(ctx)
	}

	return release != nil
}

func (w *RunWorker) start(ctx context.Context) {
	w.logger.Infof("Starting worker %s", w.name)

	ctx2, cancel2 := context.WithCancel(ctx)

	w.mu.Lock()
	w.cancel = cancel2
	w.mu.Unlock()

	// acquired lock
	// OnBoot may take some time to finish since it will be doing synchronisation
	go func() {
		err := w.runner.Run(ctx2)
		if err != nil {
			w.logger.Error("Error while running: ", err)
			cancel2()
			return
		}
	}()
	<-ctx2.Done()
	w.runner.Cancel()
	w.Stop(ctx)
}

type PartitionSlot struct {
	From uint32
	To   uint32
}

func (ps PartitionSlot) Size() uint32 {
	return ps.To - ps.From + 1
}

func ParseSlots(slots []string) ([]PartitionSlot, error) {
	pslots := make([]PartitionSlot, len(slots))
	for k, v := range slots {
		s, err := ParseSlot(v)
		if err != nil {
			return nil, err
		}
		pslots[k] = s
	}
	return pslots, nil
}

func ParseSlot(slot string) (PartitionSlot, error) {
	ps := strings.Split(slot, "-")
	s := PartitionSlot{}
	from, err := strconv.Atoi(ps[0])
	if err != nil {
		return PartitionSlot{}, faults.Wrap(err)
	}
	s.From = uint32(from)
	if len(ps) == 2 {
		to, err := strconv.Atoi(ps[1])
		if err != nil {
			return PartitionSlot{}, faults.Wrap(err)
		}
		s.To = uint32(to)
	} else {
		s.To = s.From
	}
	return s, nil
}

type Task struct {
	run    func(ctx context.Context) (<-chan struct{}, error)
	cancel context.CancelFunc
	done   <-chan struct{}
	mu     sync.RWMutex
}

func NewTask(run func(ctx context.Context) (<-chan struct{}, error)) *Task {
	return &Task{
		run: run,
	}
}

func (t *Task) Run(ctx context.Context) error {
	ctx2, cancel := context.WithCancel(ctx)

	ch, err := t.run(ctx2)
	if err != nil {
		cancel()
		return err
	}

	t.mu.Lock()
	t.cancel = cancel
	t.done = ch
	t.mu.Unlock()

	return nil
}

func (t *Task) Cancel() {
	t.mu.Lock()
	if t.cancel != nil {
		t.cancel()
	}
	// wait for the closing subscriber
	<-t.done

	t.mu.Unlock()
}

type LockerFactory func(lockName string) lock.Locker

type FeederFactory func(partitionLow, partitionHi uint32) store.Feeder

func EventForwarders(ctx context.Context, logger log.Logger, name string, lockerFactory LockerFactory, feederFactory FeederFactory, sinker sink.Sinker, partitionSlots []PartitionSlot) []Worker {
	workers := make([]Worker, len(partitionSlots))
	for i, v := range partitionSlots {
		// feed provider
		feeder := feederFactory(v.From, v.To)

		slotsName := fmt.Sprintf("%d-%d", v.From, v.To)
		workers[i] = NewRunWorker(
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
	StartConsumer(ctx context.Context, resume projection.StreamResume, handler projection.EventHandlerFunc, options ...projection.ConsumerOption) (chan struct{}, error)
}

func ReactorConsumers(ctx context.Context, logger log.Logger, streamName string, lockerFactory LockerFactory, topic string, partitions uint32, consumer Consumer, handler projection.EventHandlerFunc) []Worker {
	workers := make([]Worker, partitions)
	for i := uint32(0); i < partitions; i++ {
		x := i
		name := streamName + "-lock-" + strconv.Itoa(int(x))
		workers[x] = NewRunWorker(
			logger,
			name,
			lockerFactory(name),
			NewTask(func(ctx context.Context) (<-chan struct{}, error) {
				done, err := consumer.StartConsumer(
					ctx,
					projection.StreamResume{
						Topic:  common.TopicWithPartition(topic, x+1),
						Stream: streamName,
					},
					handler,
				)
				if err != nil {
					return nil, faults.Errorf("Unable to start consumer for %s: %w", name, err)
				}

				return done, nil
			}),
		)
	}

	return workers
}
