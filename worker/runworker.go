package worker

import (
	"context"
	"strconv"
	"strings"
	"sync"

	"github.com/quintans/faults"

	"github.com/quintans/eventsourcing/lock"
	"github.com/quintans/eventsourcing/log"
)

// Tasker is the interface for tasks that need to be balanced among a set of workers
type Tasker interface {
	Run(context.Context) error
	Cancel()
}

// RunWorker is responsible for refreshing the lease
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
	w.mu.Lock()
	if w.cancel != nil {
		w.locker.Unlock(ctx)
		w.logger.Infof("Stopping worker %s", w.name)
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
	release, err := w.locker.Lock(ctx)
	if err != nil {
		return false
	}
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

	return true
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
