package worker

import (
	"context"
	"strconv"
	"strings"
	"sync"

	"github.com/quintans/faults"
	"github.com/teris-io/shortid"

	"github.com/quintans/eventsourcing/lock"
	"github.com/quintans/eventsourcing/log"
)

// Tasker is the interface for tasks that need to be balanced among a set of workers
type Tasker interface {
	Run(context.Context) error
	Cancel(ctx context.Context, hard bool)
}

var _ Worker = (*RunWorker)(nil)

// RunWorker is responsible for refreshing the lease
type RunWorker struct {
	logger log.Logger
	name   string
	locker lock.Locker
	runner Tasker
	done   chan struct{}
	mu     sync.RWMutex
}

func NewRunWorker(logger log.Logger, name string, locker lock.Locker, runner Tasker) *RunWorker {
	logger = logger.WithTags(log.Tags{
		"id": shortid.MustGenerate(),
	})
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

func (w *RunWorker) IsRunning() bool {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.done != nil
}

func (w *RunWorker) Start(ctx context.Context) bool {
	release, err := w.locker.Lock(ctx)
	if err != nil {
		return false
	}
	done := make(chan struct{})
	w.mu.Lock()
	w.done = done
	w.mu.Unlock()
	go func() {
		select {
		case <-done:
			return
		case <-release:
		case <-ctx.Done():
		}
		w.Stop(context.Background(), false)
	}()
	go w.start(ctx, done)

	return true
}

func (w *RunWorker) Stop(ctx context.Context, hard bool) {
	w.mu.Lock()
	if w.done != nil {
		w.locker.Unlock(ctx)
		w.logger.Infof("Stopping worker '%s'", w.name)
		w.runner.Cancel(ctx, hard)
		close(w.done)
		w.done = nil
	}
	w.mu.Unlock()
}

func (w *RunWorker) start(ctx context.Context, done chan struct{}) {
	w.logger.Infof("Starting worker '%s'", w.name)

	// acquired lock
	// OnBoot may take some time to finish since it will be doing synchronisation
	go func() {
		err := w.runner.Run(ctx)
		if err != nil {
			w.logger.Errorf("Error while running: %+v", err)
			w.Stop(ctx, false)
			return
		}
	}()
	<-done
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

var _ Tasker = (*Task)(nil)

type Task struct {
	run    func(ctx context.Context) error
	cancel func(ctx context.Context, hard bool)
}

func NewTask(run func(ctx context.Context) error, cancel func(ctx context.Context, hard bool)) *Task {
	return &Task{
		run:    run,
		cancel: cancel,
	}
}

func (t *Task) Run(ctx context.Context) error {
	return t.run(ctx)
}

func (t *Task) Cancel(ctx context.Context, hard bool) {
	t.cancel(ctx, hard)
}
