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
	logger     log.Logger
	name       string
	group      string
	locker     lock.Locker
	runner     Tasker
	paused     bool
	running    bool
	cancelLock context.CancelFunc
	mu         sync.RWMutex
}

func NewUnbalancedRunWorker(logger log.Logger, name string, group string, runner Tasker) *RunWorker {
	return newRunWorker(logger, name, group, nil, runner)
}

func NewRunWorker(logger log.Logger, name string, group string, locker lock.Locker, runner Tasker) *RunWorker {
	return newRunWorker(logger, name, group, locker, runner)
}

func newRunWorker(logger log.Logger, name string, group string, locker lock.Locker, runner Tasker) *RunWorker {
	logger = logger.WithTags(log.Tags{
		"id": shortid.MustGenerate(),
	})
	return &RunWorker{
		logger: logger,
		name:   name,
		group:  group,
		locker: locker,
		runner: runner,
	}
}

func (w *RunWorker) Name() string {
	return w.name
}

func (w *RunWorker) Group() string {
	return w.group
}

func (w *RunWorker) IsRunning() bool {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.running
}

func (w *RunWorker) IsPaused() bool {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.paused
}

func (w *RunWorker) Start(ctx context.Context) bool {
	w.mu.Lock()
	if w.running {
		w.mu.Unlock()
		return true
	}
	w.mu.Unlock()

	var err error
	var release <-chan struct{}
	if w.locker != nil {
		ctx, cancel := context.WithCancel(context.Background())
		release, err = w.locker.Lock(ctx)
		if err != nil {
			cancel()
			return false
		}
		w.mu.Lock()
		w.cancelLock = cancel
		w.mu.Unlock()
		go func() {
			<-release
			w.Stop(context.Background())
		}()
	} else {
		release = make(chan struct{})
	}

	w.mu.Lock()
	w.start(ctx)
	w.mu.Unlock()

	return true
}

func (w *RunWorker) Stop(ctx context.Context) {
	w.mu.Lock()
	w.stop(ctx, false)
	w.mu.Unlock()
}

func (w *RunWorker) stop(ctx context.Context, hard bool) {
	if w.running {
		w.logger.Infof("Stopping worker '%s'", w.name)
		w.runner.Cancel(ctx, hard)
		if w.locker != nil {
			err := w.locker.Unlock(ctx)
			if err != nil {
				w.logger.WithError(err).Error("Failed to unlock worker '%s'", w.name)
			}
			w.cancelLock()
		}
		w.running = false
	}
}

func (w *RunWorker) start(ctx context.Context) {
	w.logger.Infof("Starting worker '%s'", w.name)

	err := w.runner.Run(ctx)
	if err != nil {
		w.logger.Errorf("Error while running: %+v", err)
		w.Stop(context.Background())
		return
	}
	w.running = true
	w.paused = false
}

func (w *RunWorker) Pause(ctx context.Context, pause bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if pause {
		w.stop(ctx, true)
	}
	w.paused = pause
}

func (w *RunWorker) IsBalanceable() bool {
	return !w.paused && w.isBalanceable()
}

func (w *RunWorker) isBalanceable() bool {
	return w.locker != nil
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
