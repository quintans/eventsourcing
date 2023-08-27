package worker

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"sync"

	"github.com/quintans/faults"
	"github.com/teris-io/shortid"

	"github.com/quintans/eventsourcing/lock"
	"github.com/quintans/eventsourcing/log"
)

type Task func(ctx context.Context) error

var _ Worker = (*RunWorker)(nil)

// RunWorker is responsible for refreshing the lease
type RunWorker struct {
	logger     log.Logger
	name       string
	group      string
	locker     lock.Locker
	task       Task
	cancel     context.CancelFunc
	cancelLock context.CancelFunc
	mu         sync.RWMutex
}

func NewRunWorker(logger log.Logger, name, group string, locker lock.Locker, task Task) *RunWorker {
	return newRunWorker(logger, name, group, locker, task)
}

func newRunWorker(logger log.Logger, name, group string, locker lock.Locker, task Task) *RunWorker {
	logger = logger.WithTags(log.Tags{
		"id": "worker-" + shortid.MustGenerate(),
	})
	return &RunWorker{
		logger: logger,
		name:   name,
		group:  group,
		locker: locker,
		task:   task,
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
	return w.isRunning()
}

func (w *RunWorker) isRunning() bool {
	return w.cancel != nil
}

// Start attempts to execute the worker in a separate goroutine.
// It returns true if it able to acquire the lock to execute, false otherwise.
func (w *RunWorker) Start(ctx context.Context) (bool, error) {
	if w.IsRunning() {
		return true, nil
	}

	if w.locker != nil {
		ctx, cancel := context.WithCancel(ctx)
		release, err := w.locker.Lock(ctx)
		if err != nil {
			cancel()
			if errors.Is(err, lock.ErrLockAlreadyAcquired) {
				return false, nil
			} else if errors.Is(err, lock.ErrLockAlreadyHeld) {
				return true, nil
			} else {
				return false, faults.Wrap(err)
			}
		}
		w.mu.Lock()
		w.cancelLock = cancel
		w.mu.Unlock()
		go func() {
			<-release.Done()
			w.Stop(context.Background())
		}()
	}

	w.mu.Lock()
	w.start(ctx)
	w.mu.Unlock()
	return true, nil
}

func (w *RunWorker) Stop(ctx context.Context) {
	w.mu.Lock()
	w.stop(ctx)
	w.mu.Unlock()
}

func (w *RunWorker) stop(ctx context.Context) {
	if w.isRunning() {
		w.logger.Infof("Stopping worker '%s'", w.name)
		w.cancel()
		if w.locker != nil {
			err := w.locker.Unlock(ctx)
			if err != nil {
				w.logger.WithError(err).Error("Failed to unlock worker '%s'", w.name)
			}
			w.cancelLock()
		}
		w.cancel = nil
	}
}

func (w *RunWorker) start(ctx context.Context) {
	ctx, cancel := context.WithCancel(ctx)

	w.logger.Infof("Starting worker '%s'", w.name)

	go func() {
		err := w.task(ctx)
		if err != nil {
			w.logger.WithError(err).Errorf("Error while running")
			w.Stop(context.Background())
			return
		}
	}()
	w.cancel = cancel
}

func (w *RunWorker) IsBalanceable() bool {
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
