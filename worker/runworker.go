package worker

import (
	"context"
	"strconv"
	"strings"
	"sync"

	log "github.com/sirupsen/logrus"
)

// Tasker is the interface for tasks that need to be balanced among a set of workers
type Tasker interface {
	Run(context.Context) error
	Cancel()
}

type Locker interface {
	Lock(context.Context) (chan struct{}, error)
	Unlock(context.Context) error
}

type WaitForUnlocker interface {
	WaitForUnlock(context.Context) error
}

// BootMonitor is responsible for refreshing the lease
type RunWorker struct {
	name   string
	runner Tasker
	cancel context.CancelFunc
	mu     sync.RWMutex
}

func NewRunWorker(name string, runner Tasker) *RunWorker {
	return &RunWorker{
		name:   name,
		runner: runner,
	}
}

func (l *RunWorker) Name() string {
	return l.name
}

func (l *RunWorker) Stop() {
	l.mu.Lock()
	if l.cancel != nil {
		l.cancel()
		l.cancel = nil
	}
	l.mu.Unlock()
}

func (l *RunWorker) IsRunning() bool {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.cancel != nil
}

func (l *RunWorker) Start(ctx context.Context) {
	ctx, cancel := context.WithCancel(ctx)

	l.mu.Lock()
	l.cancel = cancel
	l.mu.Unlock()

	defer func() {
		l.Stop()
	}()

	// acquired lock
	// OnBoot may take some time (minutes) to finish since it will be doing synchronisation
	go func() {
		err := l.runner.Run(ctx)
		if err != nil {
			log.Error("Error while running:", err)
		}
	}()
	select {
	case <-ctx.Done():
	}
	l.runner.Cancel()
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
		return PartitionSlot{}, err
	}
	s.From = uint32(from)
	if len(ps) == 2 {
		to, err := strconv.Atoi(ps[1])
		if err != nil {
			return PartitionSlot{}, err
		}
		s.To = uint32(to)
	} else {
		s.To = s.From
	}
	return s, nil
}
