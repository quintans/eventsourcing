package common

import (
	"context"
	"strconv"
	"strings"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
)

type Booter interface {
	OnBoot(context.Context) error
	Wait() <-chan struct{} // block if not ready
	Cancel()
}

type Locker interface {
	Lock() (chan struct{}, error)
	Unlock() error
}

// BootMonitor is responsible for refreshing the lease
type BootMonitor struct {
	name     string
	refresh  time.Duration
	lockable Booter
	cancel   context.CancelFunc
	mu       sync.RWMutex
}

type Option func(r *BootMonitor)

func WithRefreshInterval(refresh time.Duration) func(r *BootMonitor) {
	return func(r *BootMonitor) {
		r.refresh = refresh
	}
}

func NewBootMonitor(name string, lockable Booter, options ...Option) BootMonitor {
	l := BootMonitor{
		name:     name,
		refresh:  10 * time.Second,
		lockable: lockable,
	}

	for _, o := range options {
		o(&l)
	}

	return l
}

func (l BootMonitor) Name() string {
	return l.name
}

func (l BootMonitor) IsRunning() bool {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.cancel != nil
}

func (l *BootMonitor) Stop() {
	l.mu.Lock()
	if l.cancel != nil {
		l.cancel()
		l.cancel = nil
	}
	l.mu.Unlock()
}

func (l BootMonitor) Start(ctx context.Context, lockReleased chan struct{}) {
	ctx, cancel := context.WithCancel(ctx)

	l.mu.Lock()
	l.cancel = cancel
	l.mu.Unlock()

	defer func() {
		l.Stop()
	}()

	frozen := l.lockable.Wait()
	// acquired lock
	// OnBoot may take some time (minutes) to finish since it will be doing synchronisation
	go func() {
		err := l.lockable.OnBoot(ctx)
		if err != nil {
			log.Error("Error booting:", err)
		}
	}()
	select {
	// happens when the latch was cancelled
	case <-ctx.Done():
	case <-frozen:
	case <-lockReleased:
		l.lockable.Cancel()
	}
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
