package common

import (
	"context"
	"time"

	log "github.com/sirupsen/logrus"
)

type Booter interface {
	OnBoot(context.Context) error
	Wait() <-chan struct{} // block if not ready
	Cancel()
}

type Locker interface {
	Lock() (bool, error)
	Extend() (bool, error)
	Unlock() (bool, error)
}

type WaitResult int

const (
	Done WaitResult = iota + 1
	Timedout
	Released
)

// BootMonitor is responsible for refreshing the lease
type BootMonitor struct {
	refresh  time.Duration
	locker   Locker
	lockable Booter
}

type Option func(r *BootMonitor)

func WithLock(locker Locker) func(r *BootMonitor) {
	return func(r *BootMonitor) {
		r.locker = locker
	}
}

func WithRefreshInterval(refresh time.Duration) func(r *BootMonitor) {
	return func(r *BootMonitor) {
		r.refresh = refresh
	}
}

func NewBootMonitor(lockable Booter, options ...Option) BootMonitor {
	l := BootMonitor{
		refresh:  10 * time.Second,
		lockable: lockable,
	}

	for _, o := range options {
		o(&l)
	}

	return l
}

func (l BootMonitor) Start(ctx context.Context) {
	for {
		release := l.lockable.Wait()
		ok := true
		if l.locker != nil {
			ok, _ = l.locker.Lock()
		}
		if ok {
			// acquired lock
			// OnBoot may take some time (minutes) to finish since it will be doing synchronisation
			go func() {
				err := l.lockable.OnBoot(ctx)
				if err != nil {
					log.Error("Error booting:", err)
				}
			}()
			loop := true
			for loop {
				switch l.waitOn(ctx.Done(), release) {
				case Done:
					return
				case Timedout:
					loop, _ = l.locker.Extend()
					if !loop {
						l.lockable.Cancel()
					}
				case Released:
					l.locker.Unlock()
					loop = false
				}
			}
		} else if choice := l.waitOn(ctx.Done(), release); choice == Done {
			return
		}
	}
}

func (l BootMonitor) waitOn(done <-chan struct{}, release <-chan struct{}) WaitResult {
	if l.locker != nil {
		timer := time.NewTimer(l.refresh)
		defer timer.Stop()
		select {
		// happens when the latch was cancelled
		case <-done:
			return Done
		case <-timer.C:
			return Timedout
		case <-release:
			return Released
		}
	}

	select {
	// happens when the latch was cancelled
	case <-done:
		return Done
	case <-release:
		return Released
	}
}
