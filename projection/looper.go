package projection

import (
	"context"
	"time"
)

type Booter interface {
	OnBoot(context.Context) error
	Wait() <-chan struct{} // block if not ready
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

type Looper struct {
	refresh  time.Duration
	locker   Locker
	lockable Booter
}

type Option func(r *Looper)

func WithLock(locker Locker) func(r *Looper) {
	return func(r *Looper) {
		r.locker = locker
	}
}

func WithRefreshInterval(refresh time.Duration) func(r *Looper) {
	return func(r *Looper) {
		r.refresh = refresh
	}
}

func NewLooper(lockable Booter, options ...Option) Looper {
	l := Looper{
		refresh:  10 * time.Second,
		lockable: lockable,
	}

	for _, o := range options {
		o(&l)
	}

	return l
}

func (l Looper) Start(ctx context.Context) {
	for {
		release := l.lockable.Wait()
		ok := true
		if l.locker != nil {
			ok, _ = l.locker.Lock()
		}
		if ok {
			// acquired lock
			ctx, cancel := context.WithCancel(ctx)
			_ = l.lockable.OnBoot(ctx)
			loop := true
			for loop {
				switch l.waitOn(ctx.Done(), release) {
				case Done:
					cancel()
					return
				case Timedout:
					loop, _ = l.locker.Extend()
				case Released:
					l.locker.Unlock()
					loop = false
				}
			}
			cancel()
		} else if choice := l.waitOn(ctx.Done(), release); choice == Done {
			return
		}
	}
}

func (l Looper) waitOn(done <-chan struct{}, release <-chan struct{}) WaitResult {
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
