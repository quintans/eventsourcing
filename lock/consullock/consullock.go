package consullock

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/hashicorp/consul/api"
	"github.com/quintans/faults"

	"github.com/quintans/eventsourcing/lock"
)

var _ lock.Locker = (*Lock)(nil)

type Lock struct {
	client   *api.Client
	sID      string
	lockName string
	expiry   time.Duration
	done     context.CancelFunc
	mu       sync.Mutex
}

func (l *Lock) Lock(ctx context.Context) (context.Context, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.done != nil {
		return nil, faults.Errorf("failed to acquire lock: '%s': %w", l.lockName, lock.ErrLockAlreadyHeld)
	}

	sEntry := &api.SessionEntry{
		Name:     api.DefaultLockSessionName,
		TTL:      l.expiry.String(),
		Behavior: api.SessionBehaviorDelete,
	}
	options := &api.WriteOptions{}
	options = options.WithContext(ctx)
	sID, _, err := l.client.Session().Create(sEntry, options)
	if err != nil {
		return nil, faults.Wrap(err)
	}

	l.sID = sID
	acquireKv := &api.KVPair{
		Session: l.sID,
		Key:     l.lockName,
		Value:   []byte(sID),
	}
	acquired, _, err := l.client.KV().Acquire(acquireKv, options)
	if err != nil {
		return nil, faults.Wrap(err)
	}

	if !acquired {
		_, _ = l.client.Session().Destroy(sID, options)
		return nil, faults.Wrap(lock.ErrLockAlreadyAcquired)
	}

	// auto renew session
	ctx2, cancel := context.WithCancel(ctx)
	l.done = cancel
	go func() {
		err := l.client.Session().RenewPeriodic(sEntry.TTL, sID, &api.WriteOptions{}, ctx2.Done())
		if err != nil {
			l.Unlock(context.Background())
		}
	}()

	return ctx2, nil
}

func (l *Lock) Unlock(ctx context.Context) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.done == nil {
		return faults.Wrap(lock.ErrLockNotHeld)
	}

	lockEnt := &api.KVPair{
		Session: l.sID,
		Key:     l.lockName,
	}
	options := &api.WriteOptions{}
	options = options.WithContext(ctx)
	_, _, err := l.client.KV().Release(lockEnt, options)
	if err != nil {
		return faults.Errorf("failed to release lock: %w", err)
	}

	l.done()
	l.done = nil

	return nil
}

func (l *Lock) WaitForUnlock(ctx context.Context) error {
	opts := &api.QueryOptions{}
	opts = opts.WithContext(ctx)

	done := make(chan error, 1)
	heartbeat := l.expiry / 2

	go func() {
		ticker := time.NewTicker(heartbeat)
		defer ticker.Stop()
		for {
			kv, _, err := l.client.KV().Get(l.lockName, opts)
			if err != nil {
				done <- faults.Wrap(err)
				return
			}
			if kv == nil || len(kv.Value) == 0 {
				done <- nil
				return
			}
			<-ticker.C
		}
	}()
	err := <-done

	return err
}

func (l *Lock) WaitForLock(ctx context.Context) (context.Context, error) {
	for {
		ctx2, err := l.Lock(ctx)
		if errors.Is(err, lock.ErrLockAlreadyAcquired) {
			_ = l.WaitForUnlock(ctx)
			continue
		} else if err != nil {
			return nil, faults.Wrap(err)
		}

		return ctx2, nil
	}
}
