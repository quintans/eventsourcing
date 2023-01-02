package consullock

import (
	"context"
	"fmt"
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
	done     chan struct{}
	mu       sync.Mutex
}

func (l *Lock) Lock(ctx context.Context) (<-chan struct{}, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.done != nil {
		return nil, fmt.Errorf("failed to acquire lock: '%s': %w", l.lockName, lock.ErrLockAlreadyHeld)
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
	done := make(chan struct{})
	l.done = done
	go func() {
		// we use a new options because context may no longer be usable
		err := l.client.Session().RenewPeriodic(sEntry.TTL, sID, &api.WriteOptions{}, done)
		if err != nil {
			l.Unlock(context.Background())
		}
	}()

	return l.done, nil
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

	close(l.done)
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
