package lock

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/hashicorp/consul/api"
	"github.com/quintans/faults"
)

type ConsulLockPool struct {
	client *api.Client
}

func NewConsulLockPool(consulAddress string) (ConsulLockPool, error) {
	api.DefaultConfig()
	client, err := api.NewClient(&api.Config{Address: consulAddress})
	if err != nil {
		return ConsulLockPool{}, err
	}

	if err != nil {
		return ConsulLockPool{}, fmt.Errorf("session create err: %v", err)
	}

	return ConsulLockPool{
		client: client,
	}, nil
}

func (p ConsulLockPool) NewLock(lockName string, expiry time.Duration) *ConsulLock {
	return &ConsulLock{
		client:   p.client,
		lockName: lockName,
		expiry:   expiry,
	}
}

type ConsulLock struct {
	client   *api.Client
	sID      string
	lockName string
	expiry   time.Duration
	done     chan struct{}
	mu       sync.Mutex
}

func (l *ConsulLock) Lock(ctx context.Context) (chan struct{}, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.done != nil {
		return nil, fmt.Errorf("this lock is already acquire: '%s'", l.lockName)
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
		l.client.Session().Destroy(sID, options)
		return nil, nil
	}

	// auto renew session
	l.done = make(chan struct{})
	go func() {
		// we use a new options because context may no longer be usable
		l.mu.Lock()
		done := l.done
		l.mu.Unlock()
		err := l.client.Session().RenewPeriodic(sEntry.TTL, sID, &api.WriteOptions{}, done)
		if err != nil {
			l.Unlock(context.Background())
		}
	}()

	return l.done, nil
}

func (l *ConsulLock) Unlock(ctx context.Context) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.done == nil {
		return nil
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

func (l *ConsulLock) WaitForUnlock(ctx context.Context) error {
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
