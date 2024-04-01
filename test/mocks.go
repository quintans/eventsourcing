package test

import (
	"context"
	"errors"
	"sync"

	"github.com/teris-io/shortid"

	"github.com/quintans/eventsourcing/dist"
	"github.com/quintans/eventsourcing/projection"
	"github.com/quintans/eventsourcing/store"
	"github.com/quintans/eventsourcing/worker"
	"github.com/quintans/faults"
)

type InMemLockerPool struct {
	locks *sync.Map
}

func NewInMemLockerPool() *InMemLockerPool {
	return &InMemLockerPool{
		locks: &sync.Map{},
	}
}

func (l *InMemLockerPool) NewLock(name string) *InMemLocker {
	return &InMemLocker{
		locks: l.locks,
		name:  name,
	}
}

type InMemLocker struct {
	locks *sync.Map
	name  string
}

func (l *InMemLocker) Lock(ctx context.Context) (context.Context, error) {
	ctx, cancel := context.WithCancel(ctx)
	_, loaded := l.locks.LoadOrStore(l.name, cancel)
	if !loaded {
		return ctx, nil
	}
	cancel()
	return nil, dist.ErrLockAlreadyHeld
}

func (l *InMemLocker) Unlock(context.Context) error {
	done, loaded := l.locks.LoadAndDelete(l.name)
	if !loaded {
		return dist.ErrLockNotHeld
	}
	done.(context.CancelFunc)()
	return nil
}

func (l *InMemLocker) WaitForUnlock(context.Context) error {
	done, ok := l.locks.Load(l.name)
	if !ok {
		return nil
	}
	<-done.(chan struct{})
	return nil
}

func (l *InMemLocker) WaitForLock(ctx context.Context) (context.Context, error) {
	for {
		ctx2, err := l.Lock(ctx)
		if errors.Is(err, dist.ErrLockAlreadyAcquired) {
			_ = l.WaitForUnlock(ctx)
			continue
		} else if err != nil {
			return nil, faults.Wrap(err)
		}

		return ctx2, nil
	}
}

type InMemMemberList struct {
	name string
	db   *sync.Map
}

func NewInMemMemberList(ctx context.Context, db *sync.Map) *InMemMemberList {
	name := shortid.MustGenerate()
	db.Store(name, []string{})
	go func() {
		<-ctx.Done()
		db.Delete(name)
	}()
	return &InMemMemberList{
		name: name,
		db:   db,
	}
}

func (m InMemMemberList) Name() string {
	return m.name
}

func (m InMemMemberList) Peers(context.Context) ([]worker.Peer, error) {
	members := []worker.Peer{}
	m.db.Range(func(key, value interface{}) bool {
		members = append(members, worker.Peer{
			Name:    key.(string),
			Workers: value.([]string),
		})
		return true
	})

	return members, nil
}

func (m *InMemMemberList) Register(_ context.Context, workers []string) error {
	m.db.Store(m.name, workers)
	return nil
}

func (m *InMemMemberList) Close(_ context.Context) error {
	m.db.Delete(m.name)
	return nil
}

type InMemResumeStore struct {
	tokens *sync.Map
}

func NewInMemResumeStore() *InMemResumeStore {
	return &InMemResumeStore{
		tokens: &sync.Map{},
	}
}

func (s *InMemResumeStore) GetStreamResumeToken(ctx context.Context, key projection.ResumeKey) (string, error) {
	value, ok := s.tokens.Load(key.String())
	if !ok {
		return "", store.ErrResumeTokenNotFound
	}
	return value.(string), nil
}

func (s *InMemResumeStore) SetStreamResumeToken(ctx context.Context, key projection.ResumeKey, token string) error {
	s.tokens.Store(key.String(), token)
	return nil
}
