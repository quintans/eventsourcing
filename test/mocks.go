package test

import (
	"context"
	"sync"

	"github.com/teris-io/shortid"

	"github.com/quintans/eventsourcing/lock"
	"github.com/quintans/eventsourcing/projection"
	"github.com/quintans/eventsourcing/worker"
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

func (l *InMemLocker) Lock(context.Context) (<-chan struct{}, error) {
	done := make(chan struct{})
	_, loaded := l.locks.LoadOrStore(l.name, done)
	if !loaded {
		return done, nil
	}

	return nil, lock.ErrLockAlreadyHeld
}

func (l *InMemLocker) Unlock(context.Context) error {
	done, loaded := l.locks.LoadAndDelete(l.name)
	if !loaded {
		return lock.ErrLockNotHeld
	}
	close(done.(chan struct{}))
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

func (m InMemMemberList) List(context.Context) ([]worker.MemberWorkers, error) {
	members := []worker.MemberWorkers{}
	m.db.Range(func(key, value interface{}) bool {
		members = append(members, worker.MemberWorkers{
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

func (m *InMemMemberList) Unregister(_ context.Context) error {
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
		return "", nil
	}
	return value.(string), nil
}

func (s *InMemResumeStore) SetStreamResumeToken(ctx context.Context, key projection.ResumeKey, token string) error {
	s.tokens.Store(key.String(), token)
	return nil
}
