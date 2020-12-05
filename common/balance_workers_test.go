package common_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/quintans/eventstore/common"
	"github.com/stretchr/testify/require"
)

func TestMembersList(t *testing.T) {
	members := &sync.Map{}

	// node #1
	ctx1, cancel1 := context.WithCancel(context.Background())
	member1 := NewInMemMemberList(ctx1, members)
	w1 := getWorkers()
	go common.BalanceWorkers(ctx1, member1, w1, time.Second)

	time.Sleep(time.Second)
	count := countWorkersRunning(w1)
	require.Equal(t, 3, count)

	// node #2
	ctx2, cancel2 := context.WithCancel(context.Background())
	member2 := NewInMemMemberList(ctx2, members)
	w2 := getWorkers()
	go common.BalanceWorkers(ctx2, member2, w2, time.Second)

	time.Sleep(3 * time.Second)
	count = countWorkersRunning(w1)
	require.True(t, count != 0)
	count2 := countWorkersRunning(w2)
	require.True(t, count2 != 0)

	require.Equal(t, 3, count+count2)

	// kill node #1
	cancel1()
	time.Sleep(3 * time.Second)
	count = countWorkersRunning(w1)
	require.Equal(t, 0, count)
	count = countWorkersRunning(w2)
	require.Equal(t, 3, count)

	cancel2()
}

func countWorkersRunning(workers []common.LockWorker) int {
	count := 0
	for _, w := range workers {
		if w.Worker.IsRunning() {
			count++
		}
	}
	return count
}

func getWorkers() []common.LockWorker {
	return []common.LockWorker{
		{
			Lock:   NewInMemLocker(),
			Worker: NewInMemWorker("worker-one"),
		},
		{
			Lock:   NewInMemLocker(),
			Worker: NewInMemWorker("worker-two"),
		},
		{
			Lock:   NewInMemLocker(),
			Worker: NewInMemWorker("worker-three"),
		},
	}
}

type InMemLocker struct {
	done chan struct{}
}

func NewInMemLocker() *InMemLocker {
	return &InMemLocker{}
}

func (l *InMemLocker) Lock() (chan struct{}, error) {
	l.done = make(chan struct{})
	return l.done, nil
}

func (l *InMemLocker) Unlock() error {
	close(l.done)
	return nil
}

type InMemWorker struct {
	name    string
	running bool
	mu      sync.RWMutex
}

func NewInMemWorker(name string) *InMemWorker {
	return &InMemWorker{
		name: name,
	}
}

func (w *InMemWorker) Name() string {
	w.mu.RLock()
	defer w.mu.RUnlock()

	return w.name
}

func (w *InMemWorker) IsRunning() bool {
	w.mu.RLock()
	defer w.mu.RUnlock()

	return w.running
}

func (w *InMemWorker) Start(ctx context.Context, released chan struct{}) {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.running = true
	go func() {
		select {
		case <-ctx.Done():
		case <-released:
		}
		w.Stop()
	}()
}

func (w *InMemWorker) Stop() {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.running = false
}

type InMemMemberList struct {
	name string
	db   *sync.Map
}

func NewInMemMemberList(ctx context.Context, db *sync.Map) *InMemMemberList {
	name := uuid.New().String()
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

func (m InMemMemberList) List(context.Context) ([]common.MemberWorkers, error) {
	members := []common.MemberWorkers{}
	m.db.Range(func(key, value interface{}) bool {
		members = append(members, common.MemberWorkers{
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
