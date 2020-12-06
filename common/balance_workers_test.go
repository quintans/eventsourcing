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
	w1, cancel1 := newBalancer(members)

	time.Sleep(time.Second)
	count := countRunningWorkers(w1)
	require.Equal(t, 4, count)

	// node #2
	w2, cancel2 := newBalancer(members)

	time.Sleep(3 * time.Second)
	count = countRunningWorkers(w1)
	require.Equal(t, 2, count)
	count = countRunningWorkers(w2)
	require.Equal(t, 2, count)

	// node #3
	w3, cancel3 := newBalancer(members)

	time.Sleep(3 * time.Second)
	count1 := countRunningWorkers(w1)
	require.True(t, count1 >= 1 && count1 <= 2)
	count2 := countRunningWorkers(w2)
	require.True(t, count2 >= 1 && count2 <= 2)
	count3 := countRunningWorkers(w3)
	require.True(t, count3 >= 1 && count3 <= 2)

	require.Equal(t, 4, count+count2+count3)

	// after a while we expect to still have he same values
	time.Sleep(3 * time.Second)
	count1B := countRunningWorkers(w1)
	require.Equal(t, count1, count1B)
	count2B := countRunningWorkers(w2)
	require.Equal(t, count2, count2B)
	count3B := countRunningWorkers(w3)
	require.Equal(t, count3, count3B)

	// kill node #1
	cancel1()
	time.Sleep(3 * time.Second)
	count = countRunningWorkers(w2)
	require.Equal(t, 2, count)
	count = countRunningWorkers(w3)
	require.Equal(t, 2, count)

	// kill node #2
	cancel2()
	time.Sleep(3 * time.Second)
	count = countRunningWorkers(w3)
	require.Equal(t, 4, count)

	cancel3()
}

func newBalancer(members *sync.Map) ([]common.LockWorker, context.CancelFunc) {
	ctx, cancel := context.WithCancel(context.Background())
	member := NewInMemMemberList(ctx, members)
	ws := getWorkers()
	go common.BalanceWorkers(ctx, member, ws, time.Second)
	return ws, cancel
}

func countRunningWorkers(workers []common.LockWorker) int {
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
		{
			Lock:   NewInMemLocker(),
			Worker: NewInMemWorker("worker-four"),
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
