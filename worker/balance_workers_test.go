package worker_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"

	"github.com/quintans/eventsourcing/lock"
	"github.com/quintans/eventsourcing/log"
	"github.com/quintans/eventsourcing/worker"
)

var locks = &sync.Map{}

func TestMembersList(t *testing.T) {
	members := &sync.Map{}

	// node #1
	w1, cancel1 := newBalancer(members)

	time.Sleep(time.Second)
	count := countRunningWorkers(w1)
	require.Equal(t, 4, count)

	// node #2
	w2, cancel2 := newBalancer(members)

	time.Sleep(time.Second)
	count = countRunningWorkers(w1)
	require.Equal(t, 2, count)
	count = countRunningWorkers(w2)
	require.Equal(t, 2, count)

	// node #3
	w3, cancel3 := newBalancer(members)

	time.Sleep(time.Second)
	count1 := countRunningWorkers(w1)
	require.True(t, count1 >= 1 && count1 <= 2)
	count2 := countRunningWorkers(w2)
	require.True(t, count2 >= 1 && count2 <= 2)
	count3 := countRunningWorkers(w3)
	require.True(t, count3 >= 1 && count3 <= 2)

	require.Equal(t, 4, count+count2+count3)

	// after a while we expect to still have he same values
	time.Sleep(time.Second)
	count1B := countRunningWorkers(w1)
	require.Equal(t, count1, count1B)
	count2B := countRunningWorkers(w2)
	require.Equal(t, count2, count2B)
	count3B := countRunningWorkers(w3)
	require.Equal(t, count3, count3B)

	// kill node #1
	cancel1()
	time.Sleep(time.Second)
	count = countRunningWorkers(w2)
	require.Equal(t, 2, count)
	count = countRunningWorkers(w3)
	require.Equal(t, 2, count)

	// kill node #2
	cancel2()
	time.Sleep(time.Second)
	count = countRunningWorkers(w3)
	require.Equal(t, 4, count)

	// node #2
	w2, cancel2 = newBalancerWithTimeout(members, 2*time.Second)

	time.Sleep(time.Second)
	count = countRunningWorkers(w3)
	require.Equal(t, 2, count)
	count = countRunningWorkers(w2)
	require.Equal(t, 2, count)

	// after node#2 timeout, it will recover and rebalance
	time.Sleep(time.Second)
	count = countRunningWorkers(w2)
	require.Equal(t, 2, count)
	count = countRunningWorkers(w3)
	require.Equal(t, 2, count)

	cancel3()
	cancel2()
}

func newBalancer(members *sync.Map) ([]worker.Worker, context.CancelFunc) {
	return newBalancerWithTimeout(members, 0)
}

func newBalancerWithTimeout(members *sync.Map, timeout time.Duration) ([]worker.Worker, context.CancelFunc) {
	ctx, cancel := context.WithCancel(context.Background())
	member := NewInMemMemberList(ctx, members)
	ws := getWorkers(timeout)
	go worker.BalanceWorkers(ctx, log.NewLogrus(logrus.StandardLogger()), member, ws, time.Second/2)
	return ws, cancel
}

func countRunningWorkers(workers []worker.Worker) int {
	count := 0
	for _, w := range workers {
		if w.IsRunning() {
			count++
		}
	}
	return count
}

func getWorkers(timeout time.Duration) []worker.Worker {
	return []worker.Worker{
		NewInMemWorker("worker-1", timeout),
		NewInMemWorker("worker-2", timeout),
		NewInMemWorker("worker-3", timeout),
		NewInMemWorker("worker-4", timeout),
	}
}

type InMemLocker struct {
	locks *sync.Map
	name  string
	done  chan struct{}
}

func NewInMemLocker(locks *sync.Map, name string) *InMemLocker {
	return &InMemLocker{
		locks: locks,
		name:  name,
	}
}

func (l *InMemLocker) Lock(context.Context) (<-chan struct{}, error) {
	_, loaded := l.locks.LoadOrStore(l.name, true)
	if !loaded {
		l.done = make(chan struct{})
		return l.done, nil
	}

	return nil, nil
}

func (l *InMemLocker) Unlock(context.Context) error {
	close(l.done)
	l.locks.Delete(l.name)
	return nil
}

func (l *InMemLocker) WaitForUnlock(context.Context) error {
	return nil
}

type InMemWorker struct {
	name     string
	running  bool
	duration time.Duration
	mu       sync.RWMutex
	locker   lock.Locker
}

func NewInMemWorker(name string, duration time.Duration) *InMemWorker {
	return &InMemWorker{
		name:     name,
		duration: duration,
		locker:   NewInMemLocker(locks, name),
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

func (w *InMemWorker) Start(ctx context.Context) bool {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.running = true
	if w.duration > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithCancel(ctx)
		go func() {
			time.Sleep(w.duration)
			cancel()
		}()
	}
	go func() {
		<-ctx.Done()
		w.Stop(ctx)
	}()

	return true
}

func (w *InMemWorker) Stop(_ context.Context) {
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
