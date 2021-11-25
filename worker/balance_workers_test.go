package worker_test

import (
	"context"
	"fmt"
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

var (
	locks  = &sync.Map{}
	logger log.LogrusWrap
)

func init() {
	logrus.SetLevel(logrus.ErrorLevel)
	logger = log.NewLogrus(logrus.StandardLogger())
}

func TestMembersList(t *testing.T) {
	members := &sync.Map{}

	// node #1
	fmt.Println("==== starting B1 =====")
	w1, cancel1, done1 := newBalancer("B1", members)

	time.Sleep(2 * time.Second)
	count := countRunningWorkers(w1)
	require.Equal(t, 4, count)

	// node #2
	fmt.Println("==== starting B2 =====")
	w2, cancel2, done2 := newBalancer("B2", members)

	time.Sleep(2 * time.Second)
	count = countRunningWorkers(w1)
	require.Equal(t, 2, count)
	count = countRunningWorkers(w2)
	require.Equal(t, 2, count)

	// node #3
	fmt.Println("==== starting B3 =====")
	w3, cancel3, done3 := newBalancer("B3", members)

	time.Sleep(2 * time.Second)
	count1 := countRunningWorkers(w1)
	require.True(t, count1 >= 1 && count1 <= 2)
	count2 := countRunningWorkers(w2)
	require.True(t, count2 >= 1 && count2 <= 2)
	count3 := countRunningWorkers(w3)
	require.True(t, count3 >= 1 && count3 <= 2)

	require.Equal(t, 4, count+count2+count3, "total of running")

	// after a while we expect to still have he same values
	time.Sleep(2 * time.Second)
	count1B := countRunningWorkers(w1)
	require.Equal(t, count1, count1B)
	count2B := countRunningWorkers(w2)
	require.Equal(t, count2, count2B)
	count3B := countRunningWorkers(w3)
	require.Equal(t, count3, count3B)

	// kill node #1
	fmt.Println("==== stop B1 =====")
	cancel1()
	<-done1
	fmt.Println("==== stopped B1 =====")
	time.Sleep(2 * time.Second)
	count = countRunningWorkers(w2)
	require.Equal(t, 2, count)
	count = countRunningWorkers(w3)
	require.Equal(t, 2, count)

	// kill node #2
	fmt.Println("==== stop B2 =====")
	cancel2()
	<-done2
	fmt.Println("==== stopped B2 =====")
	time.Sleep(2 * time.Second)
	count = countRunningWorkers(w3)
	require.Equal(t, 4, count)

	// node #2
	fmt.Println("==== starting B2 =====")
	w2, cancel2, done2 = newBalancer("B2", members)
	go func() {
		time.Sleep(2 * time.Second)
		for _, w := range w2 {
			w.Stop(context.Background())
		}
	}()

	time.Sleep(1 * time.Second)
	count = countRunningWorkers(w3)
	require.Equal(t, 2, count)
	count = countRunningWorkers(w2)
	require.Equal(t, 2, count)

	// after all the workers of node#2 timeout, they will recover and be rebalanced again
	time.Sleep(2 * time.Second)
	count = countRunningWorkers(w2)
	require.Equal(t, 2, count)
	count = countRunningWorkers(w3)
	require.Equal(t, 2, count)

	fmt.Println("==== stop B3 and B2 =====")
	cancel3()
	cancel2()
	<-done3
	<-done2
}

func newBalancer(name string, members *sync.Map) ([]worker.Worker, context.CancelFunc, <-chan struct{}) {
	ctx, cancel := context.WithCancel(context.Background())
	member := NewInMemMemberList(ctx, members)
	ws := getWorkers(name)
	balancer := worker.NewBalancer(name, log.NewLogrus(logrus.StandardLogger()), member, ws, 500*time.Millisecond)
	done := balancer.Start(ctx)
	return ws, cancel, done
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

func getWorkers(prefix string) []worker.Worker {
	return []worker.Worker{
		NewRunner("W1", prefix+"-W1"),
		NewRunner("W2", prefix+"-W2"),
		NewRunner("W3", prefix+"-W3"),
		NewRunner("W4", prefix+"-W4"),
	}
}

func NewRunner(name, tag string) *worker.RunWorker {
	return worker.NewRunWorker(logger, name, NewInMemLocker(name), Task{name: tag})
}

type Task struct {
	name string
}

func (t Task) Run(context.Context) error {
	fmt.Println("starting ✅", t.name)
	return nil
}

func (t Task) Cancel() {
	fmt.Println("stopping ❌", t.name)
}

type InMemLocker struct {
	locks *sync.Map
	name  string
	done  chan struct{}
}

func NewInMemLocker(name string) *InMemLocker {
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

	return nil, lock.ErrLockAlreadyHeld
}

func (l *InMemLocker) Unlock(context.Context) error {
	_, loaded := l.locks.LoadAndDelete(l.name)
	if !loaded {
		return lock.ErrLockNotHeld
	}
	close(l.done)
	return nil
}

func (l *InMemLocker) WaitForUnlock(context.Context) error {
	return nil
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

func (m *InMemMemberList) Unregister(_ context.Context) error {
	m.db.Delete(m.name)
	return nil
}
