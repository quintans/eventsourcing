//go:build unit
// +build unit

package worker_test

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"

	"github.com/quintans/eventsourcing/log"
	"github.com/quintans/eventsourcing/test"
	"github.com/quintans/eventsourcing/worker"
)

var (
	lockerPool = test.NewInMemLockerPool()
	logger     log.LogrusWrap
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

	time.Sleep(1 * time.Second)
	dumpRunningTasks()
	count := countRunningWorkers(w1)
	require.Equal(t, 4, count)

	// node #2
	fmt.Println("==== starting B2 =====")
	w2, cancel2, done2 := newBalancer("B2", members)

	time.Sleep(1 * time.Second)
	dumpRunningTasks()
	count = countRunningWorkers(w1)
	require.Equal(t, 2, count)
	count = countRunningWorkers(w2)
	require.Equal(t, 2, count)

	// node #3
	fmt.Println("==== starting B3 =====")
	w3, cancel3, done3 := newBalancer("B3", members)

	time.Sleep(1 * time.Second)
	dumpRunningTasks()
	count1 := countRunningWorkers(w1)
	require.True(t, count1 >= 1 && count1 <= 2)
	count2 := countRunningWorkers(w2)
	require.True(t, count2 >= 1 && count2 <= 2)
	count3 := countRunningWorkers(w3)
	require.True(t, count3 >= 1 && count3 <= 2)

	require.Equal(t, 4, count1+count2+count3, "total of running")

	// after a while we expect to still have he same values
	time.Sleep(1 * time.Second)
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
	dumpRunningTasks()
	count = countRunningWorkers(w2)
	require.Equal(t, 2, count)
	count = countRunningWorkers(w3)
	require.Equal(t, 2, count)

	// kill node #2
	fmt.Println("==== stop B2 =====")
	cancel2()
	<-done2
	fmt.Println("==== stopped B2 =====")
	time.Sleep(1 * time.Second)
	dumpRunningTasks()
	count = countRunningWorkers(w3)
	require.Equal(t, 4, count)

	// node #2
	fmt.Println("==== starting B2 =====")
	w2, cancel2, done2 = newBalancer("B2", members)
	time.Sleep(1 * time.Second)
	dumpRunningTasks()
	count = countRunningWorkers(w3)
	require.Equal(t, 2, count)
	count = countRunningWorkers(w2)
	require.Equal(t, 2, count)

	fmt.Println("==== crashing B2 =====")
	for _, w := range w2 {
		w.Stop(context.Background())
	}

	// after all the workers of node#2 timeout, they will recover and be rebalanced again
	time.Sleep(1 * time.Second)
	dumpRunningTasks()
	count = countRunningWorkers(w2)
	require.Equal(t, 2, count)
	count = countRunningWorkers(w3)
	require.Equal(t, 2, count)

	fmt.Println("==== stop B3 and B2 =====")
	cancel3()
	cancel2()
	<-done3
	<-done2
	dumpRunningTasks()
}

func newBalancer(name string, members *sync.Map) ([]worker.Worker, context.CancelFunc, <-chan struct{}) {
	ctx, cancel := context.WithCancel(context.Background())
	member := test.NewInMemMemberList(ctx, members)
	ws := getWorkers(name)
	balancer := worker.NewMembersBalancer(
		log.NewLogrus(logrus.StandardLogger()),
		name,
		member,
		ws,
		worker.WithHeartbeat(250*time.Millisecond),
		worker.WithTurboHeartbeat(100*time.Millisecond),
	)
	balancer.Start(ctx)
	done := make(chan struct{})
	go func() {
		<-ctx.Done()
		balancer.Stop(context.Background())
		close(done)
	}()
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

func getWorkers(suffix string) []worker.Worker {
	return []worker.Worker{
		NewRunner("W1", "W1-"+suffix),
		NewRunner("W2", "W2-"+suffix),
		NewRunner("W3", "W3-"+suffix),
		NewRunner("W4", "W4-"+suffix),
	}
}

var runningTasks = sync.Map{}

func dumpRunningTasks() {
	var tasks []string
	runningTasks.Range(func(key, value interface{}) bool {
		tasks = append(tasks, key.(string))
		return true
	})
	sort.Strings(tasks)
	fmt.Println("active:", strings.Join(tasks, ", "))
}

func NewRunner(name, tag string) *worker.RunWorker {
	return worker.NewRunWorker(logger, name, "workers", lockerPool.NewLock(name), Task{name: tag})
}

type Task struct {
	name string
}

func (t Task) Run(context.Context) error {
	runningTasks.Store(t.name, true)
	fmt.Println("starting ✅", t.name)
	return nil
}

func (t Task) Cancel(context.Context, bool) {
	runningTasks.Delete(t.name)
	fmt.Println("stopping ❌", t.name)
}
