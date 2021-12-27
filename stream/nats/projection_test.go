package nats_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/docker/go-connections/nat"
	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
	"github.com/quintans/faults"
	"github.com/quintans/toolkit/locks"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	testcontainers "github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/quintans/eventsourcing"

	"github.com/quintans/eventsourcing/eventid"
	"github.com/quintans/eventsourcing/lock"
	"github.com/quintans/eventsourcing/log"
	"github.com/quintans/eventsourcing/player"
	"github.com/quintans/eventsourcing/projection"
	mynats "github.com/quintans/eventsourcing/stream/nats"
	"github.com/quintans/eventsourcing/test"
	"github.com/quintans/eventsourcing/worker"
)

var logger log.LogrusWrap

func init() {
	// logrus.SetLevel(logrus.DebugLevel)
	logger = log.NewLogrus(logrus.StandardLogger())
	logrus.SetFormatter(&logrus.TextFormatter{
		DisableQuote: true,
		// ForceQuote: true,
	})
}

type NatsConfig struct {
	Host     string
	NatPorts []nat.Port
}

func TestRestartProducer(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ctxNats, cancelNats := context.WithCancel(ctx)
	defer cancelNats()
	config, tearDown, err := setupNATS(ctxNats)
	require.NoError(t, err)
	defer tearDown()

	url := fmt.Sprintf("nats://%s:%s", config.Host, config.NatPorts[0])
	nc, err := nats.Connect(url)
	require.NoError(t, err)
	defer nc.Close()

	db := test.NewInMemDB()

	latchProd := locks.NewCountDownLatch()
	ctxProd, cancelProd := context.WithCancel(ctx)
	defer cancelProd()
	startProducer(t, ctxProd, url, latchProd, db)

	resumeStore := test.NewInMemResumeStore()
	members := &sync.Map{}
	lockerPool := test.NewInMemLockerPool()

	latchCons := locks.NewCountDownLatch()
	ctxCons, cancelCons := context.WithCancel(ctx)
	defer cancelCons()
	var mu sync.Mutex
	var actual []eventsourcing.Event
	delivered := 0
	_, err = startConsumer(ctxCons, latchCons, url, "account", resumeStore, members, lockerPool, func(ctx context.Context, e eventsourcing.Event) error {
		mu.Lock()
		defer mu.Unlock()

		actual = append(actual, e)
		delivered++
		return nil
	})
	require.NoError(t, err)

	var expected []eventsourcing.Event
	expected = append(expected, db.Add(eventsourcing.Event{
		AggregateID:      uuid.NewString(),
		AggregateVersion: 1,
		AggregateType:    "Account",
		Kind:             "Created",
		Body:             []byte("{}"),
	}))

	time.Sleep(time.Second)
	mu.Lock()
	require.Equal(t, 1, delivered)
	require.Equal(t, expected[0], actual[0])
	mu.Unlock()

	// stop producer
	cancelProd()
	latchProd.WaitWithTimeout(time.Second)

	// start new producer
	latchProd = locks.NewCountDownLatch()
	ctxProd, cancelProd = context.WithCancel(ctx)
	defer cancelProd()
	startProducer(t, ctxProd, url, latchProd, db)

	time.Sleep(time.Second)
	mu.Lock()
	// no new delivery
	require.Equal(t, 1, delivered)
	mu.Unlock()

	// stop producer
	cancelProd()
	latchProd.WaitWithTimeout(time.Second)
	// populate db
	expected = append(expected, db.Add(eventsourcing.Event{
		AggregateID:      uuid.NewString(),
		AggregateVersion: 2,
		AggregateType:    "Account",
		Kind:             "Created",
		Body:             []byte("{}"),
	}))
	expected = append(expected, db.Add(eventsourcing.Event{
		AggregateID:      uuid.NewString(),
		AggregateVersion: 3,
		AggregateType:    "Account",
		Kind:             "Created",
		Body:             []byte("{}"),
	}))

	// start new producer
	latchProd = locks.NewCountDownLatch()
	ctxProd, cancelProd = context.WithCancel(ctx)
	defer cancelProd()
	startProducer(t, ctxProd, url, latchProd, db)

	time.Sleep(time.Second)
	mu.Lock()
	// delivery  of new ones
	require.Equal(t, 3, delivered)
	require.Equal(t, expected, actual)
	mu.Unlock()

	cancel()
	latchCons.WaitWithTimeout(time.Second)
	latchProd.WaitWithTimeout(time.Second)
}

func TestRestartConsumer(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ctxNats, cancelNats := context.WithCancel(ctx)
	defer cancelNats()
	config, tearDown, err := setupNATS(ctxNats)
	require.NoError(t, err)
	defer tearDown()

	url := fmt.Sprintf("nats://%s:%s", config.Host, config.NatPorts[0])
	nc, err := nats.Connect(url)
	require.NoError(t, err)
	defer nc.Close()

	db := test.NewInMemDB()

	latchProd := locks.NewCountDownLatch()
	ctxProd, cancelProd := context.WithCancel(ctx)
	defer cancelProd()
	startProducer(t, ctxProd, url, latchProd, db)

	resumeStore := test.NewInMemResumeStore()
	members := &sync.Map{}
	lockerPool := test.NewInMemLockerPool()

	var mu sync.Mutex
	var actual []eventsourcing.Event
	delivered := 0
	handlerFactory := func(name string) func(ctx context.Context, e eventsourcing.Event) error {
		return func(ctx context.Context, e eventsourcing.Event) error {
			mu.Lock()
			defer mu.Unlock()

			fmt.Println("===>", name, "->", e.AggregateVersion)
			actual = append(actual, e)
			delivered++
			return nil
		}
	}
	latchCons1 := locks.NewCountDownLatch()
	ctxCons1, cancelCons1 := context.WithCancel(ctx)
	defer cancelCons1()
	_, err = startConsumer(ctxCons1, latchCons1, url, "account", resumeStore, members, lockerPool, handlerFactory("consumer #1"))
	require.NoError(t, err)

	latchCons2 := locks.NewCountDownLatch()
	ctxCons2, cancelCons2 := context.WithCancel(ctx)
	defer cancelCons2()
	_, err = startConsumer(ctxCons2, latchCons2, url, "account", resumeStore, members, lockerPool, handlerFactory("consumer #2"))
	require.NoError(t, err)

	time.Sleep(time.Second)

	var expected []eventsourcing.Event
	expected = append(expected, db.Add(eventsourcing.Event{
		AggregateID:      uuid.NewString(),
		AggregateVersion: 1,
		AggregateType:    "Account",
		Kind:             "Created",
		Body:             []byte("{}"),
	}))
	expected = append(expected, db.Add(eventsourcing.Event{
		AggregateID:      uuid.NewString(),
		AggregateVersion: 2,
		AggregateType:    "Account",
		Kind:             "Created",
		Body:             []byte("{}"),
	}))

	time.Sleep(time.Second)
	mu.Lock()
	require.Equal(t, 2, delivered)
	require.Equal(t, expected, actual)
	mu.Unlock()

	// stop consumer #1
	cancelCons1()
	latchCons1.WaitWithTimeout(time.Second)

	// start a new consumer #1
	latchCons1 = locks.NewCountDownLatch()
	ctxCons1, cancelCons1 = context.WithCancel(ctx)
	defer cancelCons1()
	_, err = startConsumer(ctxCons1, latchCons1, url, "account", resumeStore, members, lockerPool, handlerFactory("consumer #1"))
	require.NoError(t, err)

	// time.Sleep(time.Second)

	// populate db
	const records = 10
	go func() {
		for i := 1; i <= records; i++ {
			e := db.Add(eventsourcing.Event{
				AggregateID:      uuid.NewString(),
				AggregateVersion: uint32(2 + i),
				AggregateType:    "Account",
				Kind:             "Created",
				Body:             []byte("{}"),
			})
			mu.Lock()
			expected = append(expected, e)
			mu.Unlock()
			time.Sleep(100 * time.Millisecond)
		}
	}()

	time.Sleep(time.Second)

	mu.Lock()
	// delivery of new ones
	require.Equal(t, records+2, delivered)
	require.Equal(t, expected, actual)
	mu.Unlock()

	cancel()
	latchCons1.WaitWithTimeout(time.Second)
	latchCons2.WaitWithTimeout(time.Second)
	latchProd.WaitWithTimeout(time.Second)
}

func TestReplayProjection(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ctxNats, cancelNats := context.WithCancel(ctx)
	defer cancelNats()
	config, tearDown, err := setupNATS(ctxNats)
	require.NoError(t, err)
	defer tearDown()

	url := fmt.Sprintf("nats://%s:%s", config.Host, config.NatPorts[0])
	nc, err := nats.Connect(url)
	require.NoError(t, err)
	defer nc.Close()

	db := test.NewInMemDB()

	var mu sync.Mutex
	var expected []eventsourcing.Event
	records := 0
	done := make(chan struct{})
	go func() {
		i := 0
		for {
			i++
			e := db.Add(eventsourcing.Event{
				AggregateID:      uuid.NewString(),
				AggregateVersion: uint32(i),
				AggregateType:    "Account",
				Kind:             "Created",
				Body:             []byte("{}"),
			})
			mu.Lock()
			expected = append(expected, e)
			records = i
			select {
			case <-done:
				mu.Unlock()
				return
			default:
			}
			mu.Unlock()
			time.Sleep(100 * time.Millisecond)
		}
	}()

	latchProd := locks.NewCountDownLatch()
	ctxProd, cancelProd := context.WithCancel(ctx)
	defer cancelProd()
	startProducer(t, ctxProd, url, latchProd, db)

	latchCons := locks.NewCountDownLatch()
	ctxCons, cancelCons := context.WithCancel(ctx)
	defer cancelCons()

	var actual []eventsourcing.Event
	delivered := 0
	projectionHandlerFactory := func(name string) projection.EventHandlerFunc {
		return func(ctx context.Context, e eventsourcing.Event) error {
			mu.Lock()
			defer mu.Unlock()

			fmt.Printf("===> [%s] handling %d\n", name, e.AggregateVersion)
			actual = append(actual, e)
			delivered++
			return nil
		}
	}
	resumeStore := test.NewInMemResumeStore()
	members := &sync.Map{}
	lockerPool := test.NewInMemLockerPool()

	projectionHandler := projectionHandlerFactory("projection #1")
	rebuilder, err := startConsumer(ctxCons, latchCons, url, "account", resumeStore, members, lockerPool, projectionHandler)
	require.NoError(t, err)
	_, err = startConsumer(ctxCons, latchCons, url, "account", resumeStore, members, lockerPool, projectionHandlerFactory("projection #2"))
	require.NoError(t, err)

	time.Sleep(time.Second)

	replay := func(ctx context.Context, afterEventID eventid.EventID) (eventid.EventID, error) {
		p := player.New(db)
		fmt.Printf("===> start replay from '%s'\n", afterEventID)
		afterEventID, err := p.Replay(ctx, projectionHandler, afterEventID)
		if err != nil {
			return eventid.Zero, faults.Errorf("Unable to replay events after '%s': %w", afterEventID, err)
		}
		fmt.Println("===> replay done")
		return afterEventID, nil
	}

	err = rebuilder.Rebuild(
		ctx,
		"balance",
		func(ctx context.Context) (eventid.EventID, error) {
			fmt.Println("===> clear data")
			mu.Lock()
			delivered = 0
			actual = nil
			mu.Unlock()

			// replay
			return replay(ctx, eventid.Zero)
		},
		replay,
	)
	require.NoError(t, err)

	mu.Lock()
	close(done)
	mu.Unlock()

	time.Sleep(time.Second)

	mu.Lock()
	require.Equal(t, records, delivered)
	require.EqualValues(t, expected, actual)
	mu.Unlock()

	cancel()
	latchCons.WaitWithTimeout(time.Second)
	latchProd.WaitWithTimeout(time.Second)
}

func startProducer(t *testing.T, ctx context.Context, url string, latch *locks.CountDownLatch, db *test.InMemDB) {
	lockerPool := test.NewInMemLockerPool()

	w, err := eventForwarderWorkers(ctx, logger, latch, url, "account", lockerPool, db)
	require.NoError(t, err)
	balancer := worker.NewSingleBalancer(logger, "account", w, 2*time.Second)

	latch.Add(1)
	go func() {
		balancer.Start(ctx)
		<-ctx.Done()
		balancer.Stop(context.Background(), false)
		latch.Done()
	}()
}

type LogConsumer struct{}

func (l LogConsumer) Accept(log testcontainers.Log) {
	fmt.Print(string(log.Content))
}

func setupNATS(ctx context.Context) (NatsConfig, func(), error) {
	config := NatsConfig{
		NatPorts: []nat.Port{
			"4222",
			"8222",
			"6222",
		},
	}
	var exposedPorts []string
	for _, p := range config.NatPorts {
		exposedPorts = append(exposedPorts, p.Port()+"/tcp")
	}

	req := testcontainers.ContainerRequest{
		Image:        "nats:latest",
		ExposedPorts: exposedPorts,
		// Env:          map[string]string{},
		// WaitingFor: wait.ForListeningPort(config.NatPorts[0]),
		WaitingFor: wait.ForLog("Server is ready"),
		Cmd:        []string{"-js"},
	}
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		return NatsConfig{}, nil, faults.Wrap(err)
	}

	err = container.StartLogProducer(ctx)
	if err != nil {
		return NatsConfig{}, nil, faults.Wrap(err)
	}

	container.FollowOutput(LogConsumer{})

	tearDown := func() {
		container.StopLogProducer()
		container.Terminate(ctx)
	}

	ip, err := container.Host(ctx)
	if err != nil {
		tearDown()
		return NatsConfig{}, nil, faults.Wrap(err)
	}
	for k, p := range config.NatPorts {
		port, err := container.MappedPort(ctx, p)
		if err != nil {
			tearDown()
			return NatsConfig{}, nil, faults.Wrap(err)
		}
		config.NatPorts[k] = port
	}

	config.Host = ip

	return config, tearDown, nil
}

func eventForwarderWorkers(ctx context.Context, logger log.Logger, latch *locks.CountDownLatch, natsUrl string, topic string, lockerPool *test.InMemLockerPool, db *test.InMemDB) (worker.Worker, error) {
	lockFact := func(lockName string) lock.Locker {
		return lockerPool.NewLock(lockName)
	}
	sinker, err := mynats.NewSink(logger, topic, 0, natsUrl)
	if err != nil {
		return nil, faults.Errorf("Error initialising NATS (%s) Sink '%s' on boot: %w", natsUrl, topic, err)
	}

	latch.Add(1)
	go func() {
		<-ctx.Done()
		sinker.Close()
		latch.Done()
	}()

	return projection.EventForwarderWorker(ctx, logger, "forwarder", lockFact, test.InMemDBNewFeed(db, sinker)), nil
}

func startConsumer(
	ctx context.Context,
	latch *locks.CountDownLatch,
	natsURL string,
	topic string,
	resumeStore projection.ResumeStore,
	members *sync.Map,
	lockerPool *test.InMemLockerPool,
	handler projection.EventHandlerFunc,
) (*projection.NotifierLockRebuilder, error) {
	lockerFactory := func(name string) lock.WaitLocker {
		return lockerPool.NewLock(name)
	}

	memberlist := test.NewInMemMemberList(ctx, members)

	projector, err := mynats.NewProjector(ctx, logger, natsURL, lockerFactory, memberlist, resumeStore, "balance")
	if err != nil {
		return nil, faults.Errorf("could not instantiate NATS projector: %w", err)
	}
	projector.AddTopicHandler(topic, 0, handler)
	rebuilder, startStop, err := projector.Projection(ctx)
	if err != nil {
		return nil, faults.Errorf("could not create projection: %w", err)
	}

	latch.Add(1)
	go func() {
		startStop.Start(ctx)
		latch.Done()
	}()

	return rebuilder, nil
}
