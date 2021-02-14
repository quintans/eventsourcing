package mysql

import (
	"context"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/quintans/eventstore"
	"github.com/quintans/eventstore/sink"
	"github.com/quintans/eventstore/store/mysql"
	"github.com/quintans/eventstore/test"
	"github.com/quintans/faults"
	"github.com/stretchr/testify/require"
	"gotest.tools/assert"
)

type MockSink struct {
	mu     sync.Mutex
	events []eventstore.Event
}

func (s *MockSink) Init() error {
	return nil
}

func (s *MockSink) Sink(ctx context.Context, e eventstore.Event) error {
	s.mu.Lock()
	s.events = append(s.events, e)
	s.mu.Unlock()
	return nil
}

func (s *MockSink) LastMessage(ctx context.Context, partition uint32) (*eventstore.Event, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.events) == 0 {
		return &eventstore.Event{}, nil
	}
	return &s.events[len(s.events)-1], nil
}

func (s *MockSink) Close() {}

func (s *MockSink) Events() []eventstore.Event {
	s.mu.Lock()
	size := len(s.events)
	events := make([]eventstore.Event, size, size)
	copy(events, s.events)
	s.mu.Unlock()
	return events
}

func TestListener(t *testing.T) {
	repository, err := mysql.NewStore(dbURL)
	require.NoError(t, err)

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	es := eventstore.NewEventStore(repository, 3, test.AggregateFactory{}, test.EventFactory{})

	s := &MockSink{}
	ctx, cancel := context.WithCancel(context.Background())
	feeding(ctx, t, s)
	time.Sleep(200 * time.Millisecond)

	id := uuid.New().String()
	acc := test.CreateAccount("Paulo", id, 100)
	acc.Deposit(10)
	acc.Deposit(20)
	err = es.Save(ctx, acc)
	require.NoError(t, err)

	time.Sleep(5 * time.Second)
	events := s.Events()
	assert.Equal(t, 3, len(events), "event size")
	assert.Equal(t, "AccountCreated", events[0].Kind)
	assert.Equal(t, "MoneyDeposited", events[1].Kind)
	assert.Equal(t, "MoneyDeposited", events[2].Kind)

	cancel()
	time.Sleep(time.Second)

	ctx, cancel = context.WithCancel(context.Background())
	feeding(ctx, t, s)
	time.Sleep(200 * time.Millisecond)

	id = uuid.New().String()
	acc = test.CreateAccount("Quintans", id, 100)
	acc.Deposit(30)
	// acc.Withdraw(5)
	err = es.Save(ctx, acc)
	require.NoError(t, err)

	time.Sleep(5 * time.Second)
	events = s.Events()
	assert.Equal(t, 5, len(events), "event size")

	cancel()
}

func feeding(ctx context.Context, t *testing.T, sinker sink.Sinker) {
	listener, err := mysql.NewFeed(dbConfig)
	require.NoError(t, err)
	go func() {
		err := listener.Feed(ctx, sinker)
		if err != nil {
			log.Fatalf("Error feeding on #1: %v", faults.Wrap(err))
		}
	}()
	go func() {
		<-ctx.Done()
		time.Sleep(100 * time.Millisecond)
		listener.Close(context.Background())
	}()
}
