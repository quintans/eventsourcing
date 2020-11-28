package mongodb

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
	"github.com/quintans/eventstore/store/mongodb"
	"github.com/quintans/eventstore/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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

func (s *MockSink) LastMessage(ctx context.Context, partition int) (*eventstore.Event, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.events) == 0 {
		return &eventstore.Event{}, nil
	}
	e := s.events[len(s.events)-1]
	return &e, nil
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

func TestMongoListenere(t *testing.T) {
	repository, err := mongodb.NewStore(dbURL, dbName, test.StructFactory{})
	if err != nil {
		log.Fatalf("Error instantiating event store: %v", err)
	}

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	listener, err := mongodb.NewFeed(dbURL, dbName)

	s := &MockSink{
		events: []eventstore.Event{},
	}
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		err := listener.Feed(ctx, s)
		if err != nil {
			log.Fatalf("Error feeding on #1: %v", err)
		}
	}()

	time.Sleep(100 * time.Millisecond)

	es := eventstore.NewEventStore(repository, 3)

	id := uuid.New().String()
	acc := test.CreateAccount("Paulo", id, 100)
	acc.Deposit(10)
	acc.Deposit(20)
	err = es.Save(ctx, acc, eventstore.Options{})
	require.NoError(t, err)

	time.Sleep(100 * time.Millisecond)

	events := s.Events()
	require.Equal(t, 3, len(events), "event size")
	assert.Equal(t, "AccountCreated", events[0].Kind)
	assert.Equal(t, "MoneyDeposited", events[1].Kind)
	assert.Equal(t, "MoneyDeposited", events[2].Kind)

	// cancel current listener
	cancel()
	time.Sleep(100 * time.Millisecond)
	listener.Close(context.Background())

	// reconnecting
	listener, err = mongodb.NewFeed(dbURL, dbName)
	ctx, cancel = context.WithCancel(context.Background())
	go func() {
		err := listener.Feed(ctx, s)
		if err != nil {
			log.Fatalf("Error feeding on #2: %v", err)
		}
	}()

	time.Sleep(100 * time.Millisecond)
	events = s.Events()
	assert.Equal(t, 3, len(events), "event size")

	acc.Withdraw(5)
	err = es.Save(ctx, acc, eventstore.Options{})
	require.NoError(t, err)

	time.Sleep(100 * time.Millisecond)

	events = s.Events()
	assert.Equal(t, 4, len(events), "event size")
	assert.Equal(t, "MoneyWithdrawn", events[3].Kind)

	cancel()
}
