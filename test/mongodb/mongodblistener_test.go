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
	"github.com/quintans/eventstore/feed/mongolistener"
	"github.com/quintans/eventstore/repo"
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
	return &eventstore.Event{}, nil
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
	repository, err := repo.NewMongoEsRepository(dbURL, dbName)
	if err != nil {
		log.Fatalf("Error instantiating event store: %v", err)
	}

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	listener, err := mongolistener.New(dbURL, dbName)

	s := &MockSink{
		events: []eventstore.Event{},
	}
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		err := listener.Feed(ctx, s)
		if err != nil {
			log.Fatalf("Error feeding: %v", err)
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

	// giving time for the snapshots to write
	time.Sleep(100 * time.Millisecond)

	events := s.Events()
	assert.Equal(t, 3, len(events), "event size")
	assert.Equal(t, "AccountCreated", events[0].Kind)
	assert.Equal(t, "MoneyDeposited", events[1].Kind)
	assert.Equal(t, "MoneyDeposited", events[2].Kind)
	cancel()
}
