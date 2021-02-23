package pg

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/quintans/eventstore"
	"github.com/quintans/eventstore/store/postgresql"
	"github.com/quintans/eventstore/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPgListener(t *testing.T) {
	dbConfig, tearDown, err := setup()
	require.NoError(t, err)
	defer tearDown()

	repository, err := postgresql.NewStore(dbConfig)
	require.NoError(t, err)

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	listener := postgresql.NewFeed(dbConfig, repository, "events_channel")

	s := test.NewMockSink(1)
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		err := listener.Feed(ctx, s)
		if err != nil {
			log.Fatalf("Error feeding #1: %v", err)
		}
	}()

	time.Sleep(100 * time.Millisecond)

	es := eventstore.NewEventStore(repository, 3, test.AggregateFactory{})

	id := uuid.New().String()
	acc := test.CreateAccount("Paulo", id, 100)
	acc.Deposit(10)
	acc.Deposit(20)
	err = es.Save(ctx, acc)
	require.NoError(t, err)

	// giving time for the snapshots to write
	time.Sleep(100 * time.Millisecond)

	events := s.GetEvents()
	assert.Equal(t, 3, len(events), "event size")
	assert.Equal(t, "AccountCreated", events[0].Kind)
	assert.Equal(t, "MoneyDeposited", events[1].Kind)
	assert.Equal(t, "MoneyDeposited", events[2].Kind)

	cancel()
}
