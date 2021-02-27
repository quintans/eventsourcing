package wal

import (
	"context"
	"errors"
	"log"
	"os"
	"os/signal"
	"syscall"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/quintans/eventstore"
	"github.com/quintans/eventstore/sink"
	"github.com/quintans/eventstore/store/postgresql"
	"github.com/quintans/eventstore/test"
	tpg "github.com/quintans/eventstore/test/pg"
	"github.com/stretchr/testify/require"
	"gotest.tools/assert"
)

func TestListener(t *testing.T) {
	dbConfig, tearDown, err := setup()
	require.NoError(t, err)
	defer tearDown()

	repository, err := postgresql.NewStore(dbConfig.Url())
	require.NoError(t, err)

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	es := eventstore.NewEventStore(repository, 3, test.AggregateFactory{})

	s := test.NewMockSink(1)
	ctx, cancel := context.WithCancel(context.Background())
	feeding(ctx, dbConfig, s)
	time.Sleep(200 * time.Millisecond)

	id := uuid.New().String()
	acc := test.CreateAccount("Paulo", id, 100)
	acc.Deposit(10)
	acc.Deposit(20)
	err = es.Save(ctx, acc)
	require.NoError(t, err)

	time.Sleep(time.Second)
	events := s.GetEvents()
	assert.Equal(t, 3, len(events), "event size")
	assert.Equal(t, "AccountCreated", events[0].Kind)
	assert.Equal(t, "MoneyDeposited", events[1].Kind)
	assert.Equal(t, "MoneyDeposited", events[2].Kind)

	time.Sleep(time.Second)
	cancel()
	time.Sleep(100 * time.Millisecond)

	ctx, cancel = context.WithCancel(context.Background())
	feeding(ctx, dbConfig, s)
	time.Sleep(200 * time.Millisecond)

	id = uuid.New().String()
	acc = test.CreateAccount("Quintans", id, 100)
	acc.Deposit(30)
	err = es.Save(ctx, acc)
	require.NoError(t, err)

	time.Sleep(time.Second)
	events = s.GetEvents()
	assert.Equal(t, 5, len(events), "event size")

	cancel()
	time.Sleep(100 * time.Millisecond)
}

func feeding(ctx context.Context, dbConfig tpg.DBConfig, sinker sink.Sinker) {
	done := make(chan struct{})
	listener := postgresql.NewFeed(dbConfig.ReplicationUrl())
	go func() {
		close(done)
		err := listener.Feed(ctx, sinker)
		if err != nil && !errors.Is(err, context.Canceled) {
			log.Fatalf("Error feeding #1: %v", err)
		}
	}()
	// wait for the goroutine to run
	<-done
}
