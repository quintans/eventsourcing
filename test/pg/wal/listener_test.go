package wal

import (
	"context"
	"errors"
	"os"
	"os/signal"
	"syscall"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/quintans/eventsourcing"
	"github.com/quintans/eventsourcing/sink"
	"github.com/quintans/eventsourcing/store/postgresql"
	"github.com/quintans/eventsourcing/test"
	tpg "github.com/quintans/eventsourcing/test/pg"
)

func TestListener(t *testing.T) {
	dbConfig, tearDown, err := setup()
	require.NoError(t, err)
	defer tearDown()

	repository, err := postgresql.NewStore(dbConfig.Url())
	require.NoError(t, err)

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	es := eventsourcing.NewEventStore(repository, 3, test.AggregateFactory{})

	s := test.NewMockSink(1)
	ctx, cancel := context.WithCancel(context.Background())
	errCh := feeding(ctx, dbConfig, s)
	time.Sleep(time.Second)

	id := uuid.New()
	acc := test.CreateAccount("Paulo", id, 100)
	acc.Deposit(10)
	acc.Withdraw(20)
	acc.Deposit(5)
	err = es.Save(ctx, acc)
	require.NoError(t, err)

	time.Sleep(20 * time.Second)
	events := s.GetEvents()
	require.Equal(t, 3, len(events), "event size")
	assert.Equal(t, "AccountCreated", events[0].Kind.String())
	assert.Equal(t, "MoneyDeposited", events[1].Kind.String())
	assert.Equal(t, "MoneyWithdraw", events[2].Kind.String())
	assert.Equal(t, "MoneyDeposited", events[3].Kind.String())

	cancel()
	require.NoError(t, <-errCh, "Error feeding #1")

	ctx, cancel = context.WithCancel(context.Background())
	errCh = feeding(ctx, dbConfig, s)

	id = uuid.New()
	acc = test.CreateAccount("Quintans", id, 100)
	acc.Deposit(30)
	err = es.Save(ctx, acc)
	require.NoError(t, err)

	time.Sleep(time.Second)
	events = s.GetEvents()
	assert.Equal(t, 5, len(events), "event size")

	cancel()
	require.NoError(t, <-errCh, "Error feeding #2")
}

func feeding(ctx context.Context, dbConfig tpg.DBConfig, sinker sink.Sinker) chan error {
	errCh := make(chan error, 1)
	done := make(chan struct{})
	listener := postgresql.NewFeed(dbConfig.ReplicationUrl())
	go func() {
		close(done)
		err := listener.Feed(ctx, sinker)
		if err != nil && !errors.Is(err, context.Canceled) {
			errCh <- err
		} else {
			errCh <- nil
		}
	}()
	<-done
	return errCh
}
