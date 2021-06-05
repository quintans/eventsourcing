package mysql

import (
	"context"
	"errors"
	"os"
	"os/signal"
	"syscall"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"gotest.tools/assert"

	"github.com/quintans/eventsourcing"
	"github.com/quintans/eventsourcing/sink"
	"github.com/quintans/eventsourcing/store/mysql"
	"github.com/quintans/eventsourcing/test"
)

func TestListener(t *testing.T) {
	dbConfig, tearDown, err := setup()
	require.NoError(t, err)
	defer tearDown()

	repository, err := mysql.NewStore(dbConfig.Url())
	require.NoError(t, err)

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	es := eventsourcing.NewEventStore(repository, 3, test.AggregateFactory{})

	cfg := mysql.DBConfig{
		Host:     dbConfig.Host,
		Port:     dbConfig.Port,
		Database: dbConfig.Database,
		Username: dbConfig.Username,
		Password: dbConfig.Password,
	}

	s := test.NewMockSink(0)
	ctx, cancel := context.WithCancel(context.Background())
	errCh := feeding(ctx, cfg, s)

	id := uuid.New()
	acc := test.CreateAccount("Paulo", id, 100)
	acc.Deposit(10)
	acc.Deposit(20)
	err = es.Save(ctx, acc)
	require.NoError(t, err)

	time.Sleep(time.Second)
	events := s.GetEvents()
	assert.Equal(t, 3, len(events), "event size")
	assert.Equal(t, "AccountCreated", events[0].Kind.String())
	assert.Equal(t, "MoneyDeposited", events[1].Kind.String())
	assert.Equal(t, "MoneyDeposited", events[2].Kind.String())

	cancel()
	require.NoError(t, <-errCh, "Error feeding #1")

	ctx, cancel = context.WithCancel(context.Background())

	id = uuid.New()
	acc = test.CreateAccount("Quintans", id, 100)
	acc.Deposit(30)
	// acc.Withdraw(5)
	err = es.Save(ctx, acc)
	require.NoError(t, err)

	// resume from the last position, by using the same sinker and a new connection
	errCh = feeding(ctx, cfg, s)

	time.Sleep(time.Second)
	events = s.GetEvents()
	assert.Equal(t, 5, len(events), "event size")

	cancel()
	require.NoError(t, <-errCh, "Error feeding #2")

	// resume from the begginning
	s = test.NewMockSink(0)
	ctx, cancel = context.WithCancel(context.Background())
	errCh = feeding(ctx, cfg, s)

	time.Sleep(time.Second)
	events = s.GetEvents()
	assert.Equal(t, 5, len(events), "event size")

	cancel()
	require.NoError(t, <-errCh, "Error feeding #3")
}

func feeding(ctx context.Context, dbConfig mysql.DBConfig, sinker sink.Sinker) chan error {
	errCh := make(chan error, 1)
	done := make(chan struct{})
	listener := mysql.NewFeed(logger, dbConfig)
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
