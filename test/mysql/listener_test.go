package mysql

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
	"github.com/quintans/eventstore/store/mysql"
	"github.com/quintans/eventstore/test"
	"github.com/stretchr/testify/require"
	"gotest.tools/assert"
)

func TestListener(t *testing.T) {
	dbConfig, tearDown, err := setup()
	require.NoError(t, err)
	defer tearDown()

	repository, err := mysql.NewStore(dbConfig.Url())
	require.NoError(t, err)

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	es := eventstore.NewEventStore(repository, 3, test.AggregateFactory{})

	s := test.NewMockSink(0)
	ctx, cancel := context.WithCancel(context.Background())
	cfg := mysql.DBConfig{
		Host:     dbConfig.Host,
		Port:     dbConfig.Port,
		Database: dbConfig.Database,
		Username: dbConfig.Username,
		Password: dbConfig.Password,
	}
	feeding(ctx, cfg, s)
	time.Sleep(200 * time.Millisecond)

	id := uuid.New().String()
	acc := test.CreateAccount("Paulo", id, 100)
	acc.Deposit(10)
	acc.Deposit(20)
	err = es.Save(ctx, acc)
	require.NoError(t, err)

	time.Sleep(5 * time.Second)
	events := s.GetEvents()
	assert.Equal(t, 3, len(events), "event size")
	assert.Equal(t, "AccountCreated", events[0].Kind)
	assert.Equal(t, "MoneyDeposited", events[1].Kind)
	assert.Equal(t, "MoneyDeposited", events[2].Kind)

	cancel()
	time.Sleep(time.Second)

	ctx, cancel = context.WithCancel(context.Background())
	feeding(ctx, cfg, s)
	time.Sleep(200 * time.Millisecond)

	id = uuid.New().String()
	acc = test.CreateAccount("Quintans", id, 100)
	acc.Deposit(30)
	// acc.Withdraw(5)
	err = es.Save(ctx, acc)
	require.NoError(t, err)

	time.Sleep(5 * time.Second)
	events = s.GetEvents()
	assert.Equal(t, 5, len(events), "event size")

	cancel()
}

func feeding(ctx context.Context, dbConfig mysql.DBConfig, sinker sink.Sinker) {
	done := make(chan struct{})
	listener := mysql.NewFeed(dbConfig)
	go func() {
		close(done)
		err := listener.Feed(ctx, sinker)
		if err != nil && !errors.Is(err, context.Canceled) {
			log.Fatalf("Error feeding #1: %v", err)
		}
	}()
	<-done
}
