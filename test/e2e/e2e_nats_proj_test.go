//go:build e2e

package e2e

import (
	"context"
	"testing"
	"time"

	"github.com/quintans/eventsourcing"
	eslog "github.com/quintans/eventsourcing/log"
	"github.com/quintans/eventsourcing/projection"
	pnats "github.com/quintans/eventsourcing/projection/nats"
	"github.com/quintans/eventsourcing/sink/nats"
	"github.com/quintans/eventsourcing/store/mysql"
	"github.com/quintans/eventsourcing/test"
	shared "github.com/quintans/eventsourcing/test/mysql"
	"github.com/quintans/eventsourcing/util"
	"github.com/quintans/eventsourcing/worker"
	"github.com/quintans/toolkit/latch"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
)

var logger = eslog.NewLogrus(logrus.StandardLogger())

const (
	database = "eventsourcing"
	topic    = "accounts"
)

func TestProjectionBeforeData(t *testing.T) {
	ctx := context.Background()

	dbConfig := shared.Setup(t)

	uri := runNatsContainer(t)

	esRepo, err := mysql.NewStoreWithURL(dbConfig.URL())
	require.NoError(t, err)
	es := eventsourcing.NewEventStore[*test.Account](esRepo, test.NewJSONCodec(), &eventsourcing.EsOptions{})

	ltx := latch.NewCountDownLatch()
	ctx, cancel := context.WithCancel(context.Background())

	eventForwarderWorker(t, ctx, logger, ltx, dbConfig, uri)

	// create projection
	proj := NewProjectionMock("balances")

	topic := projection.ConsumerTopic{
		Topic:      "accounts",
		Partitions: []uint32{1},
	}
	kvStore := &MockKVStore{}

	sub, err := pnats.NewSubscriberWithURL(ctx, logger, uri, topic, kvStore)
	require.NoError(t, err)

	// repository here could be remote, like GrpcRepository
	projector := projection.Project(logger, nil, esRepo, sub, proj, 1, kvStore)
	ok, err := projector.Start(ctx)
	require.NoError(t, err)
	require.True(t, ok)

	// giving time to catchup and project events from the database
	time.Sleep(time.Second)

	id := util.MustNewULID()
	acc, err := test.CreateAccount("Paulo", id, 100)
	require.NoError(t, err)
	acc.Deposit(10)
	acc.Deposit(20)

	err = es.Create(ctx, acc)
	require.NoError(t, err)

	// giving time to forward events
	time.Sleep(time.Second)

	balance, ok := proj.BalanceByID(acc.GetID())
	require.True(t, ok)
	require.Equal(t, Balance{
		Name:   "Paulo",
		Amount: 130,
	}, balance)

	// shutdown
	cancel()
	time.Sleep(time.Second)
}

func TestProjectionAfterData(t *testing.T) {
	ctx := context.Background()

	dbConfig := shared.Setup(t)

	uri := runNatsContainer(t)

	esRepo, err := mysql.NewStoreWithURL(dbConfig.URL())
	require.NoError(t, err)
	es := eventsourcing.NewEventStore[*test.Account](esRepo, test.NewJSONCodec(), &eventsourcing.EsOptions{})

	ltx := latch.NewCountDownLatch()
	ctx, cancel := context.WithCancel(context.Background())

	eventForwarderWorker(t, ctx, logger, ltx, dbConfig, uri)

	id := util.MustNewULID()
	acc, err := test.CreateAccount("Paulo", id, 100)
	require.NoError(t, err)
	acc.Deposit(10)
	acc.Deposit(20)

	err = es.Create(ctx, acc)
	require.NoError(t, err)

	// giving time to forward events
	time.Sleep(time.Second)

	// create projection
	proj := NewProjectionMock("balances")

	topic := projection.ConsumerTopic{
		Topic:      "accounts",
		Partitions: []uint32{1},
	}
	kvStore := &MockKVStore{}

	sub, err := pnats.NewSubscriberWithURL(ctx, logger, uri, topic, kvStore)
	require.NoError(t, err)

	// repository here could be remote, like GrpcRepository
	projector := projection.Project(logger, nil, esRepo, sub, proj, 1, kvStore)
	ok, err := projector.Start(ctx)
	require.NoError(t, err)
	require.True(t, ok)

	// giving time to catchup and project events from the database
	time.Sleep(time.Second)

	balance, ok := proj.BalanceByID(acc.GetID())
	require.True(t, ok)
	require.Equal(t, Balance{
		Name:   "Paulo",
		Amount: 130,
	}, balance)

	// updating after the subscription isin place
	es.Update(ctx, acc.GetID(), func(a *test.Account) (*test.Account, error) {
		acc.Withdraw(15)
		return acc, nil
	})

	// giving time to project events through the subscription
	time.Sleep(time.Second)

	balance, ok = proj.BalanceByID(acc.GetID())
	require.True(t, ok)
	require.Equal(t, Balance{
		Name:   "Paulo",
		Amount: 115,
	}, balance)

	// shutdown
	cancel()
	time.Sleep(time.Second)
}

// eventForwarderWorker creates workers that listen to database changes,
// transform them to events and publish them into the message bus.
func eventForwarderWorker(t *testing.T, ctx context.Context, logger eslog.Logger, ltx *latch.CountDownLatch, dbConfig shared.DBConfig, natsURI string) {
	lockExpiry := 10 * time.Second

	kvStore := &MockKVStore{}

	// sinker provider
	sinker, err := nats.NewSink(kvStore, logger, topic, 1, natsURI)
	require.NoError(t, err)

	dbConf := mysql.DBConfig{
		Host:     dbConfig.Host,
		Port:     dbConfig.Port,
		Database: dbConfig.Database,
		Username: dbConfig.Username,
		Password: dbConfig.Password,
	}
	feed, err := mysql.NewFeed(logger, dbConf, sinker, mysql.WithPartitions(1, 1, 1))
	require.NoError(t, err)

	ltx.Add(1)
	go func() {
		<-ctx.Done()
		sinker.Close()
		ltx.Done()
	}()

	// setting nil for the locker factory means no lock will be used.
	// when we have multiple replicas/processes forwarding events to the message queue,
	// we need to use a distributed lock.
	forwarder := projection.EventForwarderWorker(logger, "account-forwarder", nil, feed.Run)
	balancer := worker.NewSingleBalancer(logger, forwarder, lockExpiry/2)
	ltx.Add(1)
	go func() {
		balancer.Start(ctx)
		<-ctx.Done()
		balancer.Stop(context.Background())
		ltx.Done()
	}()
}
