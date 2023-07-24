//go:build e2e

package e2e

import (
	"context"
	"log"
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
	"github.com/quintans/faults"
	"github.com/quintans/toolkit/latch"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
)

var logger = eslog.NewLogrus(logrus.StandardLogger())

const (
	database = "eventsourcing"
	topic    = "accounts"
)

func TestProjection(t *testing.T) {
	ctx := context.Background()

	dbConfig, tearDown, err := shared.Setup()
	require.NoError(t, err)
	t.Cleanup(func() {
		tearDown()
	})

	natsCont, err := runNatsContainer(ctx)
	require.NoError(t, err)
	// Clean up the container after the test is complete
	t.Cleanup(func() {
		if err := natsCont.Terminate(context.Background()); err != nil {
			t.Fatalf("failed to terminate container: %s", err)
		}
	})

	esRepo, err := mysql.NewStore(dbConfig.URL())
	require.NoError(t, err)
	es := eventsourcing.NewEventStore[*test.Account](esRepo, test.NewJSONCodec(), &eventsourcing.EsOptions{})

	ltx := latch.NewCountDownLatch()
	ctx, cancel := context.WithCancel(context.Background())

	eventForwarderWorker(t, ctx, logger, ltx, dbConfig, natsCont.URI, esRepo)

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

	sub, err := pnats.NewSubscriberWithURL(ctx, logger, natsCont.URI, topic)
	require.NoError(t, err)

	// repository here could be remote, like GrpcRepository
	projector := projection.Project(ctx, logger, nil, esRepo, sub, proj)
	ok := projector.Start(ctx)
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
func eventForwarderWorker(t *testing.T, ctx context.Context, logger eslog.Logger, ltx *latch.CountDownLatch, dbConfig shared.DBConfig, natsURI string, setSeqRepo mysql.SetSeqRepository) {
	lockExpiry := 10 * time.Second

	// sinker provider
	sinker, err := nats.NewSink(logger, topic, 1, natsURI)
	require.NoError(t, err)

	dbConf := mysql.DBConfig{
		Host:     dbConfig.Host,
		Port:     dbConfig.Port,
		Database: dbConfig.Database,
		Username: dbConfig.Username,
		Password: dbConfig.Password,
	}
	feed := mysql.NewFeed(logger, dbConf, sinker, setSeqRepo)

	ltx.Add(1)
	go func() {
		<-ctx.Done()
		sinker.Close()
		ltx.Done()
	}()

	// setting nil for the locker factory means no lock will be used.
	// when we have multiple replicas/processes forwarding events to the message queue,
	// we need to use a distributed lock.
	forwarder := projection.PartitionedEventForwarderWorker(logger, "forwarder", nil, feed.Run)
	balancer := worker.NewSingleBalancer(logger, "account", forwarder, lockExpiry/2)
	ltx.Add(1)
	go func() {
		balancer.Start(ctx)
		<-ctx.Done()
		balancer.Stop(context.Background())
		ltx.Done()
	}()
}

func dockerCompose(ctx context.Context) (func(), error) {
	compose := testcontainers.NewLocalDockerCompose([]string{"./docker-compose.yml"}, "es-set")
	destroyFn := func() {
		exErr := compose.Down()
		if err := checkIfError(exErr); err != nil {
			log.Printf("Error on compose shutdown: %v\n", err)
		}
	}

	exErr := compose.Down()
	if err := checkIfError(exErr); err != nil {
		return func() {}, err
	}
	exErr = compose.
		WithCommand([]string{"up", "--build", "-d"}).
		Invoke()
	err := checkIfError(exErr)
	if err != nil {
		return destroyFn, err
	}

	return destroyFn, err
}

func checkIfError(err testcontainers.ExecError) error {
	if err.Error != nil {
		return faults.Errorf("Failed when running %v: %v", err.Command, err.Error)
	}

	if err.Stdout != nil {
		return faults.Errorf("An error in Stdout happened when running %v: %v", err.Command, err.Stdout)
	}

	if err.Stderr != nil {
		return faults.Errorf("An error in Stderr happened when running %v: %v", err.Command, err.Stderr)
	}
	return nil
}
