//go:build integration

package nats

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/quintans/eventsourcing"
	"github.com/quintans/eventsourcing/projection"
	pnats "github.com/quintans/eventsourcing/projection/nats"
	"github.com/quintans/eventsourcing/sink/nats"
	"github.com/quintans/eventsourcing/store/mysql"
	"github.com/quintans/eventsourcing/test"
	"github.com/quintans/eventsourcing/test/integration"
	shared "github.com/quintans/eventsourcing/test/mysql"
	"github.com/quintans/eventsourcing/util/ids"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

var logger = slog.New(slog.NewTextHandler(os.Stdout, nil))

const (
	database = "eventsourcing"
	topic    = "accounts"
)

func TestNATSProjectionBeforeData(t *testing.T) {
	ctx := context.Background()

	dbConfig := shared.Setup(t)

	uri := runNatsContainer(t)

	esRepo, err := mysql.NewStoreWithURL[ids.AggID](dbConfig.URL())
	require.NoError(t, err)
	es := eventsourcing.NewEventStore[*test.Account](esRepo, test.NewJSONCodec(), &eventsourcing.EsOptions{})

	ctx, cancel := context.WithCancel(context.Background())

	// sinker provider
	kvStoreSink := &integration.MockKVStore{}
	sinker, err := nats.NewSink[ids.AggID](kvStoreSink, logger, topic, 1, uri)
	require.NoError(t, err)
	integration.EventForwarderWorker(t, ctx, logger, dbConfig, sinker)

	// before data
	kvStoreProj := &integration.MockKVStore{}
	proj := projectionFromNATS(t, ctx, uri, esRepo, kvStoreProj)

	acc, err := test.NewAccount("Paulo", 100)
	require.NoError(t, err)
	acc.Deposit(10)
	acc.Deposit(20)

	err = es.Create(ctx, acc)
	require.NoError(t, err)

	// giving time to forward events
	time.Sleep(time.Second)

	events := proj.Events()
	require.Len(t, events, 4)
	assert.Equal(t, ids.Zero, events[0].AggregateID) // control event (switch)

	balance, ok := proj.BalanceByID(acc.GetID())
	require.True(t, ok)
	require.Equal(t, integration.Balance{
		Name:   "Paulo",
		Amount: 130,
	}, balance)

	putsProj := kvStoreProj.Puts()
	assert.Len(t, putsProj, 3)

	puts := kvStoreSink.Puts()
	assert.Len(t, puts, 3)

	// shutdown
	cancel()
	time.Sleep(time.Second)
	sinker.Close()
}

func TestNATSProjectionAfterData(t *testing.T) {
	ctx := context.Background()

	dbConfig := shared.Setup(t)

	uri := runNatsContainer(t)

	esRepo, err := mysql.NewStoreWithURL[ids.AggID](dbConfig.URL())
	require.NoError(t, err)
	es := eventsourcing.NewEventStore[*test.Account](esRepo, test.NewJSONCodec(), &eventsourcing.EsOptions{})

	ctx, cancel := context.WithCancel(context.Background())

	// sinker provider
	kvStoreSink := &integration.MockKVStore{}
	sinker, err := nats.NewSink[ids.AggID](kvStoreSink, logger, topic, 1, uri)
	require.NoError(t, err)
	integration.EventForwarderWorker(t, ctx, logger, dbConfig, sinker)

	acc, err := test.NewAccount("Paulo", 100)
	require.NoError(t, err)
	acc.Deposit(10)
	acc.Deposit(20)

	err = es.Create(ctx, acc)
	require.NoError(t, err)

	// giving time to forward events
	time.Sleep(time.Second)

	// after data (replay): start projection after we have some data on the event bus
	kvStoreProj := &integration.MockKVStore{}
	proj := projectionFromNATS(t, ctx, uri, esRepo, kvStoreProj)

	balance, ok := proj.BalanceByID(acc.GetID())
	require.True(t, ok)
	require.Equal(t, integration.Balance{
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

	events := proj.Events()
	assert.Len(t, events, 5)
	assert.Equal(t, ids.Zero, events[0].AggregateID) // control event (switch)

	balance, ok = proj.BalanceByID(acc.GetID())
	require.True(t, ok)
	require.Equal(t, integration.Balance{
		Name:   "Paulo",
		Amount: 115,
	}, balance)

	putsProj := kvStoreProj.Puts()
	assert.Len(t, putsProj, 4)

	puts := kvStoreSink.Puts()
	assert.Len(t, puts, 4)

	// shutdown
	cancel()
	time.Sleep(time.Second)
	sinker.Close()
}

// natsContainer represents the nats container type used in the module
type natsContainer struct {
	testcontainers.Container
	URI string
}

// runContainer creates an instance of the nats container type
func runNatsContainer(t *testing.T) string {
	ctx := context.Background()
	req := testcontainers.ContainerRequest{
		Image:        "nats:2.9",
		ExposedPorts: []string{"4222/tcp", "6222/tcp", "8222/tcp"},
		Cmd:          []string{"-DV", "-js"},
		WaitingFor:   wait.ForLog("Listening for client connections on 0.0.0.0:4222"),
	}

	genericContainerReq := testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	}
	container, err := testcontainers.GenericContainer(ctx, genericContainerReq)
	require.NoError(t, err)

	t.Cleanup(func() {
		assert.NoError(t, container.Terminate(context.Background()), "failed to terminate container")
	})

	mappedPort, err := container.MappedPort(ctx, "4222/tcp")
	require.NoError(t, err)

	hostIP, err := container.Host(ctx)
	require.NoError(t, err)

	return fmt.Sprintf("nats://%s:%s", hostIP, mappedPort.Port())
}

func projectionFromNATS(t *testing.T, ctx context.Context, uri string, esRepo *mysql.EsRepository[ids.AggID, *ids.AggID], kvStore *integration.MockKVStore) *integration.ProjectionMock[ids.AggID] {
	// create projection
	proj := integration.NewProjectionMock[ids.AggID]("balances", test.NewJSONCodec())

	topic := projection.ConsumerTopic{
		Topic:      "accounts",
		Partitions: []uint32{1},
	}

	sub, err := pnats.NewSubscriberWithURL[ids.AggID](ctx, logger, uri, topic)
	require.NoError(t, err)

	// repository here could be remote, like GrpcRepository
	projector := projection.Project[ids.AggID](logger, nil, esRepo, sub, proj, kvStore, 1)
	ok, err := projector.Start(ctx)
	require.NoError(t, err)
	require.True(t, ok)

	// giving time to catchup and project events from the database
	time.Sleep(time.Second)

	return proj
}
