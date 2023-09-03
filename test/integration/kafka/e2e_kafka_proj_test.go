//go:build integration

package kafka

import (
	"context"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/quintans/eventsourcing"
	"github.com/quintans/eventsourcing/projection"
	pkafka "github.com/quintans/eventsourcing/projection/kafka"
	"github.com/quintans/eventsourcing/sink/kafka"
	"github.com/quintans/eventsourcing/store/mysql"
	"github.com/quintans/eventsourcing/test"
	"github.com/quintans/eventsourcing/test/integration"
	shared "github.com/quintans/eventsourcing/test/mysql"
	"github.com/quintans/eventsourcing/util"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
)

var logger = slog.New(slog.NewTextHandler(os.Stdout, nil))

const (
	database = "eventsourcing"
	topic    = "accounts"
)

func TestKafkaProjectionBeforeData(t *testing.T) {
	uris := runKafkaContainer(t)

	dbConfig := shared.Setup(t)

	esRepo, err := mysql.NewStoreWithURL(dbConfig.URL())
	require.NoError(t, err)
	es := eventsourcing.NewEventStore[*test.Account](esRepo, test.NewJSONCodec(), &eventsourcing.EsOptions{})

	ctx, cancel := context.WithCancel(context.Background())

	// sinker provider
	sinker, err := kafka.NewSink(logger, &integration.MockKVStore{}, topic, uris)
	require.NoError(t, err)
	integration.EventForwarderWorker(t, ctx, logger, dbConfig, sinker)

	// before data
	proj := projectionFromKafka(t, ctx, uris, esRepo)

	id := util.NewID()
	acc, err := test.CreateAccount("Paulo", id, 100)
	require.NoError(t, err)
	acc.Deposit(10)
	acc.Deposit(20)

	err = es.Create(ctx, acc)
	require.NoError(t, err)

	// giving time to forward events
	time.Sleep(time.Second)

	events := proj.Events()
	assert.Len(t, events, 3)

	balance, ok := proj.BalanceByID(acc.GetID())
	assert.True(t, ok)
	assert.Equal(t, integration.Balance{
		Name:   "Paulo",
		Amount: 130,
	}, balance)

	// shutdown
	cancel()
	time.Sleep(time.Second)
	sinker.Close()
}

func TestKafkaProjectionAfterData(t *testing.T) {
	uris := runKafkaContainer(t)

	dbConfig := shared.Setup(t)

	esRepo, err := mysql.NewStoreWithURL(dbConfig.URL())
	require.NoError(t, err)
	es := eventsourcing.NewEventStore[*test.Account](esRepo, test.NewJSONCodec(), &eventsourcing.EsOptions{})

	ctx, cancel := context.WithCancel(context.Background())

	// sinker provider
	sinker, err := kafka.NewSink(logger, &integration.MockKVStore{}, topic, uris)
	require.NoError(t, err)
	integration.EventForwarderWorker(t, ctx, logger, dbConfig, sinker)

	id := util.NewID()
	acc, err := test.CreateAccount("Paulo", id, 100)
	require.NoError(t, err)
	acc.Deposit(10)
	acc.Deposit(20)

	err = es.Create(ctx, acc)
	require.NoError(t, err)

	// giving time to forward events
	time.Sleep(time.Second)

	// after data (replay): start projection after we have some data on the event bus
	proj := projectionFromKafka(t, ctx, uris, esRepo)

	balance, ok := proj.BalanceByID(acc.GetID())
	require.True(t, ok)
	require.Equal(t, integration.Balance{
		Name:   "Paulo",
		Amount: 130,
	}, balance)

	// updating after the subscription is in place
	es.Update(ctx, acc.GetID(), func(a *test.Account) (*test.Account, error) {
		acc.Withdraw(15)
		return acc, nil
	})

	// giving time to project events through the subscription
	time.Sleep(time.Second)

	events := proj.Events()
	assert.Len(t, events, 4)

	balance, ok = proj.BalanceByID(acc.GetID())
	require.True(t, ok)
	require.Equal(t, integration.Balance{
		Name:   "Paulo",
		Amount: 115,
	}, balance)

	// shutdown
	cancel()
	time.Sleep(time.Second)
	sinker.Close()
}

// kafkaContainer represents the Kafka container type used in the module
type kafkaContainer struct {
	testcontainers.Container
	URI string
}

// runKafkaContainer creates an instance of the Kafka container type
func runKafkaContainer(t *testing.T) []string {
	test.DockerCompose(t, "./docker-compose.yaml", "kafka", time.Second)
	return []string{"localhost:29092"}
}

func projectionFromKafka(t *testing.T, ctx context.Context, uri []string, esRepo *mysql.EsRepository) *integration.ProjectionMock {
	// create projection
	proj := integration.NewProjectionMock("balances")

	kvStore := &integration.MockKVStore{}

	sub, err := pkafka.NewSubscriberWithBrokers(ctx, logger, uri, "accounts", nil)
	require.NoError(t, err)

	// repository here could be remote, like GrpcRepository
	projector := projection.Project(logger, nil, esRepo, sub, proj, 1, kvStore)

	ok, err := projector.Start(ctx)
	require.NoError(t, err)
	require.True(t, ok)

	// giving time to catchup and project events from the database and for the consumer group to be ready
	time.Sleep(10 * time.Second)

	return proj
}
