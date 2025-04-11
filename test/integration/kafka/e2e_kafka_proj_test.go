//go:build integration

package kafka

import (
	"context"
	"log/slog"
	"math/rand"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/quintans/eventsourcing"
	"github.com/quintans/eventsourcing/projection"
	pkafka "github.com/quintans/eventsourcing/projection/kafka"
	"github.com/quintans/eventsourcing/sink/kafka"
	"github.com/quintans/eventsourcing/store/mysql"
	"github.com/quintans/eventsourcing/test"
	"github.com/quintans/eventsourcing/test/integration"
	tMysql "github.com/quintans/eventsourcing/test/mysql"
	"github.com/quintans/eventsourcing/util/ids"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
)

var logger = slog.New(slog.NewTextHandler(os.Stdout, nil))

const (
	database = "eventsourcing"
)

var (
	kafkaUris []string
	dbConfig  tMysql.DBConfig
)

func TestMain(m *testing.M) {
	kafkaUris = runKafkaContainer()
	dbConfig = tMysql.Setup()

	// run the tests
	code := m.Run()

	// exit with the code from the tests
	os.Exit(code)
}

func TestKafkaProjectionBeforeData(t *testing.T) {
	t.Parallel()

	eventsTable := test.RandStr("events")
	esRepo, err := mysql.NewStoreWithURL(
		dbConfig.URL(),
		mysql.WithEventsTable[ids.AggID](eventsTable),
	)
	require.NoError(t, err)
	es, err := eventsourcing.NewEventStore[*test.Account](esRepo, test.NewJSONCodec())
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())

	// sinker provider
	topic := test.RandStr("accounts")
	kvStoreSink := &integration.MockKVStore{}
	sinker, err := kafka.NewSink[ids.AggID](logger, kvStoreSink, topic, kafkaUris, nil)
	require.NoError(t, err)
	integration.EventForwarderWorker(t, ctx, logger, dbConfig.DBConfig, sinker, mysql.WithFeedEventsTable[ids.AggID](eventsTable))

	// before data
	kvStoreProj := &integration.MockKVStore{}
	proj := projectionFromKafka(t, ctx, kafkaUris, topic, esRepo, kvStoreProj)

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
	assert.True(t, ok)
	assert.Equal(t, integration.Balance{
		Name:   "Paulo",
		Amount: 130,
	}, balance)

	putsProj := kvStoreProj.Puts()
	assert.Len(t, putsProj, 1)

	puts := kvStoreSink.Puts()
	assert.Len(t, puts, 3)

	// shutdown
	cancel()
	time.Sleep(time.Second)
	sinker.Close()
}

func TestKafkaProjectionAfterData(t *testing.T) {
	t.Parallel()

	eventsTable := test.RandStr("events")
	esRepo, err := mysql.NewStoreWithURL(
		dbConfig.URL(),
		mysql.WithEventsTable[ids.AggID](eventsTable),
	)
	require.NoError(t, err)
	es, err := eventsourcing.NewEventStore[*test.Account](esRepo, test.NewJSONCodec())
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())

	topic := test.RandStr("accounts")
	// sinker provider
	kvStoreSink := &integration.MockKVStore{}
	sinker, err := kafka.NewSink[ids.AggID](logger, kvStoreSink, topic, kafkaUris, nil)
	require.NoError(t, err)
	integration.EventForwarderWorker(t, ctx, logger, dbConfig.DBConfig, sinker, mysql.WithFeedEventsTable[ids.AggID](eventsTable))

	acc, err := test.NewAccount("Paulo", 100)
	require.NoError(t, err)
	acc.Deposit(10)
	acc.Deposit(20)

	err = es.Create(ctx, acc)
	require.NoError(t, err)

	// giving time to forward events
	time.Sleep(2 * time.Second)

	// after data (replay): start projection after we have some data on the event bus
	kvStoreProj := &integration.MockKVStore{}
	proj := projectionFromKafka(t, ctx, kafkaUris, topic, esRepo, kvStoreProj)

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
	require.Len(t, events, 5)
	assert.Equal(t, ids.Zero, events[3].AggregateID) // control event (switch)

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

// kafkaContainer represents the Kafka container type used in the module
type kafkaContainer struct {
	testcontainers.Container
	URI string
}

// runKafkaContainer creates an instance of the Kafka container type
func runKafkaContainer() []string {
	port := strconv.Itoa(1024 + rand.Intn(65535-1024))
	test.DockerCompose("./docker-compose.yaml", "kafka", map[string]string{
		"EXT_PORT": port,
	})
	return []string{"localhost:" + port}
}

func projectionFromKafka(t *testing.T, ctx context.Context, uri []string, topic string, esRepo *mysql.EsRepository[ids.AggID, *ids.AggID], kvStore *integration.MockKVStore) *integration.ProjectionMock[ids.AggID] {
	// create projection
	proj := integration.NewProjectionMock(test.RandStr("balances"), test.NewJSONCodec())

	sub, err := pkafka.NewSubscriberWithBrokers[ids.AggID](ctx, logger, uri, topic, nil)
	require.NoError(t, err)

	// repository here could be remote, like GrpcRepository
	projector := projection.Project(logger, nil, esRepo, sub, proj, kvStore, 1)

	ok, err := projector.Start(ctx)
	require.NoError(t, err)
	require.True(t, ok)

	// giving time to catchup and project events from the database and for the consumer group to be ready
	time.Sleep(10 * time.Second)

	return proj
}
