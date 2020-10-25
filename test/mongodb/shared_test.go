package mongodb

import (
	"context"
	"fmt"
	"log"
	"os"
	"testing"

	"github.com/docker/go-connections/nat"
	testcontainers "github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

var (
	dbURL  string
	client *mongo.Client
)

const (
	dbName        = "eventstore"
	collSnapshots = "snapshots"
	collEvents    = "events"
)

func TestMain(m *testing.M) {
	ctx := context.Background()

	dbContainer, err := bootstrapDbContainer(ctx)
	if err != nil {
		log.Fatal(err)
	}

	defer dbContainer.Terminate(ctx)
	err = dbSchema()
	if err != nil {
		log.Fatal(err)
	}

	// test run
	os.Exit(m.Run())
}

func bootstrapDbContainer(ctx context.Context) (testcontainers.Container, error) {
	tcpPort := "27017"
	natPort := nat.Port(tcpPort)

	req := testcontainers.ContainerRequest{
		Image:        "mongo:latest",
		ExposedPorts: []string{tcpPort + "/tcp"},
		Env: map[string]string{
			"MONGO_INITDB_ROOT_USERNAME": "root",
			"MONGO_INITDB_ROOT_PASSWORD": "password",
		},
		WaitingFor: wait.ForListeningPort(natPort),
	}
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		return nil, err
	}

	ip, err := container.Host(ctx)
	if err != nil {
		container.Terminate(ctx)
		return nil, err
	}
	port, err := container.MappedPort(ctx, natPort)
	if err != nil {
		container.Terminate(ctx)
		return nil, err
	}

	dbURL = fmt.Sprintf("mongodb://root:password@%s:%s/%s?authSource=admin", ip, port.Port(), dbName)

	opts := options.Client().ApplyURI(dbURL)
	client, err = mongo.Connect(ctx, opts)
	if err != nil {
		return nil, err
	}

	return container, nil
}

func dbSchema() error {
	cmds := []bson.D{
		{
			{"create", "events"},
		},
		{
			{"createIndexes", "events"},
			{"indexes", []bson.D{
				{
					{"key", bson.D{
						{"aggregate_id", 1},
						{"aggregate_version", 1},
					}},
					{"name", "unique_aggregate_version"},
					{"unique", true},
					{"background", true},
				},
				{
					{"key", bson.D{
						{"idempotency_key", 1},
						{"aggregate_id", 1},
					}},
					{"name", "idx_idempotency_aggregate"},
					{"background", true},
				},
			}},
		},
		{
			{"create", "snapshots"},
		},
		{
			{"createIndexes", "snapshots"},
			{"indexes", []bson.D{
				{
					{"key", bson.D{
						{"aggregate_id", 1},
					}},
					{"name", "idx_aggregate"},
					{"background", true},
				},
			}},
		},
	}

	db := client.Database(dbName)
	for _, c := range cmds {
		err := db.RunCommand(context.Background(), c).Err()
		if err != nil {
			return err
		}
	}

	return nil
}
