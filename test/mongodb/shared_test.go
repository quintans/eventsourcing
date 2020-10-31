package mongodb

import (
	"context"
	"fmt"
	"log"
	"os"
	"testing"

	testcontainers "github.com/testcontainers/testcontainers-go"
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

	destroy, err := dockerCompose(ctx)
	if err != nil {
		destroy()
		log.Fatal(err)
	}

	err = dbSchema()
	if err != nil {
		destroy()
		log.Fatal(err)
	}

	// test run
	code := m.Run()
	destroy()
	os.Exit(code)
}

func dockerCompose(ctx context.Context) (func(), error) {
	path := "./docker-compose.yaml"

	compose := testcontainers.NewLocalDockerCompose([]string{path}, "mongo-set")
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
		WithCommand([]string{"up", "-d"}).
		Invoke()
	err := checkIfError(exErr)
	if err != nil {
		return destroyFn, err
	}

	dbURL = fmt.Sprintf("mongodb://localhost:27017/%s?replicaSet=rs0", dbName)

	opts := options.Client().ApplyURI(dbURL)
	client, err = mongo.Connect(ctx, opts)

	return destroyFn, err
}

func checkIfError(err testcontainers.ExecError) error {
	if err.Error != nil {
		return fmt.Errorf("Failed when running %v: %v", err.Command, err.Error)
	}

	if err.Stdout != nil {
		return fmt.Errorf("An error in Stdout happened when running %v: %v", err.Command, err.Stdout)
	}

	if err.Stderr != nil {
		return fmt.Errorf("An error in Stderr happened when running %v: %v", err.Command, err.Stderr)
	}
	return nil
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
