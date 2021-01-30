package mongodb

import (
	"context"
	"fmt"
	"log"

	"github.com/quintans/faults"
	testcontainers "github.com/testcontainers/testcontainers-go"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

var (
	DBURL  string
	client *mongo.Client
)

const (
	DBName        = "eventstore"
	CollSnapshots = "snapshots"
	CollEvents    = "events"
)

func Setup(dockerComposePath string) (func(), error) {
	ctx := context.Background()

	destroy, err := dockerCompose(ctx, dockerComposePath)
	if err != nil {
		return nil, err
	}

	DBURL = fmt.Sprintf("mongodb://localhost:27017/%s?replicaSet=rs0", DBName)

	opts := options.Client().ApplyURI(DBURL)
	client, err = mongo.Connect(ctx, opts)
	if err != nil {
		destroy()
		return nil, err
	}

	tearDown := func() {
		client.Disconnect(ctx)
		destroy()
	}

	err = dbSchema(client)
	if err != nil {
		tearDown()
		return nil, err
	}

	return tearDown, nil
}

func dockerCompose(ctx context.Context, path string) (func(), error) {
	compose := testcontainers.NewLocalDockerCompose([]string{path}, "mongo-set")
	destroyFn := func() {
		exErr := compose.Down()
		if err := checkIfError(exErr); err != nil {
			log.Printf("Error on compose shutdown: %v\n", err)
		}
	}

	exErr := compose.Down()
	if err := checkIfError(exErr); err != nil {
		return nil, err
	}
	exErr = compose.
		WithCommand([]string{"up", "-d"}).
		Invoke()
	err := checkIfError(exErr)
	if err != nil {
		destroyFn()
		return nil, err
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

func dbSchema(cli *mongo.Client) error {
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

	db := cli.Database(DBName)
	for _, c := range cmds {
		err := db.RunCommand(context.Background(), c).Err()
		if err != nil {
			return err
		}
	}

	return nil
}
