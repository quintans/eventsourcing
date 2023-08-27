package mongodb

import (
	"context"
	"fmt"
	"log"
	"testing"

	"github.com/quintans/faults"
	"github.com/stretchr/testify/require"
	testcontainers "github.com/testcontainers/testcontainers-go"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type DBConfig struct {
	Database string
	Host     string
	Port     int
}

func (c DBConfig) URL() string {
	return fmt.Sprintf("mongodb://%s:%d/%s?replicaSet=rs0", c.Host, c.Port, c.Database)
}

const (
	DBName        = "eventsourcing"
	CollSnapshots = "snapshots"
	CollEvents    = "events"
)

func Setup(t *testing.T, dockerComposePath string) DBConfig {
	dbConfig := DBConfig{
		Database: DBName,
		Host:     "localhost",
		Port:     27017,
	}

	ctx := context.Background()
	dockerCompose(t, ctx, dockerComposePath)

	DBURL := fmt.Sprintf("mongodb://localhost:27017/%s?replicaSet=rs0", DBName)

	opts := options.Client().ApplyURI(DBURL)
	client, err := mongo.Connect(ctx, opts)
	require.NoError(t, err)
	defer client.Disconnect(context.Background())

	err = dbSchema(client)
	require.NoError(t, err)

	return dbConfig
}

func dockerCompose(t *testing.T, ctx context.Context, path string) {
	compose := testcontainers.NewLocalDockerCompose([]string{path}, "mongo-set")
	t.Cleanup(func() {
		exErr := compose.Down()
		if err := checkIfError(exErr); err != nil {
			log.Printf("Error on compose shutdown: %v\n", err)
		}
	})

	exErr := compose.Down()
	require.NoError(t, checkIfError(exErr), "Error on compose shutdown")

	exErr = compose.
		WithCommand([]string{"up", "-d"}).
		Invoke()
	require.NoError(t, checkIfError(exErr))
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
						{"migration", 1},
					}},
					{"name", "evt_agg_id_migrated_idx"},
					{"unique", false},
					{"background", true},
				},
				{
					{"key", bson.D{
						{"_id", 1},
						{"migration", 1},
					}},
					{"name", "evt_migration_idx"},
					{"unique", false},
					{"background", true},
				},
				{
					{"key", bson.D{
						{"aggregate_kind", 1},
						{"migration", 1},
					}},
					{"name", "evt_type_migrated_idx"},
					{"unique", false},
					{"background", true},
				},
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
						{"migration", 1},
					}},
					{"name", "idx_idempotency"},
					{"unique", true},
					{"partialFilterExpression", bson.D{
						{
							"idempotency_key", bson.D{
								{"$gt", ""},
							},
						},
					}},
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
		{
			{"create", "outbox"},
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
