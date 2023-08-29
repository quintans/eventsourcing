package mongodb

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/quintans/eventsourcing/test"
	"github.com/stretchr/testify/require"
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
	test.DockerCompose(t, dockerComposePath, "mongo", time.Second)

	DBURL := fmt.Sprintf("mongodb://localhost:27017/%s?replicaSet=rs0", DBName)

	opts := options.Client().ApplyURI(DBURL)
	client, err := mongo.Connect(ctx, opts)
	require.NoError(t, err)
	defer client.Disconnect(context.Background())

	err = dbSchema(client)
	require.NoError(t, err)

	return dbConfig
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
