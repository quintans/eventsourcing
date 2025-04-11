package mongodb

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"strconv"
	"time"

	"github.com/avast/retry-go/v3"
	"github.com/quintans/eventsourcing/test"
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
	return fmt.Sprintf("mongodb://%s:%d/%s?directConnection=true", c.Host, c.Port, c.Database)
}

const (
	DBName        = "eventsourcing"
	CollSnapshots = "snapshots"
	CollEvents    = "events"
)

func Setup(dockerComposePath string) DBConfig {
	dbConfig := DBConfig{
		Database: DBName,
		Host:     "localhost",
		Port:     1024 + rand.Intn(65535-1024),
	}

	ctx := context.Background()
	test.DockerCompose(dockerComposePath, "mongo", map[string]string{
		"MONGO_PORT": strconv.Itoa(dbConfig.Port),
	})

	opts := options.Client().ApplyURI(dbConfig.URL())
	client, err := mongo.Connect(ctx, opts)
	if err != nil {
		log.Fatalf("Failed to connect to MongoDB: %s", err)
	}
	defer client.Disconnect(context.Background())

	err = retry.Do(
		func() error {
			return dbSchema(client)
		},
		retry.Attempts(3),
		retry.Delay(time.Second),
	)
	if err != nil {
		log.Fatalf("Failed to create MongoDB schema: %s", err)
	}

	return dbConfig
}

func dbSchema(cli *mongo.Client) error {
	cmds := []bson.D{
		{
			{Key: "create", Value: "keyvalue"},
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
