package mongodb

import (
	"fmt"
	"testing"
	"time"

	"github.com/quintans/eventsourcing/test"
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

	test.DockerCompose(t, dockerComposePath, "mongo", time.Second)

	return dbConfig
}
