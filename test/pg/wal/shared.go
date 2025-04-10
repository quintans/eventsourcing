package wal

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/docker/go-connections/nat"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	_ "github.com/lib/pq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	testcontainers "github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	tpg "github.com/quintans/eventsourcing/test/pg"
)

func setup(t *testing.T) tpg.DBConfig {
	dbConfig := tpg.DBConfig{
		Database: "pglogrepl",
		Host:     "localhost",
		Port:     5432,
		Username: "pglogrepl",
		Password: "secret",
	}

	tcpPort := strconv.Itoa(dbConfig.Port)
	natPort := nat.Port(tcpPort)

	req := testcontainers.ContainerRequest{
		FromDockerfile: testcontainers.FromDockerfile{
			Context: ".",
		},
		// Image:        "postgres:12.3",
		ExposedPorts: []string{tcpPort + "/tcp"},
		Env: map[string]string{
			"POSTGRES_USER":     "root",
			"POSTGRES_PASSWORD": "postgres",
			"POSTGRES_DB":       dbConfig.Database,
			"PG_REP_USER":       dbConfig.Username,
			"PG_REP_PASSWORD":   dbConfig.Password,
		},
		WaitingFor: wait.ForListeningPort(natPort),
	}
	time.Sleep(3 * time.Second)
	ctx := context.Background()
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	require.NoError(t, err)

	t.Cleanup(func() {
		assert.NoError(t, container.Terminate(ctx))
	})

	ip, err := container.Host(ctx)
	require.NoError(t, err)

	port, err := container.MappedPort(ctx, natPort)
	require.NoError(t, err)

	dbConfig.Host = ip
	dbConfig.Port = port.Int()

	return dbConfig
}
