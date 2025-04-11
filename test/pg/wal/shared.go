package wal

import (
	"context"
	"log"
	"strconv"
	"time"

	"github.com/docker/go-connections/nat"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	_ "github.com/lib/pq"
	testcontainers "github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	tpg "github.com/quintans/eventsourcing/test/pg"
)

func setup() tpg.DBConfig {
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
	if err != nil {
		log.Fatalf("failed to start container: %v", err)
	}

	ip, err := container.Host(ctx)
	if err != nil {
		log.Fatalf("failed to get container host: %v", err)
	}

	port, err := container.MappedPort(ctx, natPort)
	if err != nil {
		log.Fatalf("failed to get container port: %v", err)
	}

	dbConfig.Host = ip
	dbConfig.Port = port.Int()

	return dbConfig
}
