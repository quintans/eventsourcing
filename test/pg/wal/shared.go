package wal

import (
	"context"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/avast/retry-go/v3"
	"github.com/docker/go-connections/nat"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	"github.com/jackc/pgx/v5/pgconn"
	_ "github.com/lib/pq"
	"github.com/quintans/faults"
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

	tcpPort := strconv.Itoa(5432)
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

	err = retry.Do(
		func() error {
			return dbSchema(dbConfig)
		},
		retry.Attempts(3),
		retry.Delay(time.Second),
	)
	require.NoError(t, err)

	return dbConfig
}

func dbSchema(config tpg.DBConfig) error {
	dburl := fmt.Sprintf("postgres://%s:%s@%s:%d/%s?replication=database&sslmode=disable", config.Username, config.Password, config.Host, config.Port, config.Database)
	conn, err := pgconn.Connect(context.Background(), dburl)
	if err != nil {
		return faults.Errorf("failed to connect %s: %w", dburl, err)
	}
	defer conn.Close(context.Background())

	sqls := []string{
		`CREATE TABLE IF NOT EXISTS events(
			id VARCHAR (50) PRIMARY KEY,
			aggregate_id VARCHAR (50) NOT NULL,
			aggregate_id_hash INTEGER NOT NULL,
			aggregate_version INTEGER NOT NULL,
			aggregate_kind VARCHAR (50) NOT NULL,
			kind VARCHAR (50) NOT NULL,
			body bytea,
			metadata JSONB,
			created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			migration INTEGER NOT NULL DEFAULT 0,
			migrated BOOLEAN NOT NULL DEFAULT false
		);`,
		`CREATE INDEX evt_agg_id_migrated_idx ON events (aggregate_id, migration);`,
		`CREATE INDEX evt_id_migrated_idx ON events (id, migration);`,
		`CREATE INDEX evt_type_migrated_idx ON events (aggregate_kind, migration);`,
		`CREATE UNIQUE INDEX evt_agg_id_ver_uk ON events (aggregate_id, aggregate_version);`,
		`CREATE INDEX evt_metadata_idx ON events USING GIN (metadata jsonb_path_ops);`,

		`CREATE TABLE IF NOT EXISTS snapshots(
			id VARCHAR (50) PRIMARY KEY,
			aggregate_id VARCHAR (50) NOT NULL,
			aggregate_version INTEGER NOT NULL,
			aggregate_kind VARCHAR (50) NOT NULL,
			body bytea NOT NULL,
			created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			FOREIGN KEY (id) REFERENCES events (id)
		);`,
		`CREATE INDEX snap_agg_id_idx ON snapshots (aggregate_id);`,
		`CREATE PUBLICATION events_pub FOR TABLE events WITH (publish = 'insert');`,
		`CREATE TABLE IF NOT EXISTS outbox(
			id VARCHAR (50) PRIMARY KEY,
			aggregate_id VARCHAR (50) NOT NULL,
			aggregate_id_hash INTEGER NOT NULL,
			aggregate_kind VARCHAR (50) NOT NULL,
			kind VARCHAR (50) NOT NULL,
			metadata JSONB
		);`,
	}

	for _, s := range sqls {
		result := conn.Exec(context.Background(), s)
		_, err := result.ReadAll()
		if err != nil {
			return faults.Errorf("failed to execute %s: %w", s, err)
		}
	}

	return nil
}
