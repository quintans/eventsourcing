package wal

import (
	"context"
	"fmt"
	"strconv"

	"github.com/docker/go-connections/nat"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	"github.com/jackc/pgconn"
	_ "github.com/lib/pq"
	"github.com/quintans/faults"
	testcontainers "github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	tpg "github.com/quintans/eventsourcing/test/pg"
)

func setup() (tpg.DBConfig, func(), error) {
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
	ctx := context.Background()
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		return tpg.DBConfig{}, nil, faults.Wrap(err)
	}

	tearDown := func() {
		container.Terminate(ctx)
	}

	ip, err := container.Host(ctx)
	if err != nil {
		tearDown()
		return tpg.DBConfig{}, nil, faults.Wrap(err)
	}
	port, err := container.MappedPort(ctx, natPort)
	if err != nil {
		tearDown()
		return tpg.DBConfig{}, nil, faults.Wrap(err)
	}

	dbConfig.Host = ip
	dbConfig.Port = port.Int()

	err = dbSchema(dbConfig)
	if err != nil {
		tearDown()
		return tpg.DBConfig{}, nil, faults.Wrap(err)
	}

	return dbConfig, tearDown, nil
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
			idempotency_key VARCHAR (50),
			metadata JSONB,
			created_at TIMESTAMP NOT NULL DEFAULT NOW()::TIMESTAMP,
			migrated INTEGER NOT NULL DEFAULT 0
		);`,
		`CREATE INDEX evt_agg_id_migrated_idx ON events (aggregate_id, migrated);`,
		`CREATE INDEX evt_id_migrated_idx ON events (id, migrated);`,
		`CREATE INDEX evt_type_migrated_idx ON events (aggregate_kind, migrated);`,
		`CREATE UNIQUE INDEX evt_agg_id_ver_uk ON events (aggregate_id, aggregate_version);`,
		`CREATE UNIQUE INDEX evt_idempot_uk ON events (idempotency_key, migrated);`,
		`CREATE INDEX evt_metadata_idx ON events USING GIN (metadata jsonb_path_ops);`,

		`CREATE TABLE IF NOT EXISTS snapshots(
			id VARCHAR (50) PRIMARY KEY,
			aggregate_id VARCHAR (50) NOT NULL,
			aggregate_version INTEGER NOT NULL,
			aggregate_kind VARCHAR (50) NOT NULL,
			body bytea NOT NULL,
			created_at TIMESTAMP NOT NULL DEFAULT NOW()::TIMESTAMP,
			FOREIGN KEY (id) REFERENCES events (id)
		);`,
		`CREATE INDEX snap_agg_id_idx ON snapshots (aggregate_id);`,
		`CREATE PUBLICATION events_pub FOR TABLE events WITH (publish = 'insert');`,
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
