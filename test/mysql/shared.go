package mysql

import (
	"context"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/docker/go-connections/nat"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	"github.com/jmoiron/sqlx"
	"github.com/quintans/faults"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	testcontainers "github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

type DBConfig struct {
	Database string
	Host     string
	Port     int
	Username string
	Password string
}

func (c DBConfig) URL() string {
	return fmt.Sprintf("%s:%s@(%s:%d)/%s?parseTime=true", c.Username, c.Password, c.Host, c.Port, c.Database)
}

func Setup(t *testing.T) DBConfig {
	dbConfig := DBConfig{
		Database: "eventsourcing",
		Host:     "localhost",
		Port:     3306,
		Username: "root",
		Password: "example",
	}

	tcpPort := strconv.Itoa(dbConfig.Port)
	natPort := nat.Port(tcpPort)

	req := testcontainers.ContainerRequest{
		Image:        "mariadb:10.2",
		ExposedPorts: []string{tcpPort + "/tcp"},
		Env: map[string]string{
			"MYSQL_ROOT_PASSWORD": dbConfig.Password,
			"MYSQL_DATABASE":      dbConfig.Database,
		},
		Cmd:        []string{"--log-bin", "--binlog-format=ROW"},
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

	dbURL := fmt.Sprintf("%s:%s@(%s:%s)/%s", dbConfig.Username, dbConfig.Password, ip, port.Port(), dbConfig.Database)
	err = dbSchema(dbURL)
	require.NoError(t, err)

	return dbConfig
}

func dbSchema(dbURL string) error {
	db, err := sqlx.Connect("mysql", dbURL)
	if err != nil {
		return faults.Errorf("connecting to db: %w", err)
	}
	defer db.Close()

	cmds := []string{
		`CREATE TABLE IF NOT EXISTS events(
			id VARCHAR (50) PRIMARY KEY,
			aggregate_id VARCHAR (50) NOT NULL,
			aggregate_id_hash INTEGER NOT NULL,
			aggregate_version INTEGER NOT NULL,
			aggregate_kind VARCHAR (50) NOT NULL,
			kind VARCHAR (50) NOT NULL,
			body VARBINARY(60000),
			idempotency_key VARCHAR (50),
			created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
			migration INTEGER NOT NULL DEFAULT 0,
			migrated BOOLEAN NOT NULL DEFAULT false,
			meta_tenant VARCHAR (50) NULL
		)ENGINE=innodb;`,
		`CREATE INDEX evt_agg_id_migrated_idx ON events (aggregate_id, migration);`,
		`CREATE INDEX evt_type_migrated_idx ON events (aggregate_kind, migration);`,
		`CREATE UNIQUE INDEX evt_agg_id_ver_uk ON events (aggregate_id, aggregate_version);`,
		`CREATE UNIQUE INDEX evt_idempot_uk ON events (idempotency_key, migration);`,
		`CREATE INDEX evt_tenant_idx ON events (meta_tenant);`,

		`CREATE TABLE IF NOT EXISTS snapshots(
			id VARCHAR (50) PRIMARY KEY,
			aggregate_id VARCHAR (50) NOT NULL,
			aggregate_version INTEGER NOT NULL,
			aggregate_kind VARCHAR (50) NOT NULL,
			body VARBINARY(60000) NOT NULL,
			created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
			meta_tenant VARCHAR (50) NULL,
			FOREIGN KEY (id) REFERENCES events (id)
		)ENGINE=innodb;`,
		`CREATE INDEX agg_id_idx ON snapshots(aggregate_id);`,
		`CREATE TABLE IF NOT EXISTS outbox(
			id VARCHAR (50) PRIMARY KEY,
			aggregate_id VARCHAR (50) NOT NULL,
			aggregate_id_hash INTEGER NOT NULL,
			aggregate_kind VARCHAR (50) NOT NULL,
			kind VARCHAR (50) NOT NULL,
			meta_tenant VARCHAR (50) NULL
		);`,
	}

	for _, cmd := range cmds {
		_, err := db.Exec(cmd)
		if err != nil {
			return faults.Errorf("failed to execute '%s': %w", cmd, err)
		}
	}

	return nil
}
