package mysql

import (
	"context"
	"fmt"
	"strconv"

	"github.com/docker/go-connections/nat"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	"github.com/jmoiron/sqlx"
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

func (c DBConfig) Url() string {
	return fmt.Sprintf("%s:%s@(%s:%d)/%s?parseTime=true", c.Username, c.Password, c.Host, c.Port, c.Database)
}

func setup() (DBConfig, func(), error) {
	dbConfig := DBConfig{
		Database: "eventstore",
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
	ctx := context.Background()
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		return DBConfig{}, nil, err
	}

	tearDown := func() {
		container.Terminate(ctx)
	}

	ip, err := container.Host(ctx)
	if err != nil {
		tearDown()
		return DBConfig{}, nil, err
	}
	port, err := container.MappedPort(ctx, natPort)
	if err != nil {
		tearDown()
		return DBConfig{}, nil, err
	}

	dbConfig.Host = ip
	dbConfig.Port = port.Int()

	dbURL := fmt.Sprintf("%s:%s@(%s:%s)/%s", dbConfig.Username, dbConfig.Password, ip, port.Port(), dbConfig.Database)
	err = dbSchema(dbURL)
	if err != nil {
		tearDown()
		return DBConfig{}, nil, err
	}

	return dbConfig, tearDown, nil
}

func dbSchema(dbURL string) error {
	db, err := sqlx.Connect("mysql", dbURL)
	if err != nil {
		return err
	}
	defer db.Close()

	cmds := []string{
		`CREATE TABLE IF NOT EXISTS events(
			id VARCHAR (50) PRIMARY KEY,
			aggregate_id VARCHAR (50) NOT NULL,
			aggregate_id_hash INTEGER NOT NULL,
			aggregate_version INTEGER NOT NULL,
			aggregate_type VARCHAR (50) NOT NULL,
			kind VARCHAR (50) NOT NULL,
			body VARBINARY(60000) NOT NULL,
			idempotency_key VARCHAR (50),
			metadata JSON NOT NULL,
			created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
		)ENGINE=innodb;`,
		`CREATE UNIQUE INDEX agg_id_ver_idx ON events(aggregate_id, aggregate_version);`,
		`CREATE UNIQUE INDEX agg_idempot_idx ON events(aggregate_type, idempotency_key);`,
		`CREATE INDEX agg_id_idx ON events(aggregate_id);`,

		`CREATE TABLE IF NOT EXISTS snapshots(
			id VARCHAR (50) PRIMARY KEY,
			aggregate_id VARCHAR (50) NOT NULL,
			aggregate_version INTEGER NOT NULL,
			aggregate_type VARCHAR (50) NOT NULL,
			body VARBINARY(60000) NOT NULL,
			created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
			FOREIGN KEY (id) REFERENCES events (id)
		)ENGINE=innodb;`,
		`CREATE INDEX agg_id_idx ON snapshots(aggregate_id);`,
	}

	for _, cmd := range cmds {
		_, err := db.Exec(cmd)
		if err != nil {
			return fmt.Errorf("failed to execute '%s': %w", cmd, err)
		}
	}

	return nil
}
