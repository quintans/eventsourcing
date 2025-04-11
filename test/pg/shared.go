package pg

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"testing"
	"time"

	"github.com/docker/go-connections/nat"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
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

func (c DBConfig) ReplicationURL() string {
	return c.URL() + "&replication=database"
}

func (c DBConfig) URL() string {
	return fmt.Sprintf("postgres://%s:%s@%s:%d/%s?sslmode=disable", c.Username, c.Password, c.Host, c.Port, c.Database)
}

func setup() DBConfig {
	dbConfig := DBConfig{
		Database: "eventsourcing",
		Host:     "localhost",
		Port:     5432,
		Username: "postgres",
		Password: "postgres",
	}
	tcpPort := strconv.Itoa(dbConfig.Port)
	natPort := nat.Port(tcpPort)

	req := testcontainers.ContainerRequest{
		Image:        "postgres:12.3",
		ExposedPorts: []string{tcpPort + "/tcp"},
		Env: map[string]string{
			"POSTGRES_USER":     dbConfig.Username,
			"POSTGRES_PASSWORD": dbConfig.Password,
			"POSTGRES_DB":       dbConfig.Database,
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
		log.Fatalf("Failed to start container: %v", err)
	}

	ip, err := container.Host(ctx)
	if err != nil {
		log.Fatalf("Failed to get container host: %v", err)
	}

	port, err := container.MappedPort(ctx, natPort)
	if err != nil {
		log.Fatalf("Failed to get container mapped port: %v", err)
	}

	dbConfig.Host = ip
	dbConfig.Port = port.Int()

	return dbConfig
}

func connect(t *testing.T, dbConfig DBConfig) *sqlx.DB {
	dburl := fmt.Sprintf("postgres://%s:%s@%s:%d/%s?sslmode=disable", dbConfig.Username, dbConfig.Password, dbConfig.Host, dbConfig.Port, dbConfig.Database)

	db, err := sqlx.Open("postgres", dburl)
	require.NoError(t, err)

	err = db.Ping()
	require.NoError(t, err)

	return db
}
