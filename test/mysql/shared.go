package mysql

import (
	"context"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/docker/go-connections/nat"
	_ "github.com/golang-migrate/migrate/v4/source/file"
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

	return dbConfig
}
