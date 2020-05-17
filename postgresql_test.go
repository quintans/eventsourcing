package eventstore

import (
	"context"
	"fmt"
	"log"
	"os"
	"testing"
	"time"

	"github.com/docker/go-connections/nat"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	testcontainers "github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

var (
	dbURL string
)

func TestMain(m *testing.M) {
	ctx := context.Background()

	dbContainer, err := bootstrapDbContainer(ctx)
	if err != nil {
		log.Fatal(err)
	}

	defer dbContainer.Terminate(ctx)
	err = dbSchema()
	if err != nil {
		log.Fatal(err)
	}

	// test run
	os.Exit(m.Run())
}

func bootstrapDbContainer(ctx context.Context) (testcontainers.Container, error) {
	tcpPort := "5432"
	natPort := nat.Port(tcpPort)

	req := testcontainers.ContainerRequest{
		Image:        "postgres:12.3",
		ExposedPorts: []string{tcpPort + "/tcp"},
		Env: map[string]string{
			"POSTGRES_USER":     "postgres",
			"POSTGRES_PASSWORD": "postgres",
			"POSTGRES_DB":       "eventstore",
		},
		WaitingFor: wait.ForListeningPort(natPort),
	}
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		return nil, err
	}

	ip, err := container.Host(ctx)
	if err != nil {
		container.Terminate(ctx)
		return nil, err
	}
	port, err := container.MappedPort(ctx, natPort)
	if err != nil {
		container.Terminate(ctx)
		return nil, err
	}

	dbURL = fmt.Sprintf("postgres://postgres:postgres@%s:%s/eventstore?sslmode=disable", ip, port.Port())
	return container, nil
}

func dbSchema() error {
	db, err := sqlx.Connect("postgres", dbURL)
	if err != nil {
		return err
	}

	db.MustExec(`
	CREATE TABLE IF NOT EXISTS events(
		id BIGSERIAL PRIMARY KEY,
		aggregate_id VARCHAR (50) NOT NULL,
		aggregate_version INTEGER NOT NULL,
		aggregate_type VARCHAR (50) NOT NULL,
		kind VARCHAR (50) NOT NULL,
		body JSONB NOT NULL,
		created_at TIMESTAMP NOT NULL DEFAULT NOW()::TIMESTAMP,
		UNIQUE (aggregate_id, aggregate_version)
	);
		
	CREATE TABLE IF NOT EXISTS snapshots(
		id BIGINT PRIMARY KEY,
		aggregate_id VARCHAR (50) NOT NULL,
		aggregate_version INTEGER NOT NULL,
		body JSONB NOT NULL,
		created_at TIMESTAMP NOT NULL DEFAULT NOW()::TIMESTAMP,
		FOREIGN KEY (id) REFERENCES events (id)
	 );	 
	`)

	return nil
}

func TestSave(t *testing.T) {
	es, err := NewESPostgreSQL(dbURL, 3)
	require.NoError(t, err)

	id := uuid.New().String()
	acc := CreateAccount(id, 100)
	acc.Deposit(10)
	acc.Deposit(20)
	acc.Deposit(5)
	err = es.Save(acc)
	require.NoError(t, err)
	time.Sleep(time.Second)

	evts := []PgEvent{}
	err = es.db.Select(&evts, "SELECT * FROM events")
	require.NoError(t, err)
	require.Equal(t, 4, len(evts))
	assert.Equal(t, "AccountCreated", evts[0].Kind)
	assert.Equal(t, "Account", evts[0].AggregateType)
	assert.Equal(t, id, evts[0].AggregateID)
	assert.Equal(t, 1, evts[0].AggregateVersion)

	acc2 := NewAccount()
	err = es.GetByID(id, acc2)
	require.NoError(t, err)
	assert.Equal(t, acc.ID, acc2.ID)
	assert.Equal(t, acc.Version, acc2.Version)
	assert.Equal(t, acc.Balance, acc2.Balance)
	assert.Equal(t, acc.Status, acc2.Status)
}
