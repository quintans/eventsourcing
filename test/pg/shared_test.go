package pg

import (
	"context"
	"fmt"
	"log"
	"os"
	"testing"

	"github.com/docker/go-connections/nat"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
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

	err = dbSchema()
	if err != nil {
		dbContainer.Terminate(ctx)
		log.Fatal(err)
	}

	// test run
	code := m.Run()
	dbContainer.Terminate(ctx)
	os.Exit(code)
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
		id VARCHAR (50) PRIMARY KEY,
		aggregate_id VARCHAR (50) NOT NULL,
		aggregate_version INTEGER NOT NULL,
		aggregate_type VARCHAR (50) NOT NULL,
		kind VARCHAR (50) NOT NULL,
		body JSONB NOT NULL,
		idempotency_key VARCHAR (50),
		labels JSONB NOT NULL,
		created_at TIMESTAMP NOT NULL DEFAULT NOW()::TIMESTAMP,
		UNIQUE (aggregate_id, aggregate_version)
	);
	CREATE INDEX aggregate_idx ON events (aggregate_id, aggregate_version);
	CREATE INDEX idempotency_key_idx ON events (idempotency_key, aggregate_id);
	CREATE INDEX labels_idx ON events USING GIN (labels jsonb_path_ops);

	CREATE TABLE IF NOT EXISTS snapshots(
		id VARCHAR (50) PRIMARY KEY,
		aggregate_id VARCHAR (50) NOT NULL,
		aggregate_version INTEGER NOT NULL,
		aggregate_type VARCHAR (50) NOT NULL,
		body JSONB NOT NULL,
		created_at TIMESTAMP NOT NULL DEFAULT NOW()::TIMESTAMP,
		FOREIGN KEY (id) REFERENCES events (id)
	);
	CREATE INDEX aggregate_id_idx ON snapshots (aggregate_id);
	
	CREATE OR REPLACE FUNCTION notify_event() RETURNS TRIGGER AS $FN$
		DECLARE 
			notification json;
		BEGIN
			notification = row_to_json(NEW);
			PERFORM pg_notify('events_channel', notification::text);
			
			-- Result is ignored since this is an AFTER trigger
			RETURN NULL; 
		END;
	$FN$ LANGUAGE plpgsql;
	
	CREATE TRIGGER events_notify_event
	AFTER INSERT ON events
		FOR EACH ROW EXECUTE PROCEDURE notify_event();
	`)

	return nil
}
