package eventstore

import (
	"context"
	"encoding/json"
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
		body JSONB NOT NULL,
		created_at TIMESTAMP NOT NULL DEFAULT NOW()::TIMESTAMP,
		FOREIGN KEY (id) REFERENCES events (id)
	 );
	 CREATE INDEX aggregate_id_idx ON snapshots (aggregate_id);
	`)

	return nil
}

type MockLocker struct {
}

func (m MockLocker) Lock() bool {
	return true
}

func (m MockLocker) Unlock() {}

func TestSaveAndGet(t *testing.T) {
	ctx := context.Background()
	es, err := NewESPostgreSQL(dbURL, 3)
	require.NoError(t, err)

	id := uuid.New().String()
	acc := CreateAccount("Paulo", id, 100)
	acc.Deposit(10)
	acc.Deposit(20)
	err = es.Save(ctx, acc, Options{})
	acc.Deposit(5)
	err = es.Save(ctx, acc, Options{})
	require.NoError(t, err)
	time.Sleep(time.Second)

	// giving time for the snapshots to write
	time.Sleep(100 * time.Millisecond)

	count := 0
	err = es.db.Get(&count, "SELECT count(*) FROM snapshots WHERE aggregate_id = $1", id)
	require.NoError(t, err)
	require.Equal(t, 1, count)

	evts := []PgEvent{}
	err = es.db.Select(&evts, "SELECT * FROM events WHERE aggregate_id = $1", id)
	require.NoError(t, err)
	require.Equal(t, 4, len(evts))
	assert.Equal(t, "AccountCreated", evts[0].Kind)
	assert.Equal(t, "Account", evts[0].AggregateType)
	assert.Equal(t, id, evts[0].AggregateID)
	assert.Equal(t, 1, evts[0].AggregateVersion)

	acc2 := NewAccount()
	err = es.GetByID(ctx, id, acc2)
	require.NoError(t, err)
	assert.Equal(t, id, acc2.ID)
	assert.Equal(t, 4, acc2.Version)
	assert.Equal(t, int64(135), acc2.Balance)
	assert.Equal(t, OPEN, acc2.Status)
}

func TestListener(t *testing.T) {
	ctx := context.Background()
	es, err := NewESPostgreSQL(dbURL, 3)
	require.NoError(t, err)

	id := uuid.New().String()
	acc := CreateAccount("Paulo", id, 100)
	acc.Deposit(10)
	acc.Deposit(20)
	err = es.Save(ctx, acc, Options{})
	acc.Deposit(5)
	err = es.Save(ctx, acc, Options{})
	require.NoError(t, err)
	time.Sleep(time.Second)

	acc2 := NewAccount()
	counter := 0
	lm := NewPoller(es, MockLocker{}, WithStartFrom(BEGINNING))

	done := make(chan struct{})
	lm.Handle(ctx, func(ctx context.Context, e Event) {
		if e.AggregateID == id {
			acc2.ApplyChangeFromHistory(e)
			counter++
			if counter == 4 {
				close(done)
			}
		}
	})

	select {
	case <-done:
	case <-time.After(time.Second):
	}
	assert.Equal(t, 4, counter)
	assert.Equal(t, id, acc2.ID)
	assert.Equal(t, 4, acc2.Version)
	assert.Equal(t, int64(135), acc2.Balance)
	assert.Equal(t, OPEN, acc2.Status)
}

func TestListenerWithAggregateType(t *testing.T) {
	ctx := context.Background()
	es, err := NewESPostgreSQL(dbURL, 3)
	require.NoError(t, err)

	id := uuid.New().String()
	acc := CreateAccount("Paulo", id, 100)
	acc.Deposit(10)
	acc.Deposit(20)
	err = es.Save(ctx, acc, Options{})
	acc.Deposit(5)
	err = es.Save(ctx, acc, Options{})
	require.NoError(t, err)
	time.Sleep(time.Second)

	acc2 := NewAccount()
	counter := 0
	p := NewPoller(es, MockLocker{}, WithStartFrom(BEGINNING), WithAggregateTypes("Account"))

	done := make(chan struct{})
	p.Handle(ctx, func(ctx context.Context, e Event) {
		if e.AggregateID == id {
			acc2.ApplyChangeFromHistory(e)
			counter++
			if counter == 4 {
				close(done)
			}
		}
	})

	select {
	case <-done:
	case <-time.After(time.Second):
	}
	assert.Equal(t, 4, counter)
	assert.Equal(t, id, acc2.ID)
	assert.Equal(t, 4, acc2.Version)
	assert.Equal(t, int64(135), acc2.Balance)
	assert.Equal(t, OPEN, acc2.Status)
}

func TestListenerWithLabels(t *testing.T) {
	ctx := context.Background()
	es, err := NewESPostgreSQL(dbURL, 3)
	require.NoError(t, err)

	id := uuid.New().String()
	acc := CreateAccount("Paulo", id, 100)
	acc.Deposit(10)
	acc.Deposit(20)
	err = es.Save(ctx, acc, Options{
		Labels: map[string]string{
			"geo": "EU",
		},
	})
	acc.Deposit(5)
	err = es.Save(ctx, acc, Options{
		Labels: map[string]string{
			"geo": "US",
		},
	})
	require.NoError(t, err)
	time.Sleep(time.Second)

	acc2 := NewAccount()
	counter := 0
	p := NewPoller(es, MockLocker{}, WithStartFrom(BEGINNING), WithLabels(NewLabel("geo", "EU")))

	done := make(chan struct{})
	p.Handle(ctx, func(ctx context.Context, e Event) {
		if e.AggregateID == id {
			acc2.ApplyChangeFromHistory(e)
			counter++
			if counter == 3 {
				close(done)
			}
		}
	})

	select {
	case <-done:
	case <-time.After(time.Second):
	}
	assert.Equal(t, 3, counter)
	assert.Equal(t, id, acc2.ID)
	assert.Equal(t, 3, acc2.Version)
	assert.Equal(t, int64(130), acc2.Balance)
	assert.Equal(t, OPEN, acc2.Status)
}

func TestForget(t *testing.T) {
	ctx := context.Background()
	es, err := NewESPostgreSQL(dbURL, 3)
	require.NoError(t, err)

	id := uuid.New().String()
	acc := CreateAccount("Paulo", id, 100)
	acc.UpdateOwner("Paulo Quintans")
	acc.Deposit(10)
	acc.Deposit(20)
	err = es.Save(ctx, acc, Options{})
	acc.Deposit(5)
	acc.Withdraw(15)
	acc.UpdateOwner("Paulo Quintans Pereira")
	err = es.Save(ctx, acc, Options{})
	require.NoError(t, err)

	// giving time for the snapshots to write
	time.Sleep(100 * time.Millisecond)

	evts := []Json{}
	err = es.db.Select(&evts, "SELECT body FROM events WHERE aggregate_id = $1 and kind = 'OwnerUpdated'", id)
	require.NoError(t, err)
	assert.Equal(t, 2, len(evts))
	for _, v := range evts {
		ou := &OwnerUpdated{}
		err = json.Unmarshal(v, ou)
		require.NoError(t, err)
		assert.NotEmpty(t, ou.Owner)
	}

	bodies := []Json{}
	err = es.db.Select(&bodies, "SELECT body FROM snapshots WHERE aggregate_id = $1", id)
	require.NoError(t, err)
	assert.Equal(t, 2, len(bodies))
	for _, v := range bodies {
		a := NewAccount()
		err = json.Unmarshal(v, a)
		require.NoError(t, err)
		assert.NotEmpty(t, a.Owner)
	}

	err = es.Forget(ctx, ForgetRequest{
		AggregateID: id,
		Events: []EventKind{
			{
				Kind:   "OwnerUpdated",
				Fields: []string{"owner"},
			},
		},
		AggregateFields: []string{"owner"},
	})
	require.NoError(t, err)

	evts = []Json{}
	err = es.db.Select(&evts, "SELECT body FROM events WHERE aggregate_id = $1 and kind = 'OwnerUpdated'", id)
	require.NoError(t, err)
	assert.Equal(t, 2, len(evts))
	for _, v := range evts {
		ou := &OwnerUpdated{}
		err = json.Unmarshal(v, ou)
		require.NoError(t, err)
		assert.Empty(t, ou.Owner)
	}

	bodies = []Json{}
	err = es.db.Select(&bodies, "SELECT body FROM snapshots WHERE aggregate_id = $1", id)
	require.NoError(t, err)
	assert.Equal(t, 2, len(bodies))
	for _, v := range bodies {
		a := NewAccount()
		err = json.Unmarshal(v, a)
		require.NoError(t, err)
		assert.Empty(t, a.Owner)
	}
}

func BenchmarkDepositAndSave2(b *testing.B) {
	es, _ := NewESPostgreSQL(dbURL, 3)
	b.RunParallel(func(pb *testing.PB) {
		ctx := context.Background()
		id := uuid.New().String()
		acc := CreateAccount("Paulo", id, 0)

		for pb.Next() {
			acc.Deposit(10)
			_ = es.Save(ctx, acc, Options{})
		}
	})
}
