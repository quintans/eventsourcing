package pg

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/quintans/eventsourcing"
	"github.com/quintans/eventsourcing/encoding"
	"github.com/quintans/eventsourcing/log"
	"github.com/quintans/eventsourcing/player"
	"github.com/quintans/eventsourcing/store/poller"
	"github.com/quintans/eventsourcing/store/postgresql"
	"github.com/quintans/eventsourcing/test"
)

const (
	aggregateType eventsourcing.AggregateType = "Account"
)

var logger = log.NewLogrus(logrus.StandardLogger())

func connect(dbConfig DBConfig) (*sqlx.DB, error) {
	dburl := fmt.Sprintf("postgres://%s:%s@%s:%d/%s?sslmode=disable", dbConfig.Username, dbConfig.Password, dbConfig.Host, dbConfig.Port, dbConfig.Database)

	db, err := sqlx.Open("postgres", dburl)
	if err != nil {
		return nil, err
	}
	if err := db.Ping(); err != nil {
		return nil, err
	}
	return db, nil
}

func TestSaveAndGet(t *testing.T) {
	dbConfig, tearDown, err := setup()
	require.NoError(t, err)
	defer tearDown()

	ctx := context.Background()
	r, err := postgresql.NewStore(dbConfig.Url())
	require.NoError(t, err)
	es := eventsourcing.NewEventStore(r, test.AggregateFactory{}, eventsourcing.WithSnapshotThreshold(3))

	id := uuid.New()
	acc := test.CreateAccount("Paulo", id, 100)
	acc.Deposit(10)
	acc.Deposit(20)
	err = es.Save(ctx, acc)
	require.NoError(t, err)
	acc.Deposit(5)
	err = es.Save(ctx, acc, eventsourcing.WithIdempotencyKey("idempotency-key"))
	require.NoError(t, err)
	time.Sleep(time.Second)

	// giving time for the snapshots to write
	time.Sleep(100 * time.Millisecond)

	db, err := connect(dbConfig)
	require.NoError(t, err)
	count := 0
	err = db.Get(&count, "SELECT count(*) FROM snapshots WHERE aggregate_id = $1", id.String())
	require.NoError(t, err)
	require.Equal(t, 1, count)

	evts := []postgresql.Event{}
	err = db.Select(&evts, "SELECT * FROM events WHERE aggregate_id = $1 ORDER by id ASC", id.String())
	require.NoError(t, err)
	require.Equal(t, 4, len(evts))
	assert.Equal(t, "AccountCreated", evts[0].Kind.String())
	assert.Equal(t, "MoneyDeposited", evts[1].Kind.String())
	assert.Equal(t, "MoneyDeposited", evts[2].Kind.String())
	assert.Equal(t, "MoneyDeposited", evts[3].Kind.String())
	assert.Equal(t, "idempotency-key", string(evts[3].IdempotencyKey))
	assert.Equal(t, aggregateType, evts[0].AggregateType)
	assert.Equal(t, id.String(), evts[0].AggregateID)
	assert.Equal(t, uint32(1), evts[0].AggregateVersion)

	a, err := es.GetByID(ctx, id.String())
	require.NoError(t, err)
	acc2 := a.(*test.Account)
	assert.Equal(t, id, acc2.ID)
	assert.Equal(t, uint32(4), acc2.GetVersion())
	assert.Equal(t, uint32(1), acc2.GetEventsCounter())
	assert.Equal(t, int64(135), acc2.Balance)
	assert.Equal(t, test.OPEN, acc2.Status)

	found, err := es.HasIdempotencyKey(ctx, "idempotency-key")
	require.NoError(t, err)
	require.True(t, found)

	acc.Deposit(5)
	err = es.Save(ctx, acc, eventsourcing.WithIdempotencyKey("idempotency-key"))
	require.Error(t, err)
}

func TestPollListener(t *testing.T) {
	dbConfig, tearDown, err := setup()
	require.NoError(t, err)
	defer tearDown()

	ctx := context.Background()
	r, err := postgresql.NewStore(dbConfig.Url())
	require.NoError(t, err)
	es := eventsourcing.NewEventStore(r, test.AggregateFactory{}, eventsourcing.WithSnapshotThreshold(3))

	id := uuid.New()
	acc := test.CreateAccount("Paulo", id, 100)
	acc.Deposit(10)
	acc.Deposit(20)
	err = es.Save(ctx, acc)
	require.NoError(t, err)
	acc.Deposit(5)
	err = es.Save(ctx, acc)
	require.NoError(t, err)
	time.Sleep(time.Second)

	acc2 := test.NewAccount()
	counter := 0
	repository, err := postgresql.NewStore(dbConfig.Url())
	require.NoError(t, err)
	p := poller.New(logger, repository)

	ctx, cancel := context.WithCancel(ctx)
	var mu sync.Mutex
	go p.Poll(ctx, player.StartBeginning(), func(ctx context.Context, e eventsourcing.Event) error {
		if e.AggregateID == id.String() {
			if err := es.ApplyChangeFromHistory(acc2, e); err != nil {
				return err
			}
			mu.Lock()
			counter++
			mu.Unlock()
		}
		return nil
	})

	time.Sleep(time.Second)
	cancel()
	time.Sleep(100 * time.Millisecond)

	assert.Equal(t, 4, counter)
	assert.Equal(t, id, acc2.ID)
	assert.Equal(t, uint32(4), acc2.GetVersion())
	assert.Equal(t, int64(135), acc2.Balance)
	assert.Equal(t, test.OPEN, acc2.Status)
}

func TestListenerWithAggregateType(t *testing.T) {
	dbConfig, tearDown, err := setup()
	require.NoError(t, err)
	defer tearDown()

	ctx := context.Background()
	r, err := postgresql.NewStore(dbConfig.Url())
	require.NoError(t, err)
	es := eventsourcing.NewEventStore(r, test.AggregateFactory{}, eventsourcing.WithSnapshotThreshold(3))

	id := uuid.New()
	acc := test.CreateAccount("Paulo", id, 100)
	acc.Deposit(10)
	acc.Deposit(20)
	err = es.Save(ctx, acc)
	require.NoError(t, err)
	acc.Deposit(5)
	err = es.Save(ctx, acc)
	require.NoError(t, err)
	time.Sleep(time.Second)

	acc2 := test.NewAccount()
	counter := 0
	repository, err := postgresql.NewStore(dbConfig.Url())
	require.NoError(t, err)
	p := poller.New(logger, repository, poller.WithAggregateTypes(aggregateType))

	ctx, cancel := context.WithCancel(ctx)
	var mu sync.Mutex
	go p.Poll(ctx, player.StartBeginning(), func(ctx context.Context, e eventsourcing.Event) error {
		if e.AggregateID == id.String() {
			if err := es.ApplyChangeFromHistory(acc2, e); err != nil {
				return err
			}
			mu.Lock()
			counter++
			mu.Unlock()
		}
		return nil
	})

	time.Sleep(time.Second)
	cancel()
	time.Sleep(100 * time.Millisecond)

	assert.Equal(t, 4, counter)
	assert.Equal(t, id, acc2.ID)
	assert.Equal(t, uint32(4), acc2.GetVersion())
	assert.Equal(t, int64(135), acc2.Balance)
	assert.Equal(t, test.OPEN, acc2.Status)
}

func TestListenerWithLabels(t *testing.T) {
	dbConfig, tearDown, err := setup()
	require.NoError(t, err)
	defer tearDown()

	ctx := context.Background()
	r, err := postgresql.NewStore(dbConfig.Url())
	require.NoError(t, err)
	es := eventsourcing.NewEventStore(r, test.AggregateFactory{}, eventsourcing.WithSnapshotThreshold(3))

	id := uuid.New()
	acc := test.CreateAccount("Paulo", id, 100)
	acc.Deposit(10)
	acc.Deposit(20)
	err = es.Save(ctx, acc, eventsourcing.WithMetadata(map[string]interface{}{"geo": "EU"}))
	require.NoError(t, err)
	acc.Deposit(5)
	err = es.Save(ctx, acc, eventsourcing.WithMetadata(map[string]interface{}{"geo": "US"}))
	require.NoError(t, err)
	time.Sleep(time.Second)

	acc2 := test.NewAccount()
	counter := 0

	repository, err := postgresql.NewStore(dbConfig.Url())
	require.NoError(t, err)
	p := poller.New(logger, repository, poller.WithMetadataKV("geo", "EU"))

	ctx, cancel := context.WithCancel(ctx)
	var mu sync.Mutex
	go p.Poll(ctx, player.StartBeginning(), func(ctx context.Context, e eventsourcing.Event) error {
		if e.AggregateID == id.String() {
			if err := es.ApplyChangeFromHistory(acc2, e); err != nil {
				return err
			}
			mu.Lock()
			counter++
			mu.Unlock()
		}
		return nil
	})

	time.Sleep(time.Second)
	cancel()
	time.Sleep(100 * time.Millisecond)

	mu.Lock()
	assert.Equal(t, 3, counter)
	mu.Unlock()
	assert.Equal(t, id, acc2.ID)
	assert.Equal(t, uint32(3), acc2.GetVersion())
	assert.Equal(t, int64(130), acc2.Balance)
	assert.Equal(t, test.OPEN, acc2.Status)
}

func TestForget(t *testing.T) {
	dbConfig, tearDown, err := setup()
	require.NoError(t, err)
	defer tearDown()

	ctx := context.Background()
	r, err := postgresql.NewStore(dbConfig.Url())
	require.NoError(t, err)
	es := eventsourcing.NewEventStore(r, test.AggregateFactory{}, eventsourcing.WithSnapshotThreshold(3))

	id := uuid.New()
	acc := test.CreateAccount("Paulo", id, 100)
	acc.UpdateOwner("Paulo Quintans")
	acc.Deposit(10)
	acc.Deposit(20)
	err = es.Save(ctx, acc)
	require.NoError(t, err)
	acc.Deposit(5)
	acc.Withdraw(15)
	acc.UpdateOwner("Paulo Quintans Pereira")
	err = es.Save(ctx, acc)
	require.NoError(t, err)

	// giving time for the snapshots to write
	time.Sleep(100 * time.Millisecond)

	db, err := connect(dbConfig)
	require.NoError(t, err)
	evts := []encoding.Json{}
	err = db.Select(&evts, "SELECT body FROM events WHERE aggregate_id = $1 and kind = 'OwnerUpdated'", id.String())
	require.NoError(t, err)
	assert.Equal(t, 2, len(evts))
	for _, v := range evts {
		ou := &test.OwnerUpdated{}
		err = json.Unmarshal(v, ou)
		require.NoError(t, err)
		assert.NotEmpty(t, ou.Owner)
	}

	bodies := []encoding.Json{}
	err = db.Select(&bodies, "SELECT body FROM snapshots WHERE aggregate_id = $1", id.String())
	require.NoError(t, err)
	assert.Equal(t, 2, len(bodies))
	for _, v := range bodies {
		a := test.NewAccount()
		err = json.Unmarshal(v, a)
		require.NoError(t, err)
		assert.NotEmpty(t, a.Owner)
	}

	err = es.Forget(ctx,
		eventsourcing.ForgetRequest{
			AggregateID: id.String(),
			EventKind:   "OwnerUpdated",
		},
		func(i interface{}) interface{} {
			switch t := i.(type) {
			case test.OwnerUpdated:
				t.Owner = ""
				return t
			case test.Account:
				t.Owner = ""
				return t
			}
			return i
		},
	)
	require.NoError(t, err)

	evts = []encoding.Json{}
	err = db.Select(&evts, "SELECT body FROM events WHERE aggregate_id = $1 and kind = 'OwnerUpdated'", id.String())
	require.NoError(t, err)
	assert.Equal(t, 2, len(evts))
	for _, v := range evts {
		ou := &test.OwnerUpdated{}
		err = json.Unmarshal(v, ou)
		require.NoError(t, err)
		assert.Empty(t, ou.Owner)
	}

	bodies = []encoding.Json{}
	err = db.Select(&bodies, "SELECT body FROM snapshots WHERE aggregate_id = $1", id.String())
	require.NoError(t, err)
	assert.Equal(t, 2, len(bodies))
	for _, v := range bodies {
		a := test.NewAccount()
		err = json.Unmarshal(v, a)
		require.NoError(t, err)
		assert.Empty(t, a.Owner)
		assert.NotEmpty(t, a.ID)
	}
}

func BenchmarkDepositAndSave2(b *testing.B) {
	dbConfig, tearDown, err := setup()
	require.NoError(b, err)
	defer tearDown()

	r, _ := postgresql.NewStore(dbConfig.Url())
	es := eventsourcing.NewEventStore(r, test.AggregateFactory{}, eventsourcing.WithSnapshotThreshold(50))
	b.RunParallel(func(pb *testing.PB) {
		ctx := context.Background()
		id := uuid.New()
		acc := test.CreateAccount("Paulo", id, 0)

		for pb.Next() {
			acc.Deposit(10)
			_ = es.Save(ctx, acc)
		}
	})
}

func TestMigrationSimple(t *testing.T) {
	dbConfig, tearDown, err := setup()
	require.NoError(t, err)
	defer tearDown()

	ctx := context.Background()
	r, err := postgresql.NewStore(dbConfig.Url())
	require.NoError(t, err)
	es := eventsourcing.NewEventStore(r, test.AggregateFactory{}, eventsourcing.WithSnapshotThreshold(3))

	id := uuid.MustParse("cd67a139-521f-479e-ad94-431e4b23226f")
	acc := test.CreateAccount("Paulo Pereira", id, 100)
	acc.Deposit(20)
	acc.Withdraw(15)
	acc.UpdateOwner("Paulo Quintans Pereira")
	err = es.Save(ctx, acc)
	require.NoError(t, err)

	// giving time for the snapshots to write
	time.Sleep(100 * time.Millisecond)

	// switching the aggregator factory
	es = eventsourcing.NewEventStore(r, test.AggregateFactoryV2{}, eventsourcing.WithSnapshotThreshold(3))
	err = r.Migrate(ctx,
		1,
		3,
		func() eventsourcing.Aggregater {
			return test.NewAccountV2()
		},
		es.ApplyChangeFromHistory,
		eventsourcing.JSONCodec{},
		func(events []*postgresql.Event) ([]*postgresql.EventMigration, error) {
			var migration []*postgresql.EventMigration
			var m *postgresql.EventMigration
			// default codec used by the event store
			codec := eventsourcing.JSONCodec{}
			for _, e := range events {
				var err error
				switch e.Kind {
				case "AccountCreated":
					m, err = migrateAccountCreated(e, codec)
				case "OwnerUpdated":
					m, err = migrateOwnerUpdated(e, codec)
				default:
					m = postgresql.DefaultEventMigration(e)
				}
				if err != nil {
					return nil, err
				}
				migration = append(migration, m)
			}
			return migration, nil
		},
		"Account",
		"AccountCreated",
		"OwnerUpdated",
	)
	require.NoError(t, err)

	db, err := connect(dbConfig)
	require.NoError(t, err)
	evts := []postgresql.Event{}
	err = db.Select(&evts, "SELECT * FROM events WHERE aggregate_id = $1 ORDER by id ASC", id.String())
	require.NoError(t, err)
	require.Equal(t, 9, len(evts))

	evt := evts[0]
	assert.Equal(t, "AccountCreated", evt.Kind.String())
	assert.Equal(t, 1, int(evt.AggregateVersion))
	assert.Equal(t, `{"id":"cd67a139-521f-479e-ad94-431e4b23226f","money":100,"owner":"Paulo Pereira"}`, string(evt.Body))
	assert.Equal(t, 1, evt.Migrated)

	evt = evts[1]
	assert.Equal(t, "MoneyDeposited", evt.Kind.String())
	assert.Equal(t, 2, int(evt.AggregateVersion))
	assert.Equal(t, 1, evt.Migrated)

	evt = evts[2]
	assert.Equal(t, "MoneyWithdrawn", evt.Kind.String())
	assert.Equal(t, 3, int(evt.AggregateVersion))
	assert.Equal(t, 1, evt.Migrated)

	evt = evts[3]
	assert.Equal(t, "OwnerUpdated", evt.Kind.String())
	assert.Equal(t, 4, int(evt.AggregateVersion))
	assert.Equal(t, `{"owner":"Paulo Quintans Pereira"}`, string(evt.Body))
	assert.Equal(t, 1, evt.Migrated)

	evt = evts[4]
	assert.Equal(t, "Invalidated", evt.Kind.String())
	assert.Equal(t, 5, int(evt.AggregateVersion))
	assert.Equal(t, 0, len(evt.Body))
	assert.Equal(t, 1, evt.Migrated)

	evt = evts[5]
	assert.Equal(t, "AccountCreated_V2", evt.Kind.String())
	assert.Equal(t, 6, int(evt.AggregateVersion))
	assert.Equal(t, `{"id":"cd67a139-521f-479e-ad94-431e4b23226f","money":100,"first_name":"Paulo","last_name":"Pereira"}`, string(evt.Body))
	assert.Equal(t, 0, evt.Migrated)

	evt = evts[6]
	assert.Equal(t, "MoneyDeposited", evt.Kind.String())
	assert.Equal(t, 7, int(evt.AggregateVersion))
	assert.Equal(t, 0, evt.Migrated)

	evt = evts[7]
	assert.Equal(t, "MoneyWithdrawn", evt.Kind.String())
	assert.Equal(t, 8, int(evt.AggregateVersion))
	assert.Equal(t, 0, evt.Migrated)

	evt = evts[8]
	assert.Equal(t, "OwnerUpdated_V2", evt.Kind.String())
	assert.Equal(t, 9, int(evt.AggregateVersion))
	assert.Equal(t, `{"first_name":"Paulo","last_name":"Quintans Pereira"}`, string(evt.Body))
	assert.Equal(t, 0, evt.Migrated)

	a, err := es.GetByID(ctx, id.String())
	require.NoError(t, err)
	acc2 := a.(*test.AccountV2)
	assert.Equal(t, uint32(9), acc2.GetVersion())
	assert.Equal(t, "Paulo", acc2.FirstName)
	assert.Equal(t, "Quintans Pereira", acc2.LastName)
}

func migrateAccountCreated(e *postgresql.Event, codec eventsourcing.Codec) (*postgresql.EventMigration, error) {
	oldEvent := test.AccountCreated{}
	err := codec.Decode(e.Body, &oldEvent)
	if err != nil {
		return nil, err
	}
	first, last := splitName(oldEvent.Owner)
	newEvent := test.AccountCreatedV2{
		ID:        oldEvent.ID,
		Money:     oldEvent.Money,
		FirstName: first,
		LastName:  last,
	}
	body, err := codec.Encode(newEvent)
	if err != nil {
		return nil, err
	}

	m := postgresql.DefaultEventMigration(e)
	m.Kind = "AccountCreated_V2"
	m.Body = body

	return m, nil
}

func migrateOwnerUpdated(e *postgresql.Event, codec eventsourcing.Codec) (*postgresql.EventMigration, error) {
	oldEvent := test.OwnerUpdated{}
	err := codec.Decode(e.Body, &oldEvent)
	if err != nil {
		return nil, err
	}
	first, last := splitName(oldEvent.Owner)
	newEvent := test.OwnerUpdatedV2{
		FirstName: first,
		LastName:  last,
	}
	body, err := codec.Encode(newEvent)
	if err != nil {
		return nil, err
	}

	m := postgresql.DefaultEventMigration(e)
	m.Kind = "OwnerUpdated_V2"
	m.Body = body

	return m, nil
}

func splitName(name string) (string, string) {
	name = strings.TrimSpace(name)
	names := strings.Split(name, " ")
	half := len(names) / 2
	var first, last string
	if half > 0 {
		first = strings.Join(names[:half], " ")
		last = strings.Join(names[half:], " ")
	} else {
		first = names[0]
	}
	return first, last
}
