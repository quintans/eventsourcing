//go:build mysql

package mysql

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/quintans/eventsourcing"
	"github.com/quintans/eventsourcing/eventid"
	"github.com/quintans/eventsourcing/sink/poller"
	"github.com/quintans/eventsourcing/store"
	"github.com/quintans/eventsourcing/store/mysql"
	"github.com/quintans/eventsourcing/test"
	"github.com/quintans/eventsourcing/util/ids"
)

var (
	logger    = slog.New(slog.NewTextHandler(os.Stdout, nil))
	esOptions = eventsourcing.EsSnapshotThreshold(3)
)

type Snapshot struct {
	ID               eventid.EventID    `db:"id,omitempty"`
	AggregateID      string             `db:"aggregate_id,omitempty"`
	AggregateVersion uint32             `db:"aggregate_version,omitempty"`
	AggregateKind    eventsourcing.Kind `db:"aggregate_kind,omitempty"`
	Body             []byte             `db:"body,omitempty"`
	CreatedAt        time.Time          `db:"created_at,omitempty"`
	DiscTenant       store.NilString    `db:"disc_tenant,omitempty"`
}

type Event struct {
	ID               eventid.EventID    `db:"id"`
	AggregateID      string             `db:"aggregate_id"`
	AggregateIDHash  int32              `db:"aggregate_id_hash"`
	AggregateVersion uint32             `db:"aggregate_version"`
	AggregateKind    eventsourcing.Kind `db:"aggregate_kind"`
	Kind             eventsourcing.Kind `db:"kind"`
	Body             []byte             `db:"body"`
	CreatedAt        time.Time          `db:"created_at"`
	Migration        int                `db:"migration"`
	Migrated         bool               `db:"migrated"`
	DiscTenant       store.NilString    `db:"disc_tenant,omitempty"`
}

func connect(t *testing.T, dbConfig DBConfig) *sqlx.DB {
	dburl := dbConfig.URL()

	db, err := sqlx.Open("mysql", dburl)
	require.NoError(t, err)

	err = db.Ping()
	require.NoError(t, err)

	return db
}

var dbConfig DBConfig

func TestMain(m *testing.M) {
	dbConfig = Setup()

	// run the tests
	code := m.Run()

	// exit with the code from the tests
	os.Exit(code)
}

func TestSaveAndGet(t *testing.T) {
	t.Parallel()

	ctx := t.Context()

	eventsTable := test.RandStr("events")
	snapshotsTable := test.RandStr("snapshots")
	r, err := mysql.NewStoreWithURL[ids.AggID](
		dbConfig.URL(),
		mysql.WithEventsTable[ids.AggID](eventsTable),
		mysql.WithSnapshotsTable[ids.AggID](snapshotsTable),
	)
	require.NoError(t, err)
	es, err := eventsourcing.NewEventStore[*test.Account](r, test.NewJSONCodec(), esOptions)
	require.NoError(t, err)

	acc, err := es.Retrieve(ctx, ids.New())
	require.ErrorIs(t, err, eventsourcing.ErrUnknownAggregateID)

	acc, err = test.NewAccount("Paulo", 100)
	require.NoError(t, err)
	id := acc.GetID()
	acc.Deposit(10)
	acc.Deposit(20)
	err = es.Create(ctx, acc)
	require.NoError(t, err)
	err = es.Update(
		ctx,
		id,
		func(acc *test.Account) (*test.Account, error) {
			acc.Deposit(5)
			acc.Deposit(1)
			return acc, nil
		},
	)
	require.NoError(t, err)
	time.Sleep(time.Second)

	// giving time for the snapshots to write
	time.Sleep(100 * time.Millisecond)

	db := connect(t, dbConfig)

	snaps := []Snapshot{}
	err = db.Select(&snaps, fmt.Sprintf("SELECT * FROM %s WHERE aggregate_id = ? ORDER by id ASC", snapshotsTable), id.String())
	require.NoError(t, err)
	require.Equal(t, 1, len(snaps))

	snap := snaps[0]
	assert.Equal(t, "Account", snap.AggregateKind.String())
	assert.Equal(t, 3, int(snap.AggregateVersion))
	assert.Equal(t, `{"status":"OPEN","balance":130,"owner":"Paulo"}`, string(snap.Body))
	assert.Equal(t, id.String(), snap.AggregateID)

	evts := []Event{}
	err = db.Select(&evts, fmt.Sprintf("SELECT * FROM %s WHERE aggregate_id = ? ORDER by id ASC", eventsTable), id.String())
	require.NoError(t, err)
	require.Equal(t, 5, len(evts))
	assert.Equal(t, "AccountCreated", evts[0].Kind.String())
	assert.Equal(t, "MoneyDeposited", evts[1].Kind.String())
	assert.Equal(t, "MoneyDeposited", evts[2].Kind.String())
	assert.Equal(t, "MoneyDeposited", evts[3].Kind.String())
	assert.Equal(t, test.KindAccount, evts[0].AggregateKind)
	assert.Equal(t, id.String(), evts[0].AggregateID)
	for i := 0; i < len(evts); i++ {
		assert.Equal(t, uint32(i+1), evts[i].AggregateVersion)
	}

	acc2, err := es.Retrieve(ctx, id)
	require.NoError(t, err)
	assert.Equal(t, id, acc2.GetID())
	assert.Equal(t, int64(136), acc2.Balance())
	assert.Equal(t, test.OPEN, acc2.Status())
}

func TestPollListener(t *testing.T) {
	t.Parallel()

	ctx := t.Context()

	db := connect(t, dbConfig)
	eventsTable := test.RandStr("events")
	snapshotsTable := test.RandStr("snapshots")
	outboxTable := test.RandStr("outbox")
	r, err := mysql.NewStore(
		db.DB,
		mysql.WithEventsTable[ids.AggID](eventsTable),
		mysql.WithSnapshotsTable[ids.AggID](snapshotsTable),
		mysql.WithTxHandler(mysql.OutboxInsertHandler[ids.AggID](outboxTable)),
	)
	require.NoError(t, err)

	outboxRepo, err := mysql.NewOutboxStore(db.DB, outboxTable, r)
	require.NoError(t, err)

	es, err := eventsourcing.NewEventStore[*test.Account](r, test.NewJSONCodec(), esOptions)
	require.NoError(t, err)

	acc, err := test.NewAccount("Paulo", 100)
	require.NoError(t, err)
	id := acc.GetID()
	acc.Deposit(10)
	acc.Deposit(20)
	err = es.Create(ctx, acc)
	require.NoError(t, err)
	err = es.Update(ctx, id, func(acc *test.Account) (*test.Account, error) {
		acc.Deposit(5)
		return acc, nil
	})
	require.NoError(t, err)
	time.Sleep(time.Second)

	acc2 := test.DehydratedAccount(id)
	counter := 0
	p := poller.New(logger, outboxRepo)

	ctx, cancel := context.WithCancel(ctx)
	var mu sync.Mutex

	mockSink := test.NewMockSink(test.NewMockSinkData[ids.AggID](), 1, 1, 1)
	mockSink.OnSink(func(ctx context.Context, e *eventsourcing.Event[ids.AggID]) error {
		if e.AggregateID == id {
			if err := es.ApplyChangeFromHistory(acc2, e); err != nil {
				return err
			}
			mu.Lock()
			counter++
			mu.Unlock()
		}
		return nil
	})

	go p.Feed(ctx, mockSink)

	time.Sleep(time.Second)
	cancel()
	time.Sleep(100 * time.Millisecond)

	mu.Lock()
	assert.Equal(t, 4, counter)
	mu.Unlock()
	assert.Equal(t, id, acc2.GetID())
	assert.Equal(t, int64(135), acc2.Balance())
	assert.Equal(t, test.OPEN, acc2.Status())
}

func TestListenerWithAggregateKind(t *testing.T) {
	t.Parallel()

	ctx := t.Context()

	db := connect(t, dbConfig)
	eventsTable := test.RandStr("events")
	snapshotsTable := test.RandStr("snapshots")
	outboxTable := test.RandStr("outbox")
	r, err := mysql.NewStore(
		db.DB,
		mysql.WithEventsTable[ids.AggID](eventsTable),
		mysql.WithSnapshotsTable[ids.AggID](snapshotsTable),
		mysql.WithTxHandler(mysql.OutboxInsertHandler[ids.AggID](outboxTable)),
	)
	require.NoError(t, err)

	outboxRepo, err := mysql.NewOutboxStore(db.DB, outboxTable, r)
	require.NoError(t, err)

	es, err := eventsourcing.NewEventStore[*test.Account](r, test.NewJSONCodec(), esOptions)
	require.NoError(t, err)

	acc, err := test.NewAccount("Paulo", 100)
	require.NoError(t, err)
	id := acc.GetID()
	acc.Deposit(10)
	acc.Deposit(20)
	err = es.Create(ctx, acc)
	require.NoError(t, err)
	err = es.Update(ctx, id, func(acc *test.Account) (*test.Account, error) {
		acc.Deposit(5)
		return acc, nil
	})
	require.NoError(t, err)
	time.Sleep(time.Second)

	acc2 := test.DehydratedAccount(id)
	counter := 0
	p := poller.New(logger, outboxRepo, poller.WithAggregateKinds[ids.AggID](test.KindAccount))

	ctx, cancel := context.WithCancel(ctx)
	var mu sync.Mutex

	mockSink := test.NewMockSink(test.NewMockSinkData[ids.AggID](), 1, 1, 1)
	mockSink.OnSink(func(ctx context.Context, e *eventsourcing.Event[ids.AggID]) error {
		if e.AggregateID == id {
			if err := es.ApplyChangeFromHistory(acc2, e); err != nil {
				return err
			}
			mu.Lock()
			counter++
			mu.Unlock()
		}
		return nil
	})

	go p.Feed(ctx, mockSink)

	time.Sleep(time.Second)
	cancel()
	time.Sleep(100 * time.Millisecond)

	mu.Lock()
	assert.Equal(t, 4, counter)
	mu.Unlock()
	assert.Equal(t, id, acc2.GetID())
	assert.Equal(t, int64(135), acc2.Balance())
	assert.Equal(t, test.OPEN, acc2.Status())
}

func TestListenerWithDiscriminator(t *testing.T) {
	t.Parallel()

	key := "tenant"

	db := connect(t, dbConfig)
	eventsTable := test.RandStr("events")
	snapshotsTable := test.RandStr("snapshots")
	outboxTable := test.RandStr("outbox")
	r, err := mysql.NewStore(
		db.DB,
		mysql.WithEventsTable[ids.AggID](eventsTable),
		mysql.WithSnapshotsTable[ids.AggID](snapshotsTable),
		mysql.WithTxHandler(mysql.OutboxInsertHandler[ids.AggID](outboxTable)),
		mysql.WithDiscriminatorHook[ids.AggID](func(c *store.DiscriminatorHookContext) eventsourcing.Discriminator {
			ctx := c.Context()
			val := ctx.Value(key).(string)
			return eventsourcing.Discriminator{key: val}
		}),
	)
	require.NoError(t, err)

	outboxRepo, err := mysql.NewOutboxStore(db.DB, outboxTable, r)
	require.NoError(t, err)

	es, err := eventsourcing.NewEventStore[*test.Account](r, test.NewJSONCodec(), esOptions)
	require.NoError(t, err)

	ctx := context.WithValue(context.Background(), key, "abc")
	acc, err := test.NewAccount("Paulo", 50)
	require.NoError(t, err)
	acc.Deposit(20)
	err = es.Create(ctx, acc)
	require.NoError(t, err)

	ctx = context.WithValue(context.Background(), key, "xyz")
	acc1, err := test.NewAccount("Pereira", 100)
	require.NoError(t, err)
	id := acc1.GetID()
	acc1.Deposit(10)
	acc1.Deposit(20)
	err = es.Create(ctx, acc1)
	require.NoError(t, err)
	err = es.Update(
		ctx,
		id,
		func(acc *test.Account) (*test.Account, error) {
			acc.Deposit(5)
			return acc, nil
		},
	)
	require.NoError(t, err)
	time.Sleep(time.Second)

	p := poller.New(logger, outboxRepo, poller.WithDiscriminatorKV[ids.AggID]("tenant", "xyz"))

	ctx, cancel := context.WithCancel(ctx)
	var mu sync.Mutex
	errCh := make(chan error, 1)

	mockSink := test.NewMockSink(test.NewMockSinkData[ids.AggID](), 1, 1, 1)
	acc2 := test.DehydratedAccount(id)
	counter := 0
	mockSink.OnSink(func(ctx context.Context, e *eventsourcing.Event[ids.AggID]) error {
		if e.AggregateID == id {
			if err := es.ApplyChangeFromHistory(acc2, e); err != nil {
				return err
			}
			mu.Lock()
			counter++
			mu.Unlock()
		}
		return nil
	})

	go func() {
		errCh <- p.Feed(ctx, mockSink)
	}()

	time.Sleep(time.Second)
	cancel()
	require.NoError(t, <-errCh, "unable to poll")

	mu.Lock()
	assert.Equal(t, 4, counter)
	mu.Unlock()
	assert.Equal(t, id, acc2.GetID())
	assert.Equal(t, int64(135), acc2.Balance())
	assert.Equal(t, test.OPEN, acc2.Status())
}

func TestForget(t *testing.T) {
	t.Parallel()

	ctx := t.Context()

	eventsTable := test.RandStr("events")
	snapshotsTable := test.RandStr("snapshots")
	r, err := mysql.NewStoreWithURL[ids.AggID](
		dbConfig.URL(),
		mysql.WithEventsTable[ids.AggID](eventsTable),
		mysql.WithSnapshotsTable[ids.AggID](snapshotsTable),
	)
	require.NoError(t, err)
	es, err := eventsourcing.NewEventStore[*test.Account](r, test.NewJSONCodec(), esOptions)
	require.NoError(t, err)

	acc, err := test.NewAccount("Paulo", 100)
	require.NoError(t, err)
	id := acc.GetID()
	acc.UpdateOwner("Paulo Quintans")
	acc.Deposit(10)
	acc.Deposit(20)
	err = es.Create(ctx, acc)
	require.NoError(t, err)
	err = es.Update(ctx, id, func(acc *test.Account) (*test.Account, error) {
		acc.Deposit(5)
		acc.Withdraw(15)
		acc.UpdateOwner("Paulo Quintans Pereira")
		return acc, nil
	})
	require.NoError(t, err)

	// giving time for the snapshots to write
	time.Sleep(100 * time.Millisecond)

	codec := test.NewJSONCodec()

	db := connect(t, dbConfig)
	require.NoError(t, err)
	evts := [][]byte{}
	err = db.Select(&evts, fmt.Sprintf("SELECT body FROM %s WHERE aggregate_id = ? and kind = 'OwnerUpdated'", eventsTable), id.String())
	require.NoError(t, err)
	assert.Equal(t, 2, len(evts))
	for _, v := range evts {
		e, er := codec.Decode(v, eventsourcing.DecoderMeta[ids.AggID]{Kind: test.KindOwnerUpdated})
		ou := e.(*test.OwnerUpdated)
		require.NoError(t, er)
		assert.NotEmpty(t, ou.Owner)
	}

	bodies := [][]byte{}
	err = db.Select(&bodies, fmt.Sprintf("SELECT body FROM %s WHERE aggregate_id = ?", snapshotsTable), id.String())
	require.NoError(t, err)
	assert.Equal(t, 2, len(bodies))
	for _, v := range bodies {
		x, er := codec.Decode(v, eventsourcing.DecoderMeta[ids.AggID]{
			Kind:        test.KindAccount,
			AggregateID: id,
		})
		a := x.(*test.Account)
		require.NoError(t, er)
		assert.NotEmpty(t, a.Owner())
	}

	err = es.Forget(ctx,
		eventsourcing.ForgetRequest[ids.AggID]{
			AggregateID: id,
			EventKind:   "OwnerUpdated",
		},
		func(i eventsourcing.Kinder) (eventsourcing.Kinder, error) {
			switch t := i.(type) {
			case *test.OwnerUpdated:
				t.Owner = ""
				return t, nil
			case *test.Account:
				t.Forget()
				return t, nil
			}
			return i, nil
		},
	)
	require.NoError(t, err)

	evts = [][]byte{}
	err = db.Select(&evts, fmt.Sprintf("SELECT body FROM %s WHERE aggregate_id = ? and kind = 'OwnerUpdated'", eventsTable), id.String())
	require.NoError(t, err)
	assert.Equal(t, 2, len(evts))
	for _, v := range evts {
		e, er := codec.Decode(v, eventsourcing.DecoderMeta[ids.AggID]{Kind: test.KindOwnerUpdated})
		ou := e.(*test.OwnerUpdated)
		require.NoError(t, er)
		assert.Empty(t, ou.Owner)
	}

	bodies = [][]byte{}
	err = db.Select(&bodies, fmt.Sprintf("SELECT body FROM %s WHERE aggregate_id = ?", snapshotsTable), id.String())
	require.NoError(t, err)
	assert.Equal(t, 2, len(bodies))
	for _, v := range bodies {
		x, err := codec.Decode(v, eventsourcing.DecoderMeta[ids.AggID]{
			Kind:        test.KindAccount,
			AggregateID: id,
		})
		a := x.(*test.Account)
		require.NoError(t, err)
		assert.Empty(t, a.Owner())
		assert.NotEmpty(t, a.GetID())
	}
}

func TestMigration(t *testing.T) {
	t.Parallel()

	ctx := t.Context()

	eventsTable := test.RandStr("events")
	snapshotsTable := test.RandStr("snapshots")
	r, err := mysql.NewStoreWithURL(
		dbConfig.URL(),
		mysql.WithEventsTable[ids.AggID](eventsTable),
		mysql.WithSnapshotsTable[ids.AggID](snapshotsTable),
	)
	require.NoError(t, err)
	es1, err := eventsourcing.NewEventStore[*test.Account](r, test.NewJSONCodec(), esOptions)
	require.NoError(t, err)

	acc, err := test.NewAccount("Paulo Pereira", 100)
	require.NoError(t, err)
	id := acc.GetID()
	acc.Deposit(20)
	acc.Withdraw(15)
	acc.UpdateOwner("Paulo Quintans Pereira")
	err = es1.Create(ctx, acc)
	require.NoError(t, err)

	// giving time for the snapshots to write
	time.Sleep(100 * time.Millisecond)

	codec := test.NewJSONCodecWithUpcaster()
	// switching the aggregator factory
	es2, err := eventsourcing.NewEventStore[*test.AccountV2](r, codec, esOptions)
	require.NoError(t, err)

	err = es2.MigrateInPlaceCopyReplace(ctx,
		1,
		3,
		func(events []*eventsourcing.Event[ids.AggID]) ([]*eventsourcing.EventMigration, error) {
			var migration []*eventsourcing.EventMigration
			var m *eventsourcing.EventMigration
			// default codec used by the event store
			for _, e := range events {
				var er error
				switch e.Kind {
				case test.KindAccountCreated:
					m, er = test.MigrateAccountCreated(e, codec)
				case test.KindOwnerUpdated:
					m, er = test.MigrateOwnerUpdated(e, codec)
				default:
					m = eventsourcing.DefaultEventMigration(e)
				}
				if er != nil {
					return nil, er
				}
				migration = append(migration, m)
			}
			return migration, nil
		},
		test.KindAccountV2,
		test.KindAccount,
		[]eventsourcing.Kind{test.KindAccountCreated, test.KindOwnerUpdated},
	)
	require.NoError(t, err)

	db := connect(t, dbConfig)
	require.NoError(t, err)

	snaps := []Snapshot{}
	err = db.Select(&snaps, fmt.Sprintf("SELECT * FROM %s WHERE aggregate_id = ? ORDER by id ASC", snapshotsTable), id.String())
	require.NoError(t, err)
	require.Equal(t, 1, len(snaps))

	snap := snaps[0]
	assert.Equal(t, "Account_V2", snap.AggregateKind.String())
	assert.Equal(t, 9, int(snap.AggregateVersion))
	assert.Equal(t, `{"status":"OPEN","balance":105,"owner":{"firstName":"Paulo","lastName":"Quintans Pereira"}}`, string(snap.Body))
	assert.Equal(t, id.String(), snap.AggregateID)

	evts := []Event{}
	err = db.Select(&evts, fmt.Sprintf("SELECT * FROM %s WHERE aggregate_id = ? ORDER by id ASC", eventsTable), id.String())
	require.NoError(t, err)
	require.Equal(t, 9, len(evts))

	evt := evts[0]
	assert.Equal(t, "AccountCreated", evt.Kind.String())
	assert.Equal(t, 1, int(evt.AggregateVersion))
	assert.Equal(t, `{"money":100,"owner":"Paulo Pereira"}`, string(evt.Body))
	assert.Equal(t, 1, evt.Migration)
	assert.False(t, evt.Migrated)

	evt = evts[1]
	assert.Equal(t, "MoneyDeposited", evt.Kind.String())
	assert.Equal(t, 2, int(evt.AggregateVersion))
	assert.Equal(t, `{"money":20}`, string(evt.Body))
	assert.Equal(t, 1, evt.Migration)
	assert.False(t, evt.Migrated)

	evt = evts[2]
	assert.Equal(t, "MoneyWithdrawn", evt.Kind.String())
	assert.Equal(t, 3, int(evt.AggregateVersion))
	assert.Equal(t, `{"money":15}`, string(evt.Body))
	assert.Equal(t, 1, evt.Migration)
	assert.False(t, evt.Migrated)

	evt = evts[3]
	assert.Equal(t, "OwnerUpdated", evt.Kind.String())
	assert.Equal(t, 4, int(evt.AggregateVersion))
	assert.Equal(t, `{"owner":"Paulo Quintans Pereira"}`, string(evt.Body))
	assert.Equal(t, 1, evt.Migration)
	assert.False(t, evt.Migrated)

	evt = evts[4]
	assert.Equal(t, "Invalidated", evt.Kind.String())
	assert.Equal(t, 5, int(evt.AggregateVersion))
	assert.Equal(t, 0, len(evt.Body))
	assert.Equal(t, 1, evt.Migration)
	assert.False(t, evt.Migrated)

	evt = evts[5]
	assert.Equal(t, "AccountCreated_V2", evt.Kind.String())
	assert.Equal(t, 6, int(evt.AggregateVersion))
	assert.Equal(t, `{"money":100,"owner":{"firstName":"Paulo","lastName":"Pereira"}}`, string(evt.Body))
	assert.Equal(t, 0, evt.Migration)
	assert.True(t, evt.Migrated)

	evt = evts[6]
	assert.Equal(t, "MoneyDeposited", evt.Kind.String())
	assert.Equal(t, 7, int(evt.AggregateVersion))
	assert.Equal(t, `{"money":20}`, string(evt.Body))
	assert.Equal(t, 0, evt.Migration)
	assert.True(t, evt.Migrated)

	evt = evts[7]
	assert.Equal(t, "MoneyWithdrawn", evt.Kind.String())
	assert.Equal(t, 8, int(evt.AggregateVersion))
	assert.Equal(t, `{"money":15}`, string(evt.Body))
	assert.Equal(t, 0, evt.Migration)
	assert.True(t, evt.Migrated)

	evt = evts[8]
	assert.Equal(t, "OwnerUpdated_V2", evt.Kind.String())
	assert.Equal(t, 9, int(evt.AggregateVersion))
	assert.Equal(t, `{"owner":{"firstName":"Paulo","lastName":"Quintans Pereira"}}`, string(evt.Body))
	assert.Equal(t, 0, evt.Migration)
	assert.True(t, evt.Migrated)

	acc2, err := es2.Retrieve(ctx, id)
	require.NoError(t, err)
	assert.Equal(t, "Paulo", acc2.Owner().FirstName())
	assert.Equal(t, "Quintans Pereira", acc2.Owner().LastName())
}
