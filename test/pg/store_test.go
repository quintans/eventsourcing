//go:build pg

package pg

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"testing"
	"time"

	_ "github.com/lib/pq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/quintans/eventsourcing"
	"github.com/quintans/eventsourcing/eventid"
	"github.com/quintans/eventsourcing/sink/poller"
	"github.com/quintans/eventsourcing/store/postgresql"
	"github.com/quintans/eventsourcing/test"
	"github.com/quintans/eventsourcing/util/ids"
)

const (
	aggregateKind eventsourcing.Kind = "Account"
)

var logger = slog.New(slog.NewTextHandler(os.Stdout, nil))

var esOptions = eventsourcing.EsSnapshotThreshold(3)

type Snapshot struct {
	ID               eventid.EventID    `db:"id,omitempty"`
	AggregateID      string             `db:"aggregate_id,omitempty"`
	AggregateVersion uint32             `db:"aggregate_version,omitempty"`
	AggregateKind    eventsourcing.Kind `db:"aggregate_kind,omitempty"`
	Body             []byte             `db:"body,omitempty"`
	CreatedAt        time.Time          `db:"created_at,omitempty"`
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
}

var dbConfig DBConfig

func TestMain(m *testing.M) {
	dbConfig = setup()

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
	r, err := postgresql.NewStoreWithURL(
		dbConfig.URL(),
		postgresql.WithEventsTable[ids.AggID](eventsTable),
		postgresql.WithSnapshotsTable[ids.AggID](snapshotsTable),
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
	err = db.Select(&snaps, fmt.Sprintf("SELECT * FROM %s WHERE aggregate_id = $1 ORDER by id ASC", snapshotsTable), id.String())
	require.NoError(t, err)
	require.Equal(t, 1, len(snaps))

	snap := snaps[0]
	assert.Equal(t, "Account", snap.AggregateKind.String())
	assert.Equal(t, 3, int(snap.AggregateVersion))
	assert.Equal(t, `{"status":"OPEN","balance":130,"owner":"Paulo"}`, string(snap.Body))
	assert.Equal(t, id.String(), snap.AggregateID)

	evts := []Event{}
	err = db.Select(&evts, fmt.Sprintf("SELECT * FROM %s WHERE aggregate_id = $1 ORDER by id ASC", eventsTable), id.String())
	require.NoError(t, err)
	require.Equal(t, 5, len(evts))
	assert.Equal(t, "AccountCreated", evts[0].Kind.String())
	assert.Equal(t, "MoneyDeposited", evts[1].Kind.String())
	assert.Equal(t, "MoneyDeposited", evts[2].Kind.String())
	assert.Equal(t, "MoneyDeposited", evts[3].Kind.String())
	assert.Equal(t, aggregateKind, evts[0].AggregateKind)
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
	r, err := postgresql.NewStore(
		db.DB,
		postgresql.WithEventsTable[ids.AggID](eventsTable),
		postgresql.WithSnapshotsTable[ids.AggID](snapshotsTable),
		postgresql.WithOutbox[ids.AggID](outboxTable),
	)
	require.NoError(t, err)

	outboxRepo, err := postgresql.NewOutboxStore(db.DB, outboxTable, r)
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
	r, err := postgresql.NewStore(
		db.DB,
		postgresql.WithEventsTable[ids.AggID](eventsTable),
		postgresql.WithSnapshotsTable[ids.AggID](snapshotsTable),
		postgresql.WithOutbox[ids.AggID](outboxTable),
	)
	require.NoError(t, err)

	outboxRepo, err := postgresql.NewOutboxStore(db.DB, outboxTable, r)
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
	err = es.Update(ctx, id, func(a *test.Account) (*test.Account, error) {
		a.Deposit(5)
		return a, nil
	})
	require.NoError(t, err)
	time.Sleep(time.Second)

	acc2 := test.DehydratedAccount(id)
	counter := 0
	p := poller.New(logger, outboxRepo, poller.WithAggregateKinds[ids.AggID](aggregateKind))

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

	db := connect(t, dbConfig)
	key := "tenant"
	eventsTable := test.RandStr("events")
	snapshotsTable := test.RandStr("snapshots")
	outboxTable := test.RandStr("outbox")
	r, err := postgresql.NewStore(
		db.DB,
		postgresql.WithEventsTable[ids.AggID](eventsTable),
		postgresql.WithSnapshotsTable[ids.AggID](snapshotsTable),
		postgresql.WithOutbox[ids.AggID](outboxTable),
		postgresql.WithDiscriminatorKeys[ids.AggID](key),
	)
	require.NoError(t, err)

	outboxRepo, err := postgresql.NewOutboxStore(db.DB, outboxTable, r)
	require.NoError(t, err)

	es, err := eventsourcing.NewEventStore[*test.Account](r, test.NewJSONCodec(), esOptions)
	require.NoError(t, err)

	ctx := eventsourcing.SetCtxDiscriminator(context.Background(), eventsourcing.Discriminator{key: "abc"})

	acc, err := test.NewAccount("Paulo", 50)
	require.NoError(t, err)
	acc.Deposit(20)
	err = es.Create(ctx, acc)
	require.NoError(t, err)

	ctx = eventsourcing.SetCtxDiscriminator(context.Background(), eventsourcing.Discriminator{key: "xyz"})

	acc1, err := test.NewAccount("Pereira", 100)
	require.NoError(t, err)
	id := acc1.GetID()
	acc1.Deposit(10)
	acc1.Deposit(20)
	err = es.Create(ctx, acc1)
	err = es.Create(ctx, acc1)
	err = es.Create(ctx, acc1)
	require.NoError(t, err)
	err = es.Update(
		ctx,
		id,
		func(a *test.Account) (*test.Account, error) {
			a.Deposit(5)
			return a, nil
		},
	)
	require.NoError(t, err)
	time.Sleep(time.Second)

	p := poller.New(logger, outboxRepo, poller.WithDiscriminatorKV[ids.AggID](key, "xyz"))

	ctx, cancel := context.WithCancel(ctx)
	var mu sync.Mutex

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

func TestForget(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	eventsTable := test.RandStr("events")
	snapshotsTable := test.RandStr("snapshots")
	r, err := postgresql.NewStoreWithURL(
		dbConfig.URL(),
		postgresql.WithEventsTable[ids.AggID](eventsTable),
		postgresql.WithSnapshotsTable[ids.AggID](snapshotsTable),
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
	err = es.Update(ctx, id, func(a *test.Account) (*test.Account, error) {
		a.Deposit(5)
		a.Withdraw(15)
		a.UpdateOwner("Paulo Quintans Pereira")
		return a, nil
	})
	require.NoError(t, err)

	// giving time for the snapshots to write
	time.Sleep(100 * time.Millisecond)

	codec := test.NewJSONCodec()

	db := connect(t, dbConfig)
	require.NoError(t, err)
	evts := [][]byte{}
	err = db.Select(&evts, fmt.Sprintf("SELECT body FROM %s WHERE aggregate_id = $1 and kind = 'OwnerUpdated'", eventsTable), id.String())
	require.NoError(t, err)
	assert.Equal(t, 2, len(evts))
	for _, v := range evts {
		e, er := codec.Decode(v, eventsourcing.DecoderMeta[ids.AggID]{
			Kind: test.KindOwnerUpdated,
		})
		require.NoError(t, er)
		ou := e.(*test.OwnerUpdated)
		assert.NotEmpty(t, ou.Owner)
	}

	bodies := [][]byte{}
	err = db.Select(&bodies, fmt.Sprintf("SELECT body FROM %s WHERE aggregate_id = $1", snapshotsTable), id.String())
	require.NoError(t, err)
	assert.Equal(t, 2, len(bodies))
	for _, v := range bodies {
		x, er := codec.Decode(v, eventsourcing.DecoderMeta[ids.AggID]{
			Kind:        test.KindAccount,
			AggregateID: id,
		})
		require.NoError(t, er)
		a := x.(*test.Account)
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
	err = db.Select(&evts, fmt.Sprintf("SELECT body FROM %s WHERE aggregate_id = $1 and kind = 'OwnerUpdated'", eventsTable), id.String())
	require.NoError(t, err)
	assert.Equal(t, 2, len(evts))
	for _, v := range evts {
		e, er := codec.Decode(v, eventsourcing.DecoderMeta[ids.AggID]{Kind: test.KindOwnerUpdated})
		require.NoError(t, er)
		ou := e.(*test.OwnerUpdated)
		assert.Empty(t, ou.Owner)
	}

	bodies = [][]byte{}
	err = db.Select(&bodies, fmt.Sprintf("SELECT body FROM %s WHERE aggregate_id = $1", snapshotsTable), id.String())
	require.NoError(t, err)
	assert.Equal(t, 2, len(bodies))
	for _, v := range bodies {
		x, err := codec.Decode(v, eventsourcing.DecoderMeta[ids.AggID]{
			Kind:        test.KindAccount,
			AggregateID: id,
		})
		require.NoError(t, err)
		a := x.(*test.Account)
		assert.Empty(t, a.Owner())
		assert.NotEmpty(t, a.GetID())
	}
}

func TestMigration(t *testing.T) {
	t.Parallel()

	ctx := t.Context()

	eventsTable := test.RandStr("events")
	snapshotsTable := test.RandStr("snapshots")
	r, err := postgresql.NewStoreWithURL(
		dbConfig.URL(),
		postgresql.WithEventsTable[ids.AggID](eventsTable),
		postgresql.WithSnapshotsTable[ids.AggID](snapshotsTable),
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

	// switching the aggregator factory
	codec := test.NewJSONCodecWithUpcaster()
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
	err = db.Select(&snaps, fmt.Sprintf("SELECT * FROM %s WHERE aggregate_id = $1 ORDER by id ASC", snapshotsTable), id.String())
	require.NoError(t, err)
	require.Equal(t, 1, len(snaps))

	snap := snaps[0]
	assert.Equal(t, "Account_V2", snap.AggregateKind.String())
	assert.Equal(t, 9, int(snap.AggregateVersion))
	assert.Equal(t, `{"status":"OPEN","balance":105,"owner":{"firstName":"Paulo","lastName":"Quintans Pereira"}}`, string(snap.Body))
	assert.Equal(t, id.String(), snap.AggregateID)

	evts := []Event{}
	err = db.Select(&evts, fmt.Sprintf("SELECT * FROM %s WHERE aggregate_id = $1 ORDER by id ASC", eventsTable), id.String())
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
	assert.Equal(t, "Paulo", acc2.Owner().FirstName())
	assert.Equal(t, "Quintans Pereira", acc2.Owner().LastName())
}
