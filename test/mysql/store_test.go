//go:build mysql

package mysql

import (
	"context"
	"log/slog"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/oklog/ulid/v2"
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
	esOptions = &eventsourcing.EsOptions{
		SnapshotThreshold: 3,
	}
)

type Snapshot struct {
	ID               eventid.EventID    `db:"id,omitempty"`
	AggregateID      string             `db:"aggregate_id,omitempty"`
	AggregateVersion uint32             `db:"aggregate_version,omitempty"`
	AggregateKind    eventsourcing.Kind `db:"aggregate_kind,omitempty"`
	Body             []byte             `db:"body,omitempty"`
	CreatedAt        time.Time          `db:"created_at,omitempty"`
	MetaTenant       store.NilString    `db:"meta_tenant,omitempty"`
}

type Event struct {
	ID               eventid.EventID    `db:"id"`
	AggregateID      string             `db:"aggregate_id"`
	AggregateIDHash  int32              `db:"aggregate_id_hash"`
	AggregateVersion uint32             `db:"aggregate_version"`
	AggregateKind    eventsourcing.Kind `db:"aggregate_kind"`
	Kind             eventsourcing.Kind `db:"kind"`
	Body             []byte             `db:"body"`
	IdempotencyKey   store.NilString    `db:"idempotency_key"`
	CreatedAt        time.Time          `db:"created_at"`
	Migration        int                `db:"migration"`
	Migrated         bool               `db:"migrated"`
	MetaTenant       store.NilString    `db:"meta_tenant,omitempty"`
}

func connect(dbConfig DBConfig) (*sqlx.DB, error) {
	dburl := dbConfig.URL()

	db, err := sqlx.Open("mysql", dburl)
	if err != nil {
		return nil, err
	}
	if err := db.Ping(); err != nil {
		return nil, err
	}
	return db, nil
}

func TestSaveAndGet(t *testing.T) {
	t.Parallel()

	dbConfig := Setup(t)

	ctx := context.Background()
	r, err := mysql.NewStoreWithURL[ids.AggID](dbConfig.URL())
	require.NoError(t, err)
	es := eventsourcing.NewEventStore[*test.Account](r, test.NewJSONCodec(), esOptions)

	acc, err := test.NewAccount("Paulo", 100)
	id := acc.GetID()
	require.NoError(t, err)
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
		eventsourcing.WithIdempotencyKey("idempotency-key"),
	)
	require.NoError(t, err)
	time.Sleep(time.Second)

	// giving time for the snapshots to write
	time.Sleep(100 * time.Millisecond)

	db, err := connect(dbConfig)
	require.NoError(t, err)
	count := 0
	err = db.Get(&count, "SELECT count(*) FROM snapshots WHERE aggregate_id = ?", id.String())
	require.NoError(t, err)
	require.Equal(t, 1, count)

	evts := []Event{}
	err = db.Select(&evts, "SELECT * FROM events WHERE aggregate_id = ? ORDER by id ASC", id.String())
	require.NoError(t, err)
	require.Equal(t, 5, len(evts))
	assert.Equal(t, "AccountCreated", evts[0].Kind.String())
	assert.Equal(t, "MoneyDeposited", evts[1].Kind.String())
	assert.Equal(t, "MoneyDeposited", evts[2].Kind.String())
	assert.Equal(t, "MoneyDeposited", evts[3].Kind.String())
	assert.Equal(t, "idempotency-key", string(evts[3].IdempotencyKey))
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

	found, err := es.HasIdempotencyKey(ctx, "idempotency-key")
	require.NoError(t, err)
	require.True(t, found)

	err = es.Update(
		ctx,
		id,
		func(acc *test.Account) (*test.Account, error) {
			acc.Deposit(5)
			return acc, nil
		},
		eventsourcing.WithIdempotencyKey("idempotency-key"),
	)
	require.Error(t, err)
}

func TestPollListener(t *testing.T) {
	t.Parallel()

	dbConfig := Setup(t)

	ctx := context.Background()
	r, err := mysql.NewStoreWithURL(
		dbConfig.URL(),
		mysql.WithTxHandler(mysql.OutboxInsertHandler[ids.AggID]("outbox")),
	)
	require.NoError(t, err)
	es := eventsourcing.NewEventStore[*test.Account](r, test.NewJSONCodec(), esOptions)

	acc, err := test.NewAccount("Paulo", 100)
	id := acc.GetID()
	require.NoError(t, err)
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
	outboxRepo := mysql.NewOutboxStore(r.Connection(), "outbox", r)
	require.NoError(t, err)

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

	dbConfig := Setup(t)

	ctx := context.Background()
	r, err := mysql.NewStoreWithURL(
		dbConfig.URL(),
		mysql.WithTxHandler(mysql.OutboxInsertHandler[ids.AggID]("outbox")),
	)
	require.NoError(t, err)
	es := eventsourcing.NewEventStore[*test.Account](r, test.NewJSONCodec(), esOptions)

	acc, err := test.NewAccount("Paulo", 100)
	id := acc.GetID()
	require.NoError(t, err)
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
	outboxRepo := mysql.NewOutboxStore(r.Connection(), "outbox", r)
	require.NoError(t, err)
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

func TestListenerWithMetadata(t *testing.T) {
	t.Parallel()

	dbConfig := Setup(t)

	key := "tenant"

	r, err := mysql.NewStoreWithURL(
		dbConfig.URL(),
		mysql.WithTxHandler(mysql.OutboxInsertHandler[ids.AggID]("outbox")),
		mysql.WithMetadataHook[ids.AggID](func(ctx context.Context) eventsourcing.Metadata {
			val := ctx.Value(key).(string)
			return eventsourcing.Metadata{key: val}
		}),
	)
	require.NoError(t, err)
	es := eventsourcing.NewEventStore[*test.Account](r, test.NewJSONCodec(), esOptions)

	ctx := context.WithValue(context.Background(), key, "abc")
	acc, err := test.NewAccount("Paulo", 50)
	require.NoError(t, err)
	acc.Deposit(20)
	err = es.Create(ctx, acc)
	require.NoError(t, err)

	ctx = context.WithValue(context.Background(), key, "xyz")
	acc1, err := test.NewAccount("Pereira", 100)
	id := acc1.GetID()
	require.NoError(t, err)
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

	outboxRepo := mysql.NewOutboxStore(r.Connection(), "outbox", r)
	require.NoError(t, err)
	p := poller.New(logger, outboxRepo, poller.WithMetadataKV[ids.AggID]("tenant", "xyz"))

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

	dbConfig := Setup(t)

	ctx := context.Background()
	r, err := mysql.NewStoreWithURL[ids.AggID](dbConfig.URL())
	require.NoError(t, err)
	es := eventsourcing.NewEventStore[*test.Account](r, test.NewJSONCodec(), esOptions)

	acc, err := test.NewAccount("Paulo", 100)
	id := acc.GetID()
	require.NoError(t, err)
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

	db, err := connect(dbConfig)
	require.NoError(t, err)
	evts := [][]byte{}
	err = db.Select(&evts, "SELECT body FROM events WHERE aggregate_id = ? and kind = 'OwnerUpdated'", id.String())
	require.NoError(t, err)
	assert.Equal(t, 2, len(evts))
	for _, v := range evts {
		e, er := codec.Decode(v, eventsourcing.DecoderMeta[ids.AggID]{Kind: test.KindOwnerUpdated})
		ou := e.(*test.OwnerUpdated)
		require.NoError(t, er)
		assert.NotEmpty(t, ou.Owner)
	}

	bodies := [][]byte{}
	err = db.Select(&bodies, "SELECT body FROM snapshots WHERE aggregate_id = ?", id.String())
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
	err = db.Select(&evts, "SELECT body FROM events WHERE aggregate_id = ? and kind = 'OwnerUpdated'", id.String())
	require.NoError(t, err)
	assert.Equal(t, 2, len(evts))
	for _, v := range evts {
		e, er := codec.Decode(v, eventsourcing.DecoderMeta[ids.AggID]{Kind: test.KindOwnerUpdated})
		ou := e.(*test.OwnerUpdated)
		require.NoError(t, er)
		assert.Empty(t, ou.Owner)
	}

	bodies = [][]byte{}
	err = db.Select(&bodies, "SELECT body FROM snapshots WHERE aggregate_id = ?", id.String())
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

	dbConfig := Setup(t)

	ctx := context.Background()
	r, err := mysql.NewStoreWithURL[ids.AggID](dbConfig.URL())
	require.NoError(t, err)
	es1 := eventsourcing.NewEventStore[*test.Account](r, test.NewJSONCodec(), esOptions)

	id := ids.AggID(ulid.MustParse("014KG56DC01GG4TEB01ZEX7WFJ"))
	acc, err := test.NewAccountWithID("Paulo Pereira", id, 100)
	require.NoError(t, err)
	acc.Deposit(20)
	acc.Withdraw(15)
	acc.UpdateOwner("Paulo Quintans Pereira")
	err = es1.Create(ctx, acc)
	require.NoError(t, err)

	// giving time for the snapshots to write
	time.Sleep(100 * time.Millisecond)

	codec := test.NewJSONCodecWithUpcaster()
	// switching the aggregator factory
	es2 := eventsourcing.NewEventStore[*test.AccountV2](r, codec, esOptions)
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

	db, err := connect(dbConfig)
	require.NoError(t, err)

	snaps := []Snapshot{}
	err = db.Select(&snaps, "SELECT * FROM snapshots WHERE aggregate_id = ? ORDER by id ASC", id.String())
	require.NoError(t, err)
	require.Equal(t, 1, len(snaps))

	snap := snaps[0]
	assert.Equal(t, "Account_V2", snap.AggregateKind.String())
	assert.Equal(t, 9, int(snap.AggregateVersion))
	assert.Equal(t, `{"id":"014KG56DC01GG4TEB01ZEX7WFJ","status":"OPEN","balance":105,"owner":{"firstName":"Paulo","lastName":"Quintans Pereira"}}`, string(snap.Body))

	evts := []Event{}
	err = db.Select(&evts, "SELECT * FROM events WHERE aggregate_id = ? ORDER by id ASC", id.String())
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
