//go:build pg

package pg

import (
	"context"
	"sync"
	"testing"
	"time"

	_ "github.com/lib/pq"
	"github.com/oklog/ulid/v2"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/quintans/eventsourcing"
	"github.com/quintans/eventsourcing/log"
	"github.com/quintans/eventsourcing/sink/poller"
	"github.com/quintans/eventsourcing/store/postgresql"
	"github.com/quintans/eventsourcing/test"
	"github.com/quintans/eventsourcing/util"
)

const (
	aggregateKind eventsourcing.Kind = "Account"
)

var logger = log.NewLogrus(logrus.StandardLogger())

var esOptions = &eventsourcing.EsOptions{
	SnapshotThreshold: 3,
}

func TestSaveAndGet(t *testing.T) {
	t.Parallel()

	dbConfig, tearDown, err := setup()
	require.NoError(t, err)
	defer tearDown()

	ctx := context.Background()
	r, err := postgresql.NewStore(dbConfig.URL())
	require.NoError(t, err)
	es := eventsourcing.NewEventStore[*test.Account](r, test.NewJSONCodec(), esOptions)

	id := util.MustNewULID()
	acc, err := test.CreateAccount("Paulo", id, 100)
	require.NoError(t, err)
	acc.Deposit(10)
	acc.Deposit(20)
	err = es.Create(ctx, acc)
	require.NoError(t, err)
	err = es.Update(
		ctx,
		id.String(),
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
	err = db.Get(&count, "SELECT count(*) FROM snapshots WHERE aggregate_id = $1", id.String())
	require.NoError(t, err)
	require.Equal(t, 1, count)

	evts := []postgresql.Event{}
	err = db.Select(&evts, "SELECT * FROM events WHERE aggregate_id = $1 ORDER by id ASC", id.String())
	require.NoError(t, err)
	require.Equal(t, 5, len(evts))
	assert.Equal(t, "AccountCreated", evts[0].Kind.String())
	assert.Equal(t, "MoneyDeposited", evts[1].Kind.String())
	assert.Equal(t, "MoneyDeposited", evts[2].Kind.String())
	assert.Equal(t, "MoneyDeposited", evts[3].Kind.String())
	assert.Equal(t, "idempotency-key", string(evts[3].IdempotencyKey))
	assert.Equal(t, aggregateKind, evts[0].AggregateKind)
	assert.Equal(t, id.String(), evts[0].AggregateID)
	for i := 0; i < len(evts); i++ {
		assert.Equal(t, uint32(i+1), evts[i].AggregateVersion)
	}

	acc2, err := es.Retrieve(ctx, id.String())
	require.NoError(t, err)
	assert.Equal(t, id, acc2.ID())
	assert.Equal(t, int64(136), acc2.Balance())
	assert.Equal(t, test.OPEN, acc2.Status())

	found, err := es.HasIdempotencyKey(ctx, "idempotency-key")
	require.NoError(t, err)
	require.True(t, found)

	err = es.Update(
		ctx,
		id.String(),
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

	dbConfig, tearDown, err := setup()
	require.NoError(t, err)
	defer tearDown()

	ctx := context.Background()
	r, err := postgresql.NewStore(dbConfig.URL())
	require.NoError(t, err)
	es := eventsourcing.NewEventStore[*test.Account](r, test.NewJSONCodec(), esOptions)

	id := util.MustNewULID()
	acc, _ := test.CreateAccount("Paulo", id, 100)
	acc.Deposit(10)
	acc.Deposit(20)
	err = es.Create(ctx, acc)
	require.NoError(t, err)
	err = es.Update(ctx, id.String(), func(acc *test.Account) (*test.Account, error) {
		acc.Deposit(5)
		return acc, nil
	})
	require.NoError(t, err)
	time.Sleep(time.Second)

	acc2 := test.NewAccount()
	counter := 0
	repository, err := postgresql.NewStore(dbConfig.URL())
	require.NoError(t, err)
	var mu sync.Mutex

	p := poller.New(logger, repository)
	ctx, cancel := context.WithCancel(ctx)

	mockSink := test.NewMockSink(1)
	mockSink.OnSink(func(ctx context.Context, e *eventsourcing.Event) error {
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

	go p.Feed(ctx, mockSink)

	time.Sleep(time.Second)
	cancel()
	time.Sleep(100 * time.Millisecond)

	assert.Equal(t, 4, counter)
	assert.Equal(t, id, acc2.ID())
	assert.Equal(t, int64(135), acc2.Balance())
	assert.Equal(t, test.OPEN, acc2.Status())
}

func TestListenerWithAggregateKind(t *testing.T) {
	t.Parallel()

	dbConfig, tearDown, err := setup()
	require.NoError(t, err)
	defer tearDown()

	ctx := context.Background()
	r, err := postgresql.NewStore(dbConfig.URL())
	require.NoError(t, err)
	es := eventsourcing.NewEventStore[*test.Account](r, test.NewJSONCodec(), esOptions)

	id := util.MustNewULID()
	acc, _ := test.CreateAccount("Paulo", id, 100)
	acc.Deposit(10)
	acc.Deposit(20)
	err = es.Create(ctx, acc)
	require.NoError(t, err)
	err = es.Update(ctx, id.String(), func(a *test.Account) (*test.Account, error) {
		a.Deposit(5)
		return a, nil
	})
	require.NoError(t, err)
	time.Sleep(time.Second)

	acc2 := test.NewAccount()
	counter := 0
	repository, err := postgresql.NewStore(dbConfig.URL())
	require.NoError(t, err)
	p := poller.New(logger, repository, poller.WithAggregateKinds(aggregateKind))

	ctx, cancel := context.WithCancel(ctx)
	var mu sync.Mutex

	mockSink := test.NewMockSink(1)
	mockSink.OnSink(func(ctx context.Context, e *eventsourcing.Event) error {
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

	go p.Feed(ctx, mockSink)

	time.Sleep(time.Second)
	cancel()
	time.Sleep(100 * time.Millisecond)

	assert.Equal(t, 4, counter)
	assert.Equal(t, id, acc2.ID())
	assert.Equal(t, int64(135), acc2.Balance())
	assert.Equal(t, test.OPEN, acc2.Status())
}

func TestListenerWithMetadata(t *testing.T) {
	t.Parallel()

	dbConfig, tearDown, err := setup()
	require.NoError(t, err)
	defer tearDown()

	ctx := context.Background()
	r, err := postgresql.NewStore(dbConfig.URL())
	require.NoError(t, err)
	es := eventsourcing.NewEventStore[*test.Account](r, test.NewJSONCodec(), esOptions)

	id := util.MustNewULID()
	acc, _ := test.CreateAccount("Paulo", id, 100)
	acc.Deposit(10)
	acc.Deposit(20)
	err = es.Create(ctx, acc, eventsourcing.WithMetadata(map[string]interface{}{"geo": "EU"}))
	require.NoError(t, err)
	err = es.Update(
		ctx,
		id.String(),
		func(a *test.Account) (*test.Account, error) {
			a.Deposit(5)
			return a, nil
		},
		eventsourcing.WithMetadata(map[string]interface{}{"geo": "US"}),
	)
	require.NoError(t, err)
	time.Sleep(time.Second)

	acc2 := test.NewAccount()
	counter := 0

	repository, err := postgresql.NewStore(dbConfig.URL())
	require.NoError(t, err)
	p := poller.New(logger, repository, poller.WithMetadataKV("geo", "EU"))

	ctx, cancel := context.WithCancel(ctx)
	var mu sync.Mutex

	mockSink := test.NewMockSink(1)
	mockSink.OnSink(func(ctx context.Context, e *eventsourcing.Event) error {
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

	go p.Feed(ctx, mockSink)

	time.Sleep(time.Second)
	cancel()
	time.Sleep(100 * time.Millisecond)

	mu.Lock()
	assert.Equal(t, 3, counter)
	mu.Unlock()
	assert.Equal(t, id, acc2.ID())
	assert.Equal(t, int64(130), acc2.Balance())
	assert.Equal(t, test.OPEN, acc2.Status())
}

func TestForget(t *testing.T) {
	t.Parallel()

	dbConfig, tearDown, err := setup()
	require.NoError(t, err)
	defer tearDown()

	ctx := context.Background()
	r, err := postgresql.NewStore(dbConfig.URL())
	require.NoError(t, err)
	es := eventsourcing.NewEventStore[*test.Account](r, test.NewJSONCodec(), esOptions)

	id := util.MustNewULID()
	acc, _ := test.CreateAccount("Paulo", id, 100)
	acc.UpdateOwner("Paulo Quintans")
	acc.Deposit(10)
	acc.Deposit(20)
	err = es.Create(ctx, acc)
	require.NoError(t, err)
	err = es.Update(ctx, id.String(), func(a *test.Account) (*test.Account, error) {
		a.Deposit(5)
		a.Withdraw(15)
		a.UpdateOwner("Paulo Quintans Pereira")
		return a, nil
	})
	require.NoError(t, err)

	// giving time for the snapshots to write
	time.Sleep(100 * time.Millisecond)

	codec := test.NewJSONCodec()

	db, err := connect(dbConfig)
	require.NoError(t, err)
	evts := [][]byte{}
	err = db.Select(&evts, "SELECT body FROM events WHERE aggregate_id = $1 and kind = 'OwnerUpdated'", id.String())
	require.NoError(t, err)
	assert.Equal(t, 2, len(evts))
	for _, v := range evts {
		e, er := codec.Decode(v, test.KindOwnerUpdated)
		require.NoError(t, er)
		ou := e.(*test.OwnerUpdated)
		assert.NotEmpty(t, ou.Owner)
	}

	bodies := [][]byte{}
	err = db.Select(&bodies, "SELECT body FROM snapshots WHERE aggregate_id = $1", id.String())
	require.NoError(t, err)
	assert.Equal(t, 2, len(bodies))
	for _, v := range bodies {
		x, er := codec.Decode(v, test.KindAccount)
		require.NoError(t, er)
		a := x.(*test.Account)
		assert.NotEmpty(t, a.Owner())
	}

	err = es.Forget(ctx,
		eventsourcing.ForgetRequest{
			AggregateID: id.String(),
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
	err = db.Select(&evts, "SELECT body FROM events WHERE aggregate_id = $1 and kind = 'OwnerUpdated'", id.String())
	require.NoError(t, err)
	assert.Equal(t, 2, len(evts))
	for _, v := range evts {
		e, er := codec.Decode(v, test.KindOwnerUpdated)
		require.NoError(t, er)
		ou := e.(*test.OwnerUpdated)
		assert.Empty(t, ou.Owner)
	}

	bodies = [][]byte{}
	err = db.Select(&bodies, "SELECT body FROM snapshots WHERE aggregate_id = $1", id.String())
	require.NoError(t, err)
	assert.Equal(t, 2, len(bodies))
	for _, v := range bodies {
		x, err := codec.Decode(v, test.KindAccount)
		require.NoError(t, err)
		a := x.(*test.Account)
		assert.Empty(t, a.Owner())
		assert.NotEmpty(t, a.ID)
	}
}

func BenchmarkDepositAndSave2(b *testing.B) {
	dbConfig, tearDown, err := setup()
	require.NoError(b, err)
	defer tearDown()

	r, _ := postgresql.NewStore(dbConfig.URL())
	es := eventsourcing.NewEventStore[*test.Account](r, test.NewJSONCodec(), &eventsourcing.EsOptions{SnapshotThreshold: 50})
	b.RunParallel(func(pb *testing.PB) {
		ctx := context.Background()
		id := util.MustNewULID()
		acc, _ := test.CreateAccount("Paulo", id, 0)

		for pb.Next() {
			_ = acc.Deposit(10)
			_ = es.Create(ctx, acc)
		}
	})
}

func TestMigration(t *testing.T) {
	t.Parallel()

	dbConfig, tearDown, err := setup()
	require.NoError(t, err)
	defer tearDown()

	ctx := context.Background()
	r, err := postgresql.NewStore(dbConfig.URL())
	require.NoError(t, err)

	es1 := eventsourcing.NewEventStore[*test.Account](r, test.NewJSONCodec(), esOptions)

	id := ulid.MustParse("014KG56DC01GG4TEB01ZEX7WFJ")
	acc, _ := test.CreateAccount("Paulo Pereira", id, 100)
	acc.Deposit(20)
	acc.Withdraw(15)
	acc.UpdateOwner("Paulo Quintans Pereira")
	err = es1.Create(ctx, acc)
	require.NoError(t, err)

	// giving time for the snapshots to write
	time.Sleep(100 * time.Millisecond)

	// switching the aggregator factory
	codec := test.NewJSONCodecWithUpcaster()
	es2 := eventsourcing.NewEventStore[*test.AccountV2](r, codec, esOptions)
	err = es2.MigrateInPlaceCopyReplace(ctx,
		1,
		3,
		func(events []*eventsourcing.Event) ([]*eventsourcing.EventMigration, error) {
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

	snaps := []postgresql.Snapshot{}
	err = db.Select(&snaps, "SELECT * FROM snapshots WHERE aggregate_id = $1 ORDER by id ASC", id.String())
	require.NoError(t, err)
	require.Equal(t, 1, len(snaps))

	snap := snaps[0]
	assert.Equal(t, "Account_V2", snap.AggregateKind.String())
	assert.Equal(t, 9, int(snap.AggregateVersion))
	assert.Equal(t, `{"id":"014KG56DC01GG4TEB01ZEX7WFJ","status":"OPEN","balance":105,"owner":{"firstName":"Paulo","lastName":"Quintans Pereira"}}`, string(snap.Body))

	evts := []postgresql.Event{}
	err = db.Select(&evts, "SELECT * FROM events WHERE aggregate_id = $1 ORDER by id ASC", id.String())
	require.NoError(t, err)
	require.Equal(t, 9, len(evts))

	evt := evts[0]
	assert.Equal(t, "AccountCreated", evt.Kind.String())
	assert.Equal(t, 1, int(evt.AggregateVersion))
	assert.Equal(t, `{"id":"014KG56DC01GG4TEB01ZEX7WFJ","money":100,"owner":"Paulo Pereira"}`, string(evt.Body))
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
	assert.Equal(t, `{"id":"014KG56DC01GG4TEB01ZEX7WFJ","money":100,"owner":{"firstName":"Paulo","lastName":"Pereira"}}`, string(evt.Body))
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

	acc2, err := es2.Retrieve(ctx, id.String())
	assert.Equal(t, "Paulo", acc2.Owner().FirstName())
	assert.Equal(t, "Quintans Pereira", acc2.Owner().LastName())
}
