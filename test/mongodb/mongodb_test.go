//go:build mongo

package mongodb

import (
	"context"
	"log/slog"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/quintans/eventsourcing"
	"github.com/quintans/eventsourcing/sink/poller"
	"github.com/quintans/eventsourcing/store/mongodb"
	"github.com/quintans/eventsourcing/test"
	"github.com/quintans/eventsourcing/util/ids"
)

var (
	logger    = slog.New(slog.NewTextHandler(os.Stdout, nil))
	esOptions = eventsourcing.EsSnapshotThreshold(3)
)

const (
	AggregateAccount    eventsourcing.Kind = "Account"
	EventAccountCreated eventsourcing.Kind = "AccountCreated"
	EventMoneyDeposited eventsourcing.Kind = "MoneyDeposited"
	EventMoneyWithdrawn eventsourcing.Kind = "MoneyWithdrawn"
)

var dbConfig DBConfig

func TestMain(m *testing.M) {
	dbConfig = Setup("./docker-compose.yaml")

	// run the tests
	code := m.Run()

	// exit with the code from the tests
	os.Exit(code)
}

// creates a independent connection
func connect(dbConfig DBConfig) (*mongo.Database, error) {
	connString := dbConfig.URL()

	opts := options.Client().ApplyURI(connString)
	client, err := mongo.Connect(context.Background(), opts)
	if err != nil {
		return nil, err
	}
	db := client.Database(DBName)
	return db, nil
}

func TestSaveAndGet(t *testing.T) {
	t.Parallel()

	ctx := t.Context()

	eventsCollection := test.RandStr(CollEvents)
	snapshotsCollection := test.RandStr(CollSnapshots)
	r, err := mongodb.NewStoreWithURI(
		ctx,
		dbConfig.URL(),
		dbConfig.Database,
		mongodb.WithEventsCollection[ids.AggID](eventsCollection),
		mongodb.WithSnapshotsCollection[ids.AggID](snapshotsCollection),
	)
	require.NoError(t, err)
	defer r.Close(context.Background())

	es, err := eventsourcing.NewEventStore[*test.Account](r, test.NewJSONCodec(), esOptions)
	require.NoError(t, err)

	acc, err := es.Retrieve(ctx, ids.New())
	require.ErrorIs(t, err, eventsourcing.ErrUnknownAggregateID)

	acc, err = test.NewAccount("Paulo", 100)
	id := acc.GetID()
	require.NoError(t, err)
	acc.Deposit(10)
	acc.Withdraw(5)
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

	// giving time for the snapshots to write
	time.Sleep(time.Second)

	evts, err := getEvents(ctx, dbConfig, eventsCollection, id)
	require.NoError(t, err)

	for k, v := range evts {
		assert.Equal(t, AggregateAccount, v.AggregateKind)
		assert.Equal(t, id.String(), v.AggregateID)
		assert.Equal(t, uint32(k+1), v.AggregateVersion)
	}

	require.Equal(t, 5, len(evts))
	assert.Equal(t, EventAccountCreated, evts[0].Kind)
	assert.Equal(t, EventMoneyDeposited, evts[1].Kind)
	assert.Equal(t, EventMoneyWithdrawn, evts[2].Kind)

	acc2, err := es.Retrieve(ctx, id)
	require.NoError(t, err)
	assert.Equal(t, id, acc2.GetID())
	assert.Equal(t, int64(111), acc2.Balance())
	assert.Equal(t, test.OPEN, acc2.Status())
}

func TestPollListener(t *testing.T) {
	t.Parallel()

	ctx := t.Context()

	eventsCollection := test.RandStr(CollEvents)
	snapshotsCollection := test.RandStr(CollSnapshots)
	outboxCollection := test.RandStr("outbox")
	r, err := mongodb.NewStoreWithURI(
		ctx,
		dbConfig.URL(),
		dbConfig.Database,
		mongodb.WithEventsCollection[ids.AggID](eventsCollection),
		mongodb.WithSnapshotsCollection[ids.AggID](snapshotsCollection),
		mongodb.WithTxHandler(mongodb.OutboxInsertHandler[ids.AggID](dbConfig.Database, outboxCollection)),
	)

	require.NoError(t, err)
	defer r.Close(context.Background())

	obs, err := mongodb.NewOutboxStore(r.Client(), dbConfig.Database, outboxCollection, r)
	require.NoError(t, err)

	es, err := eventsourcing.NewEventStore[*test.Account](r, test.NewJSONCodec(), esOptions)
	require.NoError(t, err)

	acc, err := test.NewAccount("Paulo", 100)
	id := acc.GetID()
	require.NoError(t, err)
	acc.Deposit(10)
	acc.Withdraw(5)
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
	p := poller.New(logger, obs)

	ctx, cancel := context.WithCancel(context.Background())

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
	assert.Equal(t, int64(110), acc2.Balance())
	assert.Equal(t, test.OPEN, acc2.Status())
}

func TestListenerWithAggregateKind(t *testing.T) {
	t.Parallel()

	ctx := t.Context()

	eventsCollection := test.RandStr(CollEvents)
	snapshotsCollection := test.RandStr(CollSnapshots)
	outboxCollection := test.RandStr("outbox")
	r, err := mongodb.NewStoreWithURI(
		ctx,
		dbConfig.URL(),
		dbConfig.Database,
		mongodb.WithEventsCollection[ids.AggID](eventsCollection),
		mongodb.WithSnapshotsCollection[ids.AggID](snapshotsCollection),
		mongodb.WithTxHandler(mongodb.OutboxInsertHandler[ids.AggID](dbConfig.Database, outboxCollection)),
	)

	require.NoError(t, err)
	defer r.Close(context.Background())

	obs, err := mongodb.NewOutboxStore(r.Client(), dbConfig.Database, outboxCollection, r)
	require.NoError(t, err)

	es, err := eventsourcing.NewEventStore[*test.Account](r, test.NewJSONCodec(), esOptions)
	require.NoError(t, err)

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
	p := poller.New(logger, obs, poller.WithAggregateKinds[ids.AggID](AggregateAccount))

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
	eventsCollection := test.RandStr(CollEvents)
	snapshotsCollection := test.RandStr(CollSnapshots)
	outboxCollection := test.RandStr("outbox")
	r, err := mongodb.NewStoreWithURI(
		t.Context(),
		dbConfig.URL(),
		dbConfig.Database,
		mongodb.WithEventsCollection[ids.AggID](eventsCollection),
		mongodb.WithSnapshotsCollection[ids.AggID](snapshotsCollection),
		mongodb.WithTxHandler(mongodb.OutboxInsertHandler[ids.AggID](dbConfig.Database, outboxCollection)),
		mongodb.WithDiscriminatorKeys[ids.AggID](key),
	)
	require.NoError(t, err)
	defer r.Close(context.Background())

	obs, err := mongodb.NewOutboxStore(r.Client(), dbConfig.Database, outboxCollection, r)
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

	acc1, err := test.NewAccount("Paulo", 100)
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

	acc2 := test.DehydratedAccount(id)
	counter := 0

	p := poller.New(logger, obs, poller.WithDiscriminatorKV[ids.AggID](key, "xyz"))

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

func TestForget(t *testing.T) {
	t.Parallel()

	ctx := t.Context()

	eventsCollection := test.RandStr(CollEvents)
	snapshotsCollection := test.RandStr(CollSnapshots)
	r, err := mongodb.NewStoreWithURI(
		t.Context(),
		dbConfig.URL(),
		dbConfig.Database,
		mongodb.WithEventsCollection[ids.AggID](eventsCollection),
		mongodb.WithSnapshotsCollection[ids.AggID](snapshotsCollection),
	)
	require.NoError(t, err)
	defer r.Close(context.Background())
	es, err := eventsourcing.NewEventStore[*test.Account](r, test.NewJSONCodec(), esOptions)
	require.NoError(t, err)

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

	db, err := connect(dbConfig)
	require.NoError(t, err)
	cursor, err := db.Collection(eventsCollection).Find(ctx, bson.M{
		"aggregate_id": bson.D{
			{"$eq", id.String()},
		},
		"kind": bson.D{
			{"$eq", "OwnerUpdated"},
		},
	})
	require.NoError(t, err)
	evts := []mongodb.Event{}
	err = cursor.All(ctx, &evts)
	require.NoError(t, err)
	assert.Equal(t, 2, len(evts))
	foundEvent := false
	codec := test.NewJSONCodec()
	for _, e := range evts {
		if e.Kind == "OwnerUpdated" {
			foundEvent = true
			event, er := codec.Decode(e.Body, eventsourcing.DecoderMeta[ids.AggID]{Kind: e.Kind})
			require.NoError(t, er)
			evt := event.(*test.OwnerUpdated)
			assert.NotEmpty(t, evt.Owner)
		}
	}
	assert.True(t, foundEvent)

	cursor, err = db.Collection(snapshotsCollection).Find(ctx, bson.M{
		"aggregate_id": bson.D{
			{"$eq", id.String()},
		},
	})
	require.NoError(t, err)
	snaps := []mongodb.Snapshot{}
	cursor.All(ctx, &snaps)
	assert.Equal(t, 2, len(snaps))
	for _, v := range snaps {
		a, er := codec.Decode(v.Body, eventsourcing.DecoderMeta[ids.AggID]{
			Kind:        test.KindAccount,
			AggregateID: id,
		})
		require.NoError(t, er)
		snap := a.(*test.Account)
		assert.NotEmpty(t, snap.Owner())
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

	time.Sleep(100 * time.Millisecond)

	cursor, err = db.Collection(eventsCollection).Find(ctx, bson.M{
		"aggregate_id": bson.D{
			{"$eq", id.String()},
		},
		"kind": bson.D{
			{"$eq", "OwnerUpdated"},
		},
	})
	require.NoError(t, err)
	evts = []mongodb.Event{}
	err = cursor.All(ctx, &evts)
	require.NoError(t, err)
	assert.Equal(t, 2, len(evts))
	foundEvent = false
	for _, e := range evts {
		if e.Kind == "OwnerUpdated" {
			foundEvent = true
			event, er := codec.Decode(e.Body, eventsourcing.DecoderMeta[ids.AggID]{Kind: e.Kind})
			require.NoError(t, er)
			evt := event.(*test.OwnerUpdated)
			assert.Empty(t, evt.Owner)
		}
	}
	assert.True(t, foundEvent)

	cursor, err = db.Collection(snapshotsCollection).Find(ctx, bson.M{
		"aggregate_id": bson.D{
			{"$eq", id.String()},
		},
	})
	require.NoError(t, err)
	snaps = []mongodb.Snapshot{}
	cursor.All(ctx, &snaps)
	assert.Equal(t, 2, len(snaps))
	for _, v := range snaps {
		s, err := codec.Decode(v.Body, eventsourcing.DecoderMeta[ids.AggID]{
			Kind:        test.KindAccount,
			AggregateID: id,
		})
		require.NoError(t, err)
		snap := s.(*test.Account)
		assert.Empty(t, snap.Owner())
		assert.NotEmpty(t, snap.GetID())
	}
}

func TestMigration(t *testing.T) {
	t.Parallel()

	ctx := t.Context()

	eventsCollection := test.RandStr(CollEvents)
	snapshotsCollection := test.RandStr(CollSnapshots)
	r, err := mongodb.NewStoreWithURI(
		t.Context(),
		dbConfig.URL(),
		dbConfig.Database,
		mongodb.WithEventsCollection[ids.AggID](eventsCollection),
		mongodb.WithSnapshotsCollection[ids.AggID](snapshotsCollection),
	)
	require.NoError(t, err)
	defer r.Close(context.Background())
	es1, err := eventsourcing.NewEventStore[*test.Account](r, test.NewJSONCodec(), esOptions)
	require.NoError(t, err)

	acc, err := test.NewAccount("Paulo Pereira", 100)
	id := acc.GetID()
	require.NoError(t, err)
	acc.Deposit(20)
	acc.Withdraw(15)
	acc.UpdateOwner("Paulo Quintans Pereira")
	err = es1.Create(ctx, acc)
	require.NoError(t, err)

	// giving time for the snapshots to write
	time.Sleep(time.Second)

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

	snaps, err := getSnapshots(ctx, dbConfig, snapshotsCollection, id)
	require.NoError(t, err)
	require.Equal(t, 1, len(snaps))

	snap := snaps[0]
	assert.Equal(t, "Account_V2", snap.AggregateKind.String())
	assert.Equal(t, 9, int(snap.AggregateVersion))
	assert.Equal(t, `{"status":"OPEN","balance":105,"owner":{"firstName":"Paulo","lastName":"Quintans Pereira"}}`, string(snap.Body))
	assert.Equal(t, id.String(), snap.AggregateID)

	evts, err := getEvents(ctx, dbConfig, eventsCollection, id)
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

func getEvents(ctx context.Context, dbConfig DBConfig, coll string, id ids.AggID) ([]mongodb.Event, error) {
	db, err := connect(dbConfig)
	if err != nil {
		return nil, err
	}
	defer db.Client().Disconnect(ctx)
	opts := options.Find().SetSort(bson.D{{"_id", 1}})
	cursor, err := db.Collection(coll).Find(ctx, bson.M{
		"aggregate_id": bson.D{
			{"$eq", id.String()},
		},
	}, opts)
	if err != nil {
		return nil, err
	}
	evts := []mongodb.Event{}
	err = cursor.All(ctx, &evts)
	if err != nil {
		return nil, err
	}

	return evts, nil
}

func getSnapshots(ctx context.Context, dbConfig DBConfig, coll string, id ids.AggID) ([]mongodb.Snapshot, error) {
	db, err := connect(dbConfig)
	if err != nil {
		return nil, err
	}
	defer db.Client().Disconnect(ctx)
	opts := options.Find().SetSort(bson.D{{"_id", 1}})
	cursor, err := db.Collection(coll).Find(ctx, bson.M{
		"aggregate_id": bson.D{
			{"$eq", id.String()},
		},
	}, opts)
	if err != nil {
		return nil, err
	}
	snaps := []mongodb.Snapshot{}
	err = cursor.All(ctx, &snaps)
	if err != nil {
		return nil, err
	}

	return snaps, nil
}
