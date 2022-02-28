package mysql

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/oklog/ulid/v2"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/quintans/eventsourcing"
	"github.com/quintans/eventsourcing/log"
	"github.com/quintans/eventsourcing/player"
	"github.com/quintans/eventsourcing/store/mysql"
	"github.com/quintans/eventsourcing/store/postgresql"
	"github.com/quintans/eventsourcing/stream/poller"
	"github.com/quintans/eventsourcing/test"
	"github.com/quintans/eventsourcing/util"
)

var logger = log.NewLogrus(logrus.StandardLogger())

func connect(dbConfig DBConfig) (*sqlx.DB, error) {
	dburl := dbConfig.Url()

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
	dbConfig, tearDown, err := setup()
	require.NoError(t, err)
	defer tearDown()

	ctx := context.Background()
	r, err := mysql.NewStore(dbConfig.Url())
	require.NoError(t, err)
	es := eventsourcing.NewEventStore(r, test.NewJSONCodec(), eventsourcing.WithSnapshotThreshold(3))

	id := util.MustNewULID()
	acc := test.CreateAccount("Paulo", id, 100)
	acc.Deposit(10)
	acc.Deposit(20)
	err = es.Create(ctx, acc)
	require.NoError(t, err)
	err = es.Update(
		ctx,
		id.String(),
		func(a eventsourcing.Aggregater) (eventsourcing.Aggregater, error) {
			acc := a.(*test.Account)
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

	evts := []mysql.Event{}
	err = db.Select(&evts, "SELECT * FROM events WHERE aggregate_id = ? ORDER by id ASC", id.String())
	require.NoError(t, err)
	require.Equal(t, 5, len(evts))
	assert.Equal(t, "AccountCreated", evts[0].Kind.String())
	assert.Equal(t, "MoneyDeposited", evts[1].Kind.String())
	assert.Equal(t, "MoneyDeposited", evts[2].Kind.String())
	assert.Equal(t, "MoneyDeposited", evts[3].Kind.String())
	assert.Equal(t, "idempotency-key", string(evts[3].IdempotencyKey))
	assert.Equal(t, test.TypeAccount, evts[0].AggregateType)
	assert.Equal(t, id.String(), evts[0].AggregateID)
	for i := 0; i < len(evts); i++ {
		assert.Equal(t, uint32(i+1), evts[i].AggregateVersion)
	}

	a, err := es.Retrieve(ctx, id.String())
	require.NoError(t, err)
	acc2 := a.(*test.Account)
	assert.Equal(t, id, acc2.ID)
	assert.Equal(t, int64(136), acc2.Balance)
	assert.Equal(t, test.OPEN, acc2.Status)

	found, err := es.HasIdempotencyKey(ctx, "idempotency-key")
	require.NoError(t, err)
	require.True(t, found)

	err = es.Update(
		ctx,
		id.String(),
		func(a eventsourcing.Aggregater) (eventsourcing.Aggregater, error) {
			acc := a.(*test.Account)
			acc.Deposit(5)
			return acc, nil
		},
		eventsourcing.WithIdempotencyKey("idempotency-key"),
	)
	require.Error(t, err)
}

func TestPollListener(t *testing.T) {
	dbConfig, tearDown, err := setup()
	require.NoError(t, err)
	defer tearDown()

	ctx := context.Background()
	r, err := mysql.NewStore(dbConfig.Url())
	require.NoError(t, err)
	es := eventsourcing.NewEventStore(r, test.NewJSONCodec(), eventsourcing.WithSnapshotThreshold(3))

	id := util.MustNewULID()
	acc := test.CreateAccount("Paulo", id, 100)
	acc.Deposit(10)
	acc.Deposit(20)
	err = es.Create(ctx, acc)
	require.NoError(t, err)
	err = es.Update(ctx, id.String(), func(a eventsourcing.Aggregater) (eventsourcing.Aggregater, error) {
		acc := a.(*test.Account)
		acc.Deposit(5)
		return acc, nil
	})
	require.NoError(t, err)
	time.Sleep(time.Second)

	acc2 := test.NewAccount()
	counter := 0
	repository, err := mysql.NewStore(dbConfig.Url())
	require.NoError(t, err)
	lm := poller.New(logger, repository)

	done := make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		select {
		case <-done:
			logger.Info("Done...")
		case <-time.After(2 * time.Second):
			logger.Info("Timeout...")
		}
		logger.Info("Cancelling...")
		cancel()
	}()
	lm.Poll(ctx, player.StartBeginning(), func(ctx context.Context, e eventsourcing.Event) error {
		if e.AggregateID == id.String() {
			if err := es.ApplyChangeFromHistory(acc2, e); err != nil {
				return err
			}
			counter++
			if counter == 4 {
				logger.Info("Reached the expected count. Done.")
				close(done)
			}
		}
		return nil
	})

	assert.Equal(t, 4, counter)
	assert.Equal(t, id, acc2.ID)
	assert.Equal(t, int64(135), acc2.Balance)
	assert.Equal(t, test.OPEN, acc2.Status)
}

func TestListenerWithAggregateType(t *testing.T) {
	dbConfig, tearDown, err := setup()
	require.NoError(t, err)
	defer tearDown()

	ctx := context.Background()
	r, err := mysql.NewStore(dbConfig.Url())
	require.NoError(t, err)
	es := eventsourcing.NewEventStore(r, test.NewJSONCodec(), eventsourcing.WithSnapshotThreshold(3))

	id := util.MustNewULID()
	acc := test.CreateAccount("Paulo", id, 100)
	acc.Deposit(10)
	acc.Deposit(20)
	err = es.Create(ctx, acc)
	require.NoError(t, err)
	err = es.Update(ctx, id.String(), func(a eventsourcing.Aggregater) (eventsourcing.Aggregater, error) {
		acc := a.(*test.Account)
		acc.Deposit(5)
		return acc, nil
	})
	require.NoError(t, err)
	time.Sleep(time.Second)

	acc2 := test.NewAccount()
	counter := 0
	repository, err := mysql.NewStore(dbConfig.Url())
	require.NoError(t, err)
	p := poller.New(logger, repository, poller.WithAggregateTypes(test.TypeAccount))

	done := make(chan struct{})
	go p.Poll(ctx, player.StartBeginning(), func(ctx context.Context, e eventsourcing.Event) error {
		if e.AggregateID == id.String() {
			if err := es.ApplyChangeFromHistory(acc2, e); err != nil {
				return err
			}
			counter++
			if counter == 4 {
				logger.Info("Reached the expected count. Done.")
				close(done)
			}
		}
		return nil
	})

	select {
	case <-done:
		logger.Info("Done...")
	case <-time.After(time.Second):
		logger.Info("Timeout...")
	}
	assert.Equal(t, 4, counter)
	assert.Equal(t, id, acc2.ID)
	assert.Equal(t, int64(135), acc2.Balance)
	assert.Equal(t, test.OPEN, acc2.Status)
}

func TestListenerWithLabels(t *testing.T) {
	dbConfig, tearDown, err := setup()
	require.NoError(t, err)
	defer tearDown()

	ctx := context.Background()
	r, err := mysql.NewStore(dbConfig.Url())
	require.NoError(t, err)
	es := eventsourcing.NewEventStore(r, test.NewJSONCodec(), eventsourcing.WithSnapshotThreshold(3))

	id := util.MustNewULID()
	acc := test.CreateAccount("Paulo", id, 100)
	acc.Deposit(10)
	acc.Deposit(20)
	err = es.Create(ctx, acc, eventsourcing.WithMetadata(map[string]interface{}{"geo": "EU"}))
	require.NoError(t, err)
	err = es.Update(
		ctx,
		id.String(),
		func(a eventsourcing.Aggregater) (eventsourcing.Aggregater, error) {
			acc := a.(*test.Account)
			acc.Deposit(5)
			return acc, nil
		},
		eventsourcing.WithMetadata(map[string]interface{}{"geo": "US"}),
	)
	require.NoError(t, err)
	time.Sleep(time.Second)

	acc2 := test.NewAccount()
	counter := 0

	repository, err := mysql.NewStore(dbConfig.Url())
	require.NoError(t, err)
	p := poller.New(logger, repository, poller.WithMetadataKV("geo", "EU"))

	ctx, cancel := context.WithCancel(ctx)
	var mu sync.Mutex
	errCh := make(chan error, 1)
	go func() {
		err := p.Poll(ctx, player.StartBeginning(), func(ctx context.Context, e eventsourcing.Event) error {
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
		errCh <- err
	}()

	time.Sleep(time.Second)
	cancel()
	require.NoError(t, <-errCh, "unable to poll")

	mu.Lock()
	assert.Equal(t, 3, counter)
	mu.Unlock()
	assert.Equal(t, id, acc2.ID)
	assert.Equal(t, int64(130), acc2.Balance)
	assert.Equal(t, test.OPEN, acc2.Status)
}

func TestForget(t *testing.T) {
	dbConfig, tearDown, err := setup()
	require.NoError(t, err)
	defer tearDown()

	ctx := context.Background()
	r, err := mysql.NewStore(dbConfig.Url())
	require.NoError(t, err)
	es := eventsourcing.NewEventStore(r, test.NewJSONCodec(), eventsourcing.WithSnapshotThreshold(3))

	id := util.MustNewULID()
	acc := test.CreateAccount("Paulo", id, 100)
	acc.UpdateOwner("Paulo Quintans")
	acc.Deposit(10)
	acc.Deposit(20)
	err = es.Create(ctx, acc)
	require.NoError(t, err)
	err = es.Update(ctx, id.String(), func(a eventsourcing.Aggregater) (eventsourcing.Aggregater, error) {
		acc := a.(*test.Account)
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
	evts := [][]byte{}
	err = db.Select(&evts, "SELECT body FROM events WHERE aggregate_id = ? and kind = 'OwnerUpdated'", id.String())
	require.NoError(t, err)
	assert.Equal(t, 2, len(evts))
	for _, v := range evts {
		ou := &test.OwnerUpdated{}
		err = json.Unmarshal(v, ou)
		require.NoError(t, err)
		assert.NotEmpty(t, ou.Owner)
	}

	bodies := [][]byte{}
	err = db.Select(&bodies, "SELECT body FROM snapshots WHERE aggregate_id = ?", id.String())
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
				t.Forget()
				return t
			}
			return i
		},
	)
	require.NoError(t, err)

	evts = [][]byte{}
	err = db.Select(&evts, "SELECT body FROM events WHERE aggregate_id = ? and kind = 'OwnerUpdated'", id.String())
	require.NoError(t, err)
	assert.Equal(t, 2, len(evts))
	for _, v := range evts {
		ou := &test.OwnerUpdated{}
		err = json.Unmarshal(v, ou)
		require.NoError(t, err)
		assert.Empty(t, ou.Owner)
	}

	bodies = [][]byte{}
	err = db.Select(&bodies, "SELECT body FROM snapshots WHERE aggregate_id = ?", id.String())
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

func TestMigration(t *testing.T) {
	dbConfig, tearDown, err := setup()
	require.NoError(t, err)
	defer tearDown()

	ctx := context.Background()
	r, err := mysql.NewStore(dbConfig.Url())
	require.NoError(t, err)
	es := eventsourcing.NewEventStore(r, test.NewJSONCodec(), eventsourcing.WithSnapshotThreshold(3))

	id := ulid.MustParse("014KG56DC01GG4TEB01ZEX7WFJ")
	acc := test.CreateAccount("Paulo Pereira", id, 100)
	acc.Deposit(20)
	acc.Withdraw(15)
	acc.UpdateOwner("Paulo Quintans Pereira")
	err = es.Create(ctx, acc)
	require.NoError(t, err)

	// giving time for the snapshots to write
	time.Sleep(100 * time.Millisecond)

	codec := test.NewJSONCodecV2()
	// switching the aggregator factory
	es = eventsourcing.NewEventStore(r, codec, eventsourcing.WithSnapshotThreshold(3))
	err = es.MigrateInPlaceCopyReplace(ctx,
		1,
		3,
		func(events []*eventsourcing.Event) ([]*eventsourcing.EventMigration, error) {
			var migration []*eventsourcing.EventMigration
			var m *eventsourcing.EventMigration
			// default codec used by the event store
			for _, e := range events {
				var err error
				switch e.Kind {
				case test.KindAccountCreated:
					m, err = test.MigrateAccountCreated(e, codec)
				case test.KindOwnerUpdated:
					m, err = test.MigrateOwnerUpdated(e, codec)
				default:
					m = eventsourcing.DefaultEventMigration(e)
				}
				if err != nil {
					return nil, err
				}
				migration = append(migration, m)
			}
			return migration, nil
		},
		test.TypeAccount,
		test.KindAccountCreated, test.KindOwnerUpdated,
	)
	require.NoError(t, err)

	db, err := connect(dbConfig)
	require.NoError(t, err)
	evts := []postgresql.Event{}
	err = db.Select(&evts, "SELECT * FROM events WHERE aggregate_id = ? ORDER by id ASC", id.String())
	require.NoError(t, err)
	require.Equal(t, 9, len(evts))

	evt := evts[0]
	assert.Equal(t, "AccountCreated", evt.Kind.String())
	assert.Equal(t, 1, int(evt.AggregateVersion))
	assert.Equal(t, `{"id":"014KG56DC01GG4TEB01ZEX7WFJ","money":100,"owner":"Paulo Pereira"}`, string(evt.Body))
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
	assert.Equal(t, `{"id":"014KG56DC01GG4TEB01ZEX7WFJ","money":100,"first_name":"Paulo","last_name":"Pereira"}`, string(evt.Body))
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

	a, err := es.Retrieve(ctx, id.String())
	require.NoError(t, err)
	acc2 := a.(*test.AccountV2)
	assert.Equal(t, "Paulo", acc2.Owner().FirstName())
	assert.Equal(t, "Quintans Pereira", acc2.Owner().LastName())
}
