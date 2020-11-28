package pg

import (
	"context"
	"encoding/json"
	"log"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	"github.com/quintans/eventstore"
	"github.com/quintans/eventstore/common"
	"github.com/quintans/eventstore/player"
	"github.com/quintans/eventstore/store"
	"github.com/quintans/eventstore/store/poller"
	"github.com/quintans/eventstore/store/postgresql"
	"github.com/quintans/eventstore/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func connect(dburl string) (*sqlx.DB, error) {
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
	ctx := context.Background()
	r, err := postgresql.NewStore(dbURL, test.StructFactory{})
	require.NoError(t, err)
	es := eventstore.NewEventStore(r, 3)

	id := uuid.New().String()
	acc := test.CreateAccount("Paulo", id, 100)
	acc.Deposit(10)
	acc.Deposit(20)
	err = es.Save(ctx, acc, eventstore.Options{})
	require.NoError(t, err)
	acc.Deposit(5)
	err = es.Save(ctx, acc, eventstore.Options{})
	require.NoError(t, err)
	time.Sleep(time.Second)

	// giving time for the snapshots to write
	time.Sleep(100 * time.Millisecond)

	db, err := connect(dbURL)
	require.NoError(t, err)
	count := 0
	err = db.Get(&count, "SELECT count(*) FROM snapshots WHERE aggregate_id = $1", id)
	require.NoError(t, err)
	require.Equal(t, 1, count)

	evts := []postgresql.Event{}
	err = db.Select(&evts, "SELECT * FROM events WHERE aggregate_id = $1", id)
	require.NoError(t, err)
	require.Equal(t, 4, len(evts))
	assert.Equal(t, "AccountCreated", evts[0].Kind)
	assert.Equal(t, "MoneyDeposited", evts[1].Kind)
	assert.Equal(t, "MoneyDeposited", evts[2].Kind)
	assert.Equal(t, "MoneyDeposited", evts[3].Kind)
	assert.Equal(t, "Account", evts[0].AggregateType)
	assert.Equal(t, id, evts[0].AggregateID)
	assert.Equal(t, uint32(1), evts[0].AggregateVersion)

	acc2 := test.NewAccount()
	err = es.GetByID(ctx, id, acc2)
	require.NoError(t, err)
	assert.Equal(t, id, acc2.ID)
	assert.Equal(t, uint32(4), acc2.Version)
	assert.Equal(t, int64(135), acc2.Balance)
	assert.Equal(t, test.OPEN, acc2.Status)
	assert.Equal(t, uint32(4), acc2.GetEventsCounter())
}

func TestPollListener(t *testing.T) {
	ctx := context.Background()
	r, err := postgresql.NewStore(dbURL, test.StructFactory{})
	require.NoError(t, err)
	es := eventstore.NewEventStore(r, 3)

	id := uuid.New().String()
	acc := test.CreateAccount("Paulo", id, 100)
	acc.Deposit(10)
	acc.Deposit(20)
	err = es.Save(ctx, acc, eventstore.Options{})
	require.NoError(t, err)
	acc.Deposit(5)
	err = es.Save(ctx, acc, eventstore.Options{})
	require.NoError(t, err)
	time.Sleep(time.Second)

	acc2 := test.NewAccount()
	counter := 0
	repository, err := postgresql.NewStore(dbURL, test.StructFactory{})
	require.NoError(t, err)
	lm := poller.New(repository)

	done := make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		select {
		case <-done:
			log.Println("Done...")
		case <-time.After(2 * time.Second):
			log.Println("Timeout...")
		}
		log.Println("Cancelling...")
		cancel()
	}()
	lm.Poll(ctx, player.StartBeginning(), func(ctx context.Context, e eventstore.Event) error {
		if e.AggregateID == id {
			if err := test.ApplyChangeFromHistory(acc2, e); err != nil {
				return err
			}
			counter++
			if counter == 4 {
				log.Println("Reached the expected count. Done.")
				close(done)
			}
		}
		return nil
	})

	assert.Equal(t, 4, counter)
	assert.Equal(t, id, acc2.ID)
	assert.Equal(t, uint32(4), acc2.Version)
	assert.Equal(t, int64(135), acc2.Balance)
	assert.Equal(t, test.OPEN, acc2.Status)
}

func TestListenerWithAggregateType(t *testing.T) {
	ctx := context.Background()
	r, err := postgresql.NewStore(dbURL, test.StructFactory{})
	require.NoError(t, err)
	es := eventstore.NewEventStore(r, 3)

	id := uuid.New().String()
	acc := test.CreateAccount("Paulo", id, 100)
	acc.Deposit(10)
	acc.Deposit(20)
	err = es.Save(ctx, acc, eventstore.Options{})
	require.NoError(t, err)
	acc.Deposit(5)
	err = es.Save(ctx, acc, eventstore.Options{})
	require.NoError(t, err)
	time.Sleep(time.Second)

	acc2 := test.NewAccount()
	counter := 0
	repository, err := postgresql.NewStore(dbURL, test.StructFactory{})
	require.NoError(t, err)
	p := poller.New(repository)

	done := make(chan struct{})
	go p.Poll(ctx, player.StartBeginning(), func(ctx context.Context, e eventstore.Event) error {
		if e.AggregateID == id {
			if err := test.ApplyChangeFromHistory(acc2, e); err != nil {
				return err
			}
			counter++
			if counter == 4 {
				log.Println("Reached the expected count. Done.")
				close(done)
			}
		}
		return nil
	}, store.WithAggregateTypes("Account"))

	select {
	case <-done:
		log.Println("Done...")
	case <-time.After(time.Second):
		log.Println("Timeout...")
	}
	assert.Equal(t, 4, counter)
	assert.Equal(t, id, acc2.ID)
	assert.Equal(t, uint32(4), acc2.Version)
	assert.Equal(t, int64(135), acc2.Balance)
	assert.Equal(t, test.OPEN, acc2.Status)
}

func TestListenerWithLabels(t *testing.T) {
	ctx := context.Background()
	r, err := postgresql.NewStore(dbURL, test.StructFactory{})
	require.NoError(t, err)
	es := eventstore.NewEventStore(r, 3)

	id := uuid.New().String()
	acc := test.CreateAccount("Paulo", id, 100)
	acc.Deposit(10)
	acc.Deposit(20)
	err = es.Save(ctx, acc, eventstore.Options{
		Labels: map[string]interface{}{
			"geo": "EU",
		},
	})
	require.NoError(t, err)
	acc.Deposit(5)
	err = es.Save(ctx, acc, eventstore.Options{
		Labels: map[string]interface{}{
			"geo": "US",
		},
	})
	require.NoError(t, err)
	time.Sleep(time.Second)

	acc2 := test.NewAccount()
	counter := 0

	repository, err := postgresql.NewStore(dbURL, test.StructFactory{})
	require.NoError(t, err)
	p := poller.New(repository)

	done := make(chan struct{})
	go p.Poll(ctx, player.StartBeginning(), func(ctx context.Context, e eventstore.Event) error {
		if e.AggregateID == id {
			if err := test.ApplyChangeFromHistory(acc2, e); err != nil {
				return err
			}
			counter++
			if counter == 3 {
				log.Println("Reached the expected count. Done.")
				close(done)
			}
		}
		return nil
	}, store.WithLabel("geo", "EU"))

	select {
	case <-done:
		log.Println("Done...")
	case <-time.After(time.Second):
		log.Println("Timeout...")
	}
	assert.Equal(t, 3, counter)
	assert.Equal(t, id, acc2.ID)
	assert.Equal(t, uint32(3), acc2.Version)
	assert.Equal(t, int64(130), acc2.Balance)
	assert.Equal(t, test.OPEN, acc2.Status)
}

func TestForget(t *testing.T) {
	ctx := context.Background()
	r, err := postgresql.NewStore(dbURL, test.StructFactory{})
	require.NoError(t, err)
	es := eventstore.NewEventStore(r, 3)

	id := uuid.New().String()
	acc := test.CreateAccount("Paulo", id, 100)
	acc.UpdateOwner("Paulo Quintans")
	acc.Deposit(10)
	acc.Deposit(20)
	err = es.Save(ctx, acc, eventstore.Options{})
	require.NoError(t, err)
	acc.Deposit(5)
	acc.Withdraw(15)
	acc.UpdateOwner("Paulo Quintans Pereira")
	err = es.Save(ctx, acc, eventstore.Options{})
	require.NoError(t, err)

	// giving time for the snapshots to write
	time.Sleep(100 * time.Millisecond)

	db, err := connect(dbURL)
	require.NoError(t, err)
	evts := []common.Json{}
	err = db.Select(&evts, "SELECT body FROM events WHERE aggregate_id = $1 and kind = 'OwnerUpdated'", id)
	require.NoError(t, err)
	assert.Equal(t, 2, len(evts))
	for _, v := range evts {
		ou := &test.OwnerUpdated{}
		err = json.Unmarshal(v, ou)
		require.NoError(t, err)
		assert.NotEmpty(t, ou.Owner)
	}

	bodies := []common.Json{}
	err = db.Select(&bodies, "SELECT body FROM snapshots WHERE aggregate_id = $1", id)
	require.NoError(t, err)
	assert.Equal(t, 2, len(bodies))
	for _, v := range bodies {
		a := test.NewAccount()
		err = json.Unmarshal(v, a)
		require.NoError(t, err)
		assert.NotEmpty(t, a.Owner)
	}

	err = es.Forget(ctx,
		eventstore.ForgetRequest{
			AggregateID: id,
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

	evts = []common.Json{}
	err = db.Select(&evts, "SELECT body FROM events WHERE aggregate_id = $1 and kind = 'OwnerUpdated'", id)
	require.NoError(t, err)
	assert.Equal(t, 2, len(evts))
	for _, v := range evts {
		ou := &test.OwnerUpdated{}
		err = json.Unmarshal(v, ou)
		require.NoError(t, err)
		assert.Empty(t, ou.Owner)
	}

	bodies = []common.Json{}
	err = db.Select(&bodies, "SELECT body FROM snapshots WHERE aggregate_id = $1", id)
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
	r, _ := postgresql.NewStore(dbURL, test.StructFactory{})
	es := eventstore.NewEventStore(r, 50)
	b.RunParallel(func(pb *testing.PB) {
		ctx := context.Background()
		id := uuid.New().String()
		acc := test.CreateAccount("Paulo", id, 0)

		for pb.Next() {
			acc.Deposit(10)
			_ = es.Save(ctx, acc, eventstore.Options{})
		}
	})
}
