package mongodb

import (
	"context"
	"log"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/quintans/eventstore"
	"github.com/quintans/eventstore/feed/poller"
	"github.com/quintans/eventstore/player"
	"github.com/quintans/eventstore/repo"
	"github.com/quintans/eventstore/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

var codec = eventstore.JsonCodec{}

// creates a independent connection
func connect() (*mongo.Database, error) {
	ctx := context.Background()
	opts := options.Client().ApplyURI(dbURL)
	client, err := mongo.Connect(ctx, opts)
	if err != nil {
		return nil, err
	}
	db := client.Database(dbName)
	return db, nil
}

func TestSaveAndGet(t *testing.T) {
	ctx := context.Background()
	r := repo.NewMongoEsRepositoryDB(client, dbName, test.StructFactory{})
	es := eventstore.NewEventStore(r, 3)

	id := uuid.New().String()
	acc := test.CreateAccount("Paulo", id, 100)
	acc.Deposit(10)
	acc.Deposit(20)
	err := es.Save(ctx, acc, eventstore.Options{})
	require.NoError(t, err)
	acc.Deposit(5)
	err = es.Save(ctx, acc, eventstore.Options{})
	require.NoError(t, err)

	// giving time for the snapshots to write
	time.Sleep(time.Second)

	db, err := connect()
	require.NoError(t, err)

	count, err := db.Collection(collSnapshots).CountDocuments(ctx, bson.M{
		"aggregate_id": bson.D{
			{"$eq", id},
		},
	})
	require.NoError(t, err)
	require.Equal(t, int64(1), count)

	opts := options.Find().SetSort(bson.D{{"_id", 1}})
	cursor, err := db.Collection(collEvents).Find(ctx, bson.M{
		"aggregate_id": bson.D{
			{"$eq", id},
		},
	}, opts)
	require.NoError(t, err)
	evts := []repo.MongoEvent{}
	err = cursor.All(ctx, &evts)
	require.NoError(t, err)

	require.Equal(t, 2, len(evts))
	evt := evts[0]
	assert.Equal(t, "AccountCreated", evt.Details[0].Kind)
	assert.Equal(t, "MoneyDeposited", evt.Details[1].Kind)
	assert.Equal(t, "MoneyDeposited", evt.Details[2].Kind)
	assert.Equal(t, "Account", evts[0].AggregateType)
	assert.Equal(t, id, evt.AggregateID)
	assert.Equal(t, uint32(1), evt.AggregateVersion)

	acc2 := test.NewAccount()
	err = es.GetByID(ctx, id, acc2)
	require.NoError(t, err)
	assert.Equal(t, id, acc2.ID)
	assert.Equal(t, uint32(2), acc2.Version)
	assert.Equal(t, int64(135), acc2.Balance)
	assert.Equal(t, test.OPEN, acc2.Status)
	assert.Equal(t, uint32(4), acc2.GetEventsCounter())
}

func TestPollListener(t *testing.T) {
	ctx := context.Background()
	r := repo.NewMongoEsRepositoryDB(client, dbName, test.StructFactory{})
	es := eventstore.NewEventStore(r, 3)

	id := uuid.New().String()
	acc := test.CreateAccount("Paulo", id, 100)
	acc.Deposit(10)
	acc.Withdraw(5)
	err := es.Save(ctx, acc, eventstore.Options{})
	require.NoError(t, err)
	acc.Deposit(5)
	err = es.Save(ctx, acc, eventstore.Options{})
	require.NoError(t, err)
	time.Sleep(time.Second)

	acc2 := test.NewAccount()
	counter := 0
	r = repo.NewMongoEsRepositoryDB(client, dbName, test.StructFactory{})
	lm := poller.New(r)

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
	assert.Equal(t, uint32(2), acc2.Version)
	assert.Equal(t, int64(110), acc2.Balance)
	assert.Equal(t, test.OPEN, acc2.Status)
}

func TestListenerWithAggregateType(t *testing.T) {
	ctx := context.Background()
	r := repo.NewMongoEsRepositoryDB(client, dbName, test.StructFactory{})
	es := eventstore.NewEventStore(r, 3)

	id := uuid.New().String()
	acc := test.CreateAccount("Paulo", id, 100)
	acc.Deposit(10)
	acc.Deposit(20)
	err := es.Save(ctx, acc, eventstore.Options{})
	require.NoError(t, err)
	acc.Deposit(5)
	err = es.Save(ctx, acc, eventstore.Options{})
	require.NoError(t, err)
	time.Sleep(time.Second)

	acc2 := test.NewAccount()
	counter := 0
	repository := repo.NewMongoEsRepositoryDB(client, dbName, test.StructFactory{})
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
	}, repo.WithAggregateTypes("Account"))

	select {
	case <-done:
		log.Println("Done...")
	case <-time.After(time.Second):
		log.Println("Timeout...")
	}
	assert.Equal(t, 4, counter)
	assert.Equal(t, id, acc2.ID)
	assert.Equal(t, uint32(2), acc2.Version)
	assert.Equal(t, int64(135), acc2.Balance)
	assert.Equal(t, test.OPEN, acc2.Status)
}

func TestListenerWithLabels(t *testing.T) {
	ctx := context.Background()
	r := repo.NewMongoEsRepositoryDB(client, dbName, test.StructFactory{})
	es := eventstore.NewEventStore(r, 3)

	id := uuid.New().String()
	acc := test.CreateAccount("Paulo", id, 100)
	acc.Deposit(10)
	acc.Deposit(20)
	err := es.Save(ctx, acc, eventstore.Options{
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

	repository := repo.NewMongoEsRepositoryDB(client, dbName, test.StructFactory{})
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
	}, repo.WithLabel("geo", "EU"))

	select {
	case <-done:
		log.Println("Done...")
	case <-time.After(time.Second):
		log.Println("Timeout...")
	}
	assert.Equal(t, 3, counter)
	assert.Equal(t, id, acc2.ID)
	assert.Equal(t, uint32(1), acc2.Version)
	assert.Equal(t, int64(130), acc2.Balance)
	assert.Equal(t, test.OPEN, acc2.Status)
}

func TestForget(t *testing.T) {
	ctx := context.Background()
	r := repo.NewMongoEsRepositoryDB(client, dbName, test.StructFactory{})
	es := eventstore.NewEventStore(r, 3)

	id := uuid.New().String()
	acc := test.CreateAccount("Paulo", id, 100)
	acc.UpdateOwner("Paulo Quintans")
	acc.Deposit(10)
	acc.Deposit(20)
	err := es.Save(ctx, acc, eventstore.Options{})
	require.NoError(t, err)
	acc.Deposit(5)
	acc.Withdraw(15)
	acc.UpdateOwner("Paulo Quintans Pereira")
	err = es.Save(ctx, acc, eventstore.Options{})
	require.NoError(t, err)

	// giving time for the snapshots to write
	time.Sleep(100 * time.Millisecond)

	db, err := connect()
	cursor, err := db.Collection(collEvents).Find(ctx, bson.M{
		"aggregate_id": bson.D{
			{"$eq", id},
		},
		"details.kind": bson.D{
			{"$eq", "OwnerUpdated"},
		},
	})
	require.NoError(t, err)
	evts := []repo.MongoEvent{}
	err = cursor.All(ctx, &evts)
	require.NoError(t, err)
	assert.Equal(t, 2, len(evts))
	foundEvent := false
	for _, e := range evts {
		for _, v := range e.Details {
			if v.Kind == "OwnerUpdated" {
				foundEvent = true
				evt := test.OwnerUpdated{}
				codec.Decode(v.Body, &evt)
				assert.NotEmpty(t, evt.Owner)
			}
		}
	}
	assert.True(t, foundEvent)

	cursor, err = db.Collection(collSnapshots).Find(ctx, bson.M{
		"aggregate_id": bson.D{
			{"$eq", id},
		},
	})
	require.NoError(t, err)
	snaps := []repo.MongoSnapshot{}
	cursor.All(ctx, &snaps)
	assert.Equal(t, 2, len(snaps))
	for _, v := range snaps {
		snap := test.Account{}
		codec.Decode(v.Body, &snap)
		assert.NotEmpty(t, snap.Owner)
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

	time.Sleep(100 * time.Millisecond)

	cursor, err = db.Collection(collEvents).Find(ctx, bson.M{
		"aggregate_id": bson.D{
			{"$eq", id},
		},
		"details.kind": bson.D{
			{"$eq", "OwnerUpdated"},
		},
	})
	require.NoError(t, err)
	evts = []repo.MongoEvent{}
	err = cursor.All(ctx, &evts)
	require.NoError(t, err)
	assert.Equal(t, 2, len(evts))
	foundEvent = false
	for _, e := range evts {
		for _, v := range e.Details {
			if v.Kind == "OwnerUpdated" {
				foundEvent = true
				evt := test.OwnerUpdated{}
				codec.Decode(v.Body, &evt)
				assert.NotEmpty(t, evt.Owner)
			}
		}
	}
	assert.True(t, foundEvent)

	cursor, err = db.Collection(collSnapshots).Find(ctx, bson.M{
		"aggregate_id": bson.D{
			{"$eq", id},
		},
	})
	require.NoError(t, err)
	snaps = []repo.MongoSnapshot{}
	cursor.All(ctx, &snaps)
	assert.Equal(t, 2, len(snaps))
	for _, v := range snaps {
		snap := test.Account{}
		codec.Decode(v.Body, &snap)
		assert.Empty(t, snap.Owner)
		assert.NotEmpty(t, snap.ID)
	}
}
