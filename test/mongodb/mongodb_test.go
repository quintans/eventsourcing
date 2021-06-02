package mongodb

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/quintans/eventsourcing"
	"github.com/quintans/eventsourcing/log"
	"github.com/quintans/eventsourcing/player"
	"github.com/quintans/eventsourcing/store/mongodb"
	"github.com/quintans/eventsourcing/store/poller"
	"github.com/quintans/eventsourcing/test"
)

var (
	codec  = eventsourcing.JSONCodec{}
	logger = log.NewLogrus(logrus.StandardLogger())
)

const (
	AggregateAccount    eventsourcing.AggregateType = "Account"
	EventAccountCreated eventsourcing.EventKind     = "AccountCreated"
	EventMoneyDeposited eventsourcing.EventKind     = "MoneyDeposited"
	EventMoneyWithdrawn eventsourcing.EventKind     = "MoneyWithdrawn"
)

// creates a independent connection
func connect(dbConfig DBConfig) (*mongo.Database, error) {
	connString := fmt.Sprintf("mongodb://%s:%d/%s?replicaSet=rs0", dbConfig.Host, dbConfig.Port, dbConfig.Database)

	opts := options.Client().ApplyURI(connString)
	client, err := mongo.Connect(context.Background(), opts)
	if err != nil {
		return nil, err
	}
	db := client.Database(DBName)
	return db, nil
}

func TestSaveAndGet(t *testing.T) {
	dbConfig, tearDown, err := Setup("./docker-compose.yaml")
	require.NoError(t, err)
	defer tearDown()

	ctx := context.Background()
	r, err := mongodb.NewStore(dbConfig.Url(), dbConfig.Database)
	require.NoError(t, err)
	defer r.Close(context.Background())

	es := eventsourcing.NewEventStore(r, 3, test.AggregateFactory{})

	id := uuid.New()
	acc := test.CreateAccount("Paulo", id, 100)
	acc.Deposit(10)
	acc.Withdraw(5)
	err = es.Save(ctx, acc)
	require.NoError(t, err)
	acc.Deposit(5)
	err = es.Save(ctx, acc, eventsourcing.WithIdempotencyKey("idempotency-key"))
	require.NoError(t, err)

	// giving time for the snapshots to write
	time.Sleep(time.Second)

	evts, err := getEvents(ctx, dbConfig, id)
	require.NoError(t, err)

	require.Equal(t, 2, len(evts))
	evt := evts[0]
	assert.Equal(t, EventAccountCreated, evt.Details[0].Kind)
	assert.Equal(t, EventMoneyDeposited, evt.Details[1].Kind)
	assert.Equal(t, EventMoneyWithdrawn, evt.Details[2].Kind)
	assert.Equal(t, AggregateAccount, evts[0].AggregateType)
	assert.Equal(t, id.String(), evt.AggregateID)
	assert.Equal(t, uint32(1), evt.AggregateVersion)
	assert.Equal(t, "idempotency-key", evts[1].IdempotencyKey)

	a, err := es.GetByID(ctx, id.String())
	require.NoError(t, err)
	acc2 := a.(*test.Account)
	assert.Equal(t, id, acc2.ID)
	assert.Equal(t, uint32(2), acc2.GetVersion())
	assert.Equal(t, uint32(1), acc2.GetEventsCounter())
	assert.Equal(t, int64(110), acc2.Balance)
	assert.Equal(t, test.OPEN, acc2.Status)

	found, err := es.HasIdempotencyKey(ctx, AggregateAccount, "idempotency-key")
	require.NoError(t, err)
	require.True(t, found)

	acc.Deposit(5)
	err = es.Save(ctx, acc, eventsourcing.WithIdempotencyKey("idempotency-key"))
	require.Error(t, err)
}

func getEvents(ctx context.Context, dbConfig DBConfig, id uuid.UUID) ([]mongodb.Event, error) {
	db, err := connect(dbConfig)
	if err != nil {
		return nil, err
	}
	opts := options.Find().SetSort(bson.D{{"_id", 1}})
	cursor, err := db.Collection(CollEvents).Find(ctx, bson.M{
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

func TestPollListener(t *testing.T) {
	dbConfig, tearDown, err := Setup("./docker-compose.yaml")
	require.NoError(t, err)
	defer tearDown()

	ctx := context.Background()
	r, err := mongodb.NewStore(dbConfig.Url(), dbConfig.Database)
	require.NoError(t, err)
	defer r.Close(context.Background())
	es := eventsourcing.NewEventStore(r, 3, test.AggregateFactory{})

	id := uuid.New()
	acc := test.CreateAccount("Paulo", id, 100)
	acc.Deposit(10)
	acc.Withdraw(5)
	err = es.Save(ctx, acc)
	require.NoError(t, err)
	acc.Deposit(5)
	err = es.Save(ctx, acc)
	require.NoError(t, err)
	time.Sleep(time.Second)

	acc2 := test.NewAccount()
	counter := 0
	r, err = mongodb.NewStore(dbConfig.Url(), dbConfig.Database)
	require.NoError(t, err)
	defer r.Close(context.Background())
	lm := poller.New(logger, r)

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
	assert.Equal(t, uint32(2), acc2.GetVersion())
	assert.Equal(t, int64(110), acc2.Balance)
	assert.Equal(t, test.OPEN, acc2.Status)
}

func TestListenerWithAggregateType(t *testing.T) {
	dbConfig, tearDown, err := Setup("./docker-compose.yaml")
	require.NoError(t, err)
	defer tearDown()

	ctx := context.Background()
	r, err := mongodb.NewStore(dbConfig.Url(), dbConfig.Database)
	require.NoError(t, err)
	defer r.Close(context.Background())
	es := eventsourcing.NewEventStore(r, 3, test.AggregateFactory{})

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
	repository, err := mongodb.NewStore(dbConfig.Url(), dbConfig.Database)
	require.NoError(t, err)
	defer r.Close(context.Background())
	p := poller.New(logger, repository, poller.WithAggregateTypes(AggregateAccount))

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
	assert.Equal(t, uint32(2), acc2.GetVersion())
	assert.Equal(t, int64(135), acc2.Balance)
	assert.Equal(t, test.OPEN, acc2.Status)
}

func TestListenerWithLabels(t *testing.T) {
	dbConfig, tearDown, err := Setup("./docker-compose.yaml")
	require.NoError(t, err)
	defer tearDown()

	ctx := context.Background()
	r, err := mongodb.NewStore(dbConfig.Url(), dbConfig.Database)
	require.NoError(t, err)
	defer r.Close(context.Background())
	es := eventsourcing.NewEventStore(r, 3, test.AggregateFactory{})

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

	repository, err := mongodb.NewStore(dbConfig.Url(), dbConfig.Database)
	require.NoError(t, err)
	defer r.Close(context.Background())
	p := poller.New(logger, repository, poller.WithMetadataKV("geo", "EU"))

	done := make(chan struct{})
	go p.Poll(ctx, player.StartBeginning(), func(ctx context.Context, e eventsourcing.Event) error {
		if e.AggregateID == id.String() {
			if err := es.ApplyChangeFromHistory(acc2, e); err != nil {
				return err
			}
			counter++
			if counter == 3 {
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
	assert.Equal(t, 3, counter)
	assert.Equal(t, id, acc2.ID)
	assert.Equal(t, uint32(1), acc2.GetVersion())
	assert.Equal(t, int64(130), acc2.Balance)
	assert.Equal(t, test.OPEN, acc2.Status)
}

func TestForget(t *testing.T) {
	dbConfig, tearDown, err := Setup("./docker-compose.yaml")
	require.NoError(t, err)
	defer tearDown()

	ctx := context.Background()
	r, err := mongodb.NewStore(dbConfig.Url(), dbConfig.Database)
	require.NoError(t, err)
	defer r.Close(context.Background())
	es := eventsourcing.NewEventStore(r, 3, test.AggregateFactory{})

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
	cursor, err := db.Collection(CollEvents).Find(ctx, bson.M{
		"aggregate_id": bson.D{
			{"$eq", id.String()},
		},
		"details.kind": bson.D{
			{"$eq", "OwnerUpdated"},
		},
	})
	require.NoError(t, err)
	evts := []mongodb.Event{}
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

	cursor, err = db.Collection(CollSnapshots).Find(ctx, bson.M{
		"aggregate_id": bson.D{
			{"$eq", id.String()},
		},
	})
	require.NoError(t, err)
	snaps := []mongodb.Snapshot{}
	cursor.All(ctx, &snaps)
	assert.Equal(t, 2, len(snaps))
	for _, v := range snaps {
		snap := test.NewAccount()
		codec.Decode(v.Body, &snap)
		assert.NotEmpty(t, snap.Owner)
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

	time.Sleep(100 * time.Millisecond)

	cursor, err = db.Collection(CollEvents).Find(ctx, bson.M{
		"aggregate_id": bson.D{
			{"$eq", id.String()},
		},
		"details.kind": bson.D{
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

	cursor, err = db.Collection(CollSnapshots).Find(ctx, bson.M{
		"aggregate_id": bson.D{
			{"$eq", id.String()},
		},
	})
	require.NoError(t, err)
	snaps = []mongodb.Snapshot{}
	cursor.All(ctx, &snaps)
	assert.Equal(t, 2, len(snaps))
	for _, v := range snaps {
		snap := test.NewAccount()
		codec.Decode(v.Body, &snap)
		assert.Empty(t, snap.Owner)
		assert.NotEmpty(t, snap.ID)
	}
}
