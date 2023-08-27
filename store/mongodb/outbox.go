package mongodb

import (
	"context"

	"github.com/quintans/eventsourcing"
	"github.com/quintans/eventsourcing/eventid"
	"github.com/quintans/eventsourcing/sink/poller"
	"github.com/quintans/eventsourcing/store"
	"github.com/quintans/faults"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

var _ poller.Repository = (*OutboxRepository)(nil)

type EventsRepository interface {
	GetEventsByIDs(context.Context, []string) ([]*eventsourcing.Event, error)
}

type pending struct {
	ID string `bson:"_id,omitempty"`
}

type Outbox struct {
	ID              string             `bson:"_id,omitempty"`
	AggregateID     string             `bson:"aggregate_id,omitempty"`
	AggregateIDHash uint32             `bson:"aggregate_id_hash,omitempty"`
	AggregateKind   eventsourcing.Kind `bson:"aggregate_kind,omitempty"`
	Kind            eventsourcing.Kind `bson:"kind,omitempty"`
	Metadata        bson.M             `bson:"metadata,omitempty"`
}

type OutboxRepository struct {
	Repository

	dbName         string
	collectionName string
	eventsRepo     EventsRepository
}

func NewOutboxStore(client *mongo.Client, database, collectionName string, eventsRepo EventsRepository) *OutboxRepository {
	r := &OutboxRepository{
		Repository: Repository{
			client: client,
		},
		dbName:         database,
		collectionName: collectionName,
		eventsRepo:     eventsRepo,
	}

	return r
}

func (r *OutboxRepository) PendingEvents(ctx context.Context, batchSize int, filter store.Filter) ([]*eventsourcing.Event, error) {
	opts := options.Find().SetSort(bson.D{{"_id", 1}})
	if batchSize > 0 {
		opts.SetBatchSize(int32(batchSize))
	} else {
		opts.SetBatchSize(-1)
	}

	flt := buildFilter(filter, bson.D{})

	cursor, err := r.collection().Find(ctx, flt, opts)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return nil, nil
		}
		return nil, faults.Wrap(err)
	}

	pendings := []*pending{}
	if err = cursor.All(ctx, &pendings); err != nil {
		return nil, faults.Wrap(err)
	}
	ids := make([]string, len(pendings))
	for k, v := range pendings {
		ids[k] = v.ID
	}

	if len(ids) == 0 {
		return nil, nil
	}

	rows, err := r.eventsRepo.GetEventsByIDs(ctx, ids)
	if err != nil {
		return nil, faults.Errorf("Unable to get pending events for filter %+v: %w", filter, err)
	}
	if len(rows) == 0 {
		return nil, nil
	}

	return rows, nil
}

func (r *OutboxRepository) AfterSink(ctx context.Context, evtID eventid.EventID) error {
	filter := bson.D{{"_id", bson.D{{"$eq", evtID.String()}}}}

	_, err := r.collection().DeleteOne(ctx, filter)
	return faults.Wrapf(err, "deleting from '%s' where id='%s'", r.collectionName, evtID)
}

func (r *OutboxRepository) collection() *mongo.Collection {
	return r.client.Database(r.dbName).Collection(r.collectionName)
}

func OutboxInsertHandler(database, collName string) store.InTxHandler {
	return func(ctx context.Context, event *eventsourcing.Event) error {
		sess := mongo.SessionFromContext(ctx)
		if sess == nil {
			return faults.Errorf("no session in context")
		}
		m, err := event.Metadata.AsMap()
		if err != nil {
			return faults.Wrap(err)
		}
		coll := sess.Client().Database(database).Collection(collName)
		_, err = coll.InsertOne(ctx, Outbox{
			ID:              event.ID.String(),
			AggregateID:     event.AggregateID,
			AggregateIDHash: event.AggregateIDHash,
			AggregateKind:   event.AggregateKind,
			Kind:            event.Kind,
			Metadata:        m,
		})
		return faults.Wrap(err)
	}
}
