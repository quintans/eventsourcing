package mongodb

import (
	"context"
	"time"

	"github.com/quintans/eventsourcing"
	"github.com/quintans/eventsourcing/eventid"
	"github.com/quintans/faults"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type pending struct {
	ID string `bson:"_id,omitempty"`
}

type Outbox struct {
	ID     string    `bson:"_id,omitempty"`
	DoneAt time.Time `bson:"done_at,omitempty"`
	Done   bool      `bson:"done"`
}

type OutboxRepository struct {
	Repository

	dbName         string
	collectionName string
}

func NewOutboxStore(client *mongo.Client, database string, collectionName string) *OutboxRepository {
	r := &OutboxRepository{
		Repository: Repository{
			client: client,
		},
		dbName:         database,
		collectionName: collectionName,
	}

	return r
}

func (r *OutboxRepository) GetPendingEvents(ctx context.Context, batchSize int) ([]*eventsourcing.Event, error) {
	opts := options.Find().SetSort(bson.D{{"_id", 1}})
	if batchSize > 0 {
		opts.SetBatchSize(int32(batchSize))
	} else {
		opts.SetBatchSize(-1)
	}

	flt := bson.D{
		{"done", bson.D{{"$eq", false}}},
	}

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

	opts = options.Find().SetSort(bson.D{{"_id", 1}})
	filter := append(flt, bson.E{"_id", bson.D{{"$in", ids}}})
	rows, err := r.queryEvents(ctx, filter, opts)
	if err != nil {
		return nil, faults.Errorf("Unable to get pending events for filter %+v: %w", filter, err)
	}
	if len(rows) == 0 {
		return nil, nil
	}

	return rows, nil
}

func (r *OutboxRepository) SetSinkData(ctx context.Context, evtID eventid.EventID) error {
	_, err := r.collection().UpdateByID(ctx, evtID.String(), bson.M{
		"$set": bson.M{
			"done_at": time.Now().UTC(),
			"done":    true,
		},
	})
	return faults.Wrapf(err, "setting done=true for event id '%s'", evtID)
}

func (r *OutboxRepository) collection() *mongo.Collection {
	return r.client.Database(r.dbName).Collection(r.collectionName)
}

func (r *OutboxRepository) queryEvents(ctx context.Context, filter bson.D, opts *options.FindOptions) ([]*eventsourcing.Event, error) {
	return queryEvents(ctx, r.collection(), filter, opts)
}

func OutboxInsertHandler(database, collName string) func(context.Context, ...*eventsourcing.Event) error {
	return func(ctx context.Context, events ...*eventsourcing.Event) error {
		sess := mongo.SessionFromContext(ctx)
		if sess == nil {
			return faults.Errorf("no session in context")
		}
		coll := sess.Client().Database(database).Collection(collName)
		for _, event := range events {
			_, err := coll.InsertOne(ctx, Outbox{
				ID:   event.ID.String(),
				Done: false,
			})
			if err != nil {
				return faults.Wrap(err)
			}
		}
		return nil
	}
}
