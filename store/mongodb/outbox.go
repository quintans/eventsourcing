package mongodb

import (
	"context"
	"slices"

	"github.com/oklog/ulid/v2"
	"github.com/quintans/eventsourcing"
	"github.com/quintans/eventsourcing/eventid"
	"github.com/quintans/eventsourcing/sink/poller"
	"github.com/quintans/eventsourcing/store"
	"github.com/quintans/faults"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

var _ poller.Repository[*ulid.ULID] = (*OutboxRepository[*ulid.ULID])(nil)

type EventsRepository[K eventsourcing.ID] interface {
	GetEventsByRawIDs(context.Context, []string) ([]*eventsourcing.Event[K], error)
}

type pending struct {
	ID string `bson:"_id,omitempty"`
}

type Outbox struct {
	ID string `bson:"_id,omitempty"`
}

type OutboxRepository[K eventsourcing.ID] struct {
	Repository

	dbName         string
	collectionName string
	eventsRepo     EventsRepository[K]
}

func NewOutboxStore[K eventsourcing.ID](client *mongo.Client, database, collectionName string, eventsRepo EventsRepository[K]) (*OutboxRepository[K], error) {
	r := &OutboxRepository[K]{
		Repository: Repository{
			client: client,
		},
		dbName:         database,
		collectionName: collectionName,
		eventsRepo:     eventsRepo,
	}

	err := r.createSchema()
	if err != nil {
		return nil, faults.Wrap(err)
	}

	return r, nil
}

func (r *OutboxRepository[K]) PendingEvents(ctx context.Context, batchSize int) ([]*eventsourcing.Event[K], error) {
	opts := options.Find().SetSort(bson.D{{Key: "_id", Value: 1}})
	if batchSize > 0 {
		opts.SetBatchSize(int32(batchSize))
	} else {
		opts.SetBatchSize(-1)
	}

	cursor, err := r.collection().Find(ctx, bson.D{}, opts)
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

	rows, err := r.eventsRepo.GetEventsByRawIDs(ctx, ids)
	if err != nil {
		return nil, faults.Errorf("Unable to get pending events: %w", err)
	}
	if len(rows) == 0 {
		return nil, nil
	}

	return rows, nil
}

func (r *OutboxRepository[K]) AfterSink(ctx context.Context, evtID eventid.EventID) error {
	filter := bson.D{{Key: "_id", Value: bson.D{{Key: "$eq", Value: evtID.String()}}}}

	_, err := r.collection().DeleteOne(ctx, filter)
	return faults.Wrapf(err, "deleting from '%s' where id='%s'", r.collectionName, evtID)
}

func (r *OutboxRepository[K]) collection() *mongo.Collection {
	return r.client.Database(r.dbName).Collection(r.collectionName)
}

func OutboxInsertHandler[K eventsourcing.ID](database, collName string) store.InTxHandler[K] {
	return func(c *store.InTxHandlerContext[K]) error {
		ctx := c.Context()
		sess := mongo.SessionFromContext(ctx)
		if sess == nil {
			return faults.Errorf("no session in context")
		}

		event := c.Event()
		coll := sess.Client().Database(database).Collection(collName)
		_, err := coll.InsertOne(ctx, Outbox{
			ID: event.ID.String(),
		})
		return faults.Wrap(err)
	}
}

func (r *OutboxRepository[K]) createSchema() error {
	ctx := context.Background()
	db := r.client.Database(r.dbName)
	cNames, err := db.ListCollectionNames(ctx, bson.M{
		"name": bson.M{"$in": bson.A{r.collectionName}},
	})
	if err != nil {
		return faults.Wrapf(err, "getting collections")
	}

	cmds := []bson.D{}
	if !slices.Contains(cNames, r.collectionName) {
		cmds = append(cmds,
			bson.D{
				{Key: "create", Value: r.collectionName},
			},
		)
	}

	for _, c := range cmds {
		err := db.RunCommand(ctx, c).Err()
		if err != nil {
			return faults.Wrapf(err, "running command %+v", c)
		}
	}

	return nil
}
