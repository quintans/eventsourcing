package repo

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/quintans/eventstore"
	"github.com/quintans/eventstore/common"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const (
	mongoUniqueViolation = 11000
	eventsCollection     = "events"
	snapshotCollection   = "snapshots"
)

// MongoEvent is the event data stored in the database
type MongoEvent struct {
	ID               string             `bson:"_id,omitempty"`
	AggregateID      string             `bson:"aggregate_id,omitempty"`
	AggregateVersion uint32             `bson:"aggregate_version,omitempty"`
	AggregateType    string             `bson:"aggregate_type,omitempty"`
	Details          []MongoEventDetail `bson:"details,omitempty"`
	IdempotencyKey   string             `bson:"idempotency_key,omitempty"`
	Labels           bson.M             `bson:"labels,omitempty"`
	CreatedAt        time.Time          `bson:"created_at,omitempty"`
}

type MongoEventDetail struct {
	Kind string `bson:"kind,omitempty"`
	Body []byte `bson:"body,omitempty"`
}

type MongoSnapshot struct {
	ID               string    `bson:"_id,omitempty"`
	AggregateID      string    `bson:"aggregate_id,omitempty"`
	AggregateVersion uint32    `bson:"aggregate_version,omitempty"`
	AggregateType    string    `bson:"aggregate_type,omitempty"`
	Body             []byte    `bson:"body,omitempty"`
	CreatedAt        time.Time `bson:"created_at,omitempty"`
}

var _ eventstore.EsRepository = (*MongoEsRepository)(nil)

type MgOption func(*MongoEsRepository)

func MgCodecOption(codec eventstore.Codec) MgOption {
	return func(r *MongoEsRepository) {
		r.codec = codec
	}
}

type MgProjectorFactory func(mongo.SessionContext) Projector

func MgProjectorFactoryOption(fn MgProjectorFactory) MgOption {
	return func(r *MongoEsRepository) {
		r.projectorFactory = fn
	}
}

type MongoEsRepository struct {
	dbName           string
	client           *mongo.Client
	factory          eventstore.Factory
	codec            eventstore.Codec
	projectorFactory MgProjectorFactory
}

func NewMongoEsRepository(connString string, dbName string, factory eventstore.Factory, opts ...MgOption) (*MongoEsRepository, error) {
	ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(connString))
	if err != nil {
		return nil, err
	}

	return NewMongoEsRepositoryDB(client, dbName, factory, opts...), nil
}

// NewMongoEsRepositoryDB creates a new instance of MongoEsRepository
func NewMongoEsRepositoryDB(client *mongo.Client, dbName string, factory eventstore.Factory, options ...MgOption) *MongoEsRepository {
	r := &MongoEsRepository{
		dbName:  dbName,
		client:  client,
		factory: factory,
		codec:   eventstore.JsonCodec{},
	}
	for _, o := range options {
		o(r)
	}

	return r
}

func (r *MongoEsRepository) collection(coll string) *mongo.Collection {
	return r.client.Database(r.dbName).Collection(coll)
}

func (r *MongoEsRepository) eventsCollection() *mongo.Collection {
	return r.collection(eventsCollection)
}

func (r *MongoEsRepository) snapshotCollection() *mongo.Collection {
	return r.collection(snapshotCollection)
}

func (r *MongoEsRepository) SaveEvent(ctx context.Context, eRec eventstore.EventRecord) (string, uint32, error) {
	if len(eRec.Details) == 0 {
		return "", 0, errors.New("No events to be saved")
	}
	details := make([]MongoEventDetail, 0, len(eRec.Details))
	for _, e := range eRec.Details {
		body, err := r.codec.Encode(e.Body)
		if err != nil {
			return "", 0, err
		}

		details = append(details, MongoEventDetail{
			Kind: e.Kind,
			Body: body,
		})
	}

	version := eRec.Version + 1
	id := common.NewEventID(eRec.CreatedAt, eRec.AggregateID, version)
	doc := MongoEvent{
		ID:               id,
		AggregateID:      eRec.AggregateID,
		AggregateType:    eRec.AggregateType,
		Details:          details,
		AggregateVersion: version,
		IdempotencyKey:   eRec.IdempotencyKey,
		Labels:           eRec.Labels,
		CreatedAt:        eRec.CreatedAt,
	}

	var err error
	if r.projectorFactory != nil {
		r.withTx(ctx, func(mCtx mongo.SessionContext) (interface{}, error) {
			res, err := r.eventsCollection().InsertOne(mCtx, doc)
			if err != nil {
				return nil, err
			}

			projector := r.projectorFactory(mCtx)
			for _, d := range doc.Details {
				evt := eventstore.Event{
					ID:               doc.ID,
					AggregateID:      doc.AggregateID,
					AggregateVersion: doc.AggregateVersion,
					AggregateType:    doc.AggregateType,
					Kind:             d.Kind,
					Body:             d.Body,
					Labels:           doc.Labels,
					CreatedAt:        doc.CreatedAt,
					Decode:           eventstore.EventDecoder(r.factory, r.codec, d.Kind, d.Body),
				}
				projector.Project(evt)
			}

			return res, nil
		})
	} else {
		_, err = r.eventsCollection().InsertOne(ctx, doc)
	}
	if err != nil {
		if isMongoDup(err) {
			return "", 0, eventstore.ErrConcurrentModification
		}
		return "", 0, fmt.Errorf("Unable to insert event: %w", err)
	}

	return id, version, nil

}

func isMongoDup(err error) bool {
	var e mongo.WriteException
	if errors.As(err, &e) {
		for _, we := range e.WriteErrors {
			if we.Code == mongoUniqueViolation {
				return true
			}
		}
	}
	return false
}

func (r *MongoEsRepository) withTx(ctx context.Context, callback func(mongo.SessionContext) (interface{}, error)) (err error) {
	session, err := r.client.StartSession()
	if err != nil {
		return err
	}
	defer session.EndSession(ctx)

	_, err = session.WithTransaction(ctx, callback)
	if err != nil {
		return err
	}

	return nil
}

func (r *MongoEsRepository) GetSnapshot(ctx context.Context, aggregateID string, aggregate eventstore.Aggregater) error {
	snap := MongoSnapshot{}
	opts := options.FindOne()
	opts.SetSort(bson.D{{"aggregate_version", -1}})
	if err := r.snapshotCollection().FindOne(ctx, bson.D{{"aggregate_id", aggregateID}}, opts).Decode(&snap); err != nil {
		if err == mongo.ErrNoDocuments {
			return nil
		}
		return fmt.Errorf("Unable to get snapshot for aggregate '%s': %w", aggregateID, err)
	}
	return r.codec.Decode(snap.Body, aggregate)
}

func (r *MongoEsRepository) SaveSnapshot(ctx context.Context, aggregate eventstore.Aggregater, eventID string) error {
	body, err := r.codec.Encode(aggregate)
	if err != nil {
		return fmt.Errorf("Failed to create serialize snapshot: %w", err)
	}
	snap := MongoSnapshot{
		ID:               eventID,
		AggregateID:      aggregate.GetID(),
		AggregateVersion: aggregate.GetVersion(),
		AggregateType:    aggregate.GetType(),
		Body:             body,
		CreatedAt:        time.Now().UTC(),
	}

	_, err = r.snapshotCollection().InsertOne(ctx, snap)

	return err
}

func (r *MongoEsRepository) GetAggregateEvents(ctx context.Context, aggregateID string, snapVersion int) ([]eventstore.Event, error) {
	filter := bson.D{
		{"aggregate_id", bson.D{{"$eq", aggregateID}}},
	}
	if snapVersion > -1 {
		filter = append(filter, bson.E{"aggregate_version", bson.D{{"$gt", snapVersion}}})
	}

	opts := options.Find()
	opts.SetSort(bson.D{{"aggregate_version", 1}})

	events, err := r.queryEvents(ctx, filter, opts, "", 0)
	if err != nil {
		return nil, fmt.Errorf("Unable to get events for Aggregate '%s': %w", aggregateID, err)
	}
	if len(events) == 0 {
		return nil, fmt.Errorf("Aggregate '%s' events were not found: %w", aggregateID, err)
	}

	return events, nil
}

func (r *MongoEsRepository) HasIdempotencyKey(ctx context.Context, aggregateID, idempotencyKey string) (bool, error) {
	filter := bson.D{{"idempotency_key", idempotencyKey}, {"aggregate_type", aggregateID}}
	opts := options.FindOne().SetProjection(bson.D{{"_id", 1}})
	evt := MongoEvent{}
	if err := r.eventsCollection().FindOne(ctx, filter, opts).Decode(&evt); err != nil {
		if err == mongo.ErrNoDocuments {
			return false, nil
		}
		return false, fmt.Errorf("Unable to verify the existence of the idempotency key: %w", err)
	}

	return true, nil
}

func (r *MongoEsRepository) Forget(ctx context.Context, request eventstore.ForgetRequest, forget func(interface{}) interface{}) error {
	// FIXME use a transaction.
	// FIXME in the end should also add a new event to the event store.

	// for events
	filter := bson.D{
		{"aggregate_id", bson.D{{"$eq", request.AggregateID}}},
		{"details.kind", bson.D{{"$eq", request.EventKind}}},
	}
	cursor, err := r.eventsCollection().Find(ctx, filter)
	if err != nil && err != mongo.ErrNoDocuments {
		return err
	}
	events := []MongoEvent{}
	if err = cursor.All(ctx, &events); err != nil {
		return fmt.Errorf("Unable to get events for Aggregate '%s' and event kind '%s': %w", request.AggregateID, request.EventKind, err)
	}
	for _, evt := range events {
		for k, d := range evt.Details {
			e, err := r.factory.New(d.Kind)
			if err != nil {
				return err
			}
			err = r.codec.Decode(d.Body, e)
			if err != nil {
				return err
			}
			e = common.Dereference(e)
			e = forget(e)
			body, err := r.codec.Encode(e)
			if err != nil {
				return err
			}

			filter := bson.D{
				{"_id", evt.ID},
			}
			update := bson.D{
				{"$set", bson.E{fmt.Sprintf("details.%d.body", k), body}},
			}
			_, err = r.eventsCollection().UpdateOne(ctx, filter, update)
			if err != nil {
				return fmt.Errorf("Unable to forget event ID %s: %w", evt.ID, err)
			}
		}
	}

	// for snapshots
	filter = bson.D{
		{"aggregate_id", bson.D{{"$eq", request.AggregateID}}},
	}
	cursor, err = r.snapshotCollection().Find(ctx, filter)
	if err != nil && err != mongo.ErrNoDocuments {
		return err
	}
	snaps := []MongoSnapshot{}
	if err = cursor.All(ctx, &snaps); err != nil {
		return fmt.Errorf("Unable to get snapshot for aggregate '%s': %w", request.AggregateID, err)
	}

	for _, s := range snaps {
		e, err := r.factory.New(s.AggregateType)
		if err != nil {
			return err
		}
		err = r.codec.Decode(s.Body, e)
		if err != nil {
			return err
		}
		e = common.Dereference(e)
		e = forget(e)
		body, err := r.codec.Encode(e)
		if err != nil {
			return err
		}

		filter := bson.D{
			{"_id", s.ID},
		}
		update := bson.M{
			"$set": bson.M{"body": body},
		}
		_, err = r.snapshotCollection().UpdateOne(ctx, filter, update)
		if err != nil {
			return fmt.Errorf("Unable to forget snapshot with ID %s: %w", s.ID, err)
		}
	}

	return nil
}

func (r *MongoEsRepository) GetLastEventID(ctx context.Context, trailingLag time.Duration, filter Filter) (string, error) {
	flt := bson.D{}

	if trailingLag != time.Duration(0) {
		safetyMargin := time.Now().UTC().Add(-trailingLag)
		flt = append(flt, bson.E{"created_at", bson.D{{"$lte", safetyMargin}}})
	}
	flt = writeMongoFilter(filter, flt)

	opts := options.FindOne().
		SetSort(bson.D{{"_id", -1}}).
		SetProjection(bson.D{{"_id", 1}})
	evt := MongoEvent{}
	if err := r.eventsCollection().FindOne(ctx, flt, opts).Decode(&evt); err != nil {
		if err == mongo.ErrNoDocuments {
			return "", nil
		}
		return "", fmt.Errorf("Unable to get the last event ID: %w", err)
	}

	return evt.ID, nil
}

func (r *MongoEsRepository) GetEvents(ctx context.Context, afterMessageID string, batchSize int, trailingLag time.Duration, filter Filter) ([]eventstore.Event, error) {
	eventID, count, err := common.SplitMessageID(afterMessageID)
	if err != nil {
		return nil, err
	}

	// since we have to consider the count, the query starts with the eventID
	flt := bson.D{
		{"_id", bson.D{{"$gte", eventID}}},
	}

	if trailingLag != time.Duration(0) {
		safetyMargin := time.Now().UTC().Add(-trailingLag)
		flt = append(flt, bson.E{"created_at", bson.D{{"$lte", safetyMargin}}})
	}
	flt = writeMongoFilter(filter, flt)

	opts := options.Find().SetSort(bson.D{{"_id", 1}})
	if batchSize > 0 {
		opts.SetBatchSize(int32(batchSize))
	} else {
		opts.SetBatchSize(-1)
	}

	rows, err := r.queryEvents(ctx, flt, opts, eventID, count)
	if err != nil {
		return nil, fmt.Errorf("Unable to get events after '%s' for filter %+v: %w", eventID, filter, err)
	}
	return rows, nil
}

func writeMongoFilter(filter Filter, flt bson.D) bson.D {
	if len(filter.AggregateTypes) > 0 {
		flt = append(flt, bson.E{"aggregate_type", bson.D{{"$in", filter.AggregateTypes}}})
	}
	if len(filter.Labels) > 0 {
		m := map[string][]string{}
		for _, v := range filter.Labels {
			lbls := m[v.Key]
			if lbls == nil {
				lbls = []string{}
			}
			lbls = append(lbls, v.Value)
			m[v.Key] = lbls
		}
		for k, v := range m {
			flt = append(flt, bson.E{"labels." + k, bson.D{{"$in", v}}})
		}
	}
	return flt
}

func (r *MongoEsRepository) queryEvents(ctx context.Context, filter bson.D, opts *options.FindOptions, afterEventID string, afterCount uint8) ([]eventstore.Event, error) {
	cursor, err := r.eventsCollection().Find(ctx, filter, opts)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return []eventstore.Event{}, nil
		}
		return nil, err
	}

	evts := []MongoEvent{}
	if err = cursor.All(ctx, &evts); err != nil {
		return nil, err
	}

	events := []eventstore.Event{}
	after := int(afterCount)
	for _, v := range evts {
		for k, d := range v.Details {
			// only collect events that are greater than afterEventID-afterCount
			if v.ID > afterEventID || k > after {
				events = append(events, eventstore.Event{
					ID:               common.NewMessageID(v.ID, uint8(k)),
					AggregateID:      v.AggregateID,
					AggregateVersion: v.AggregateVersion,
					AggregateType:    v.AggregateType,
					Kind:             d.Kind,
					Body:             d.Body,
					IdempotencyKey:   v.IdempotencyKey,
					Labels:           v.Labels,
					CreatedAt:        v.CreatedAt,
					Decode:           eventstore.EventDecoder(r.factory, r.codec, d.Kind, d.Body),
				})
			}
		}
	}

	return events, nil
}
