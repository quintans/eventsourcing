package mongodb

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/quintans/eventstore"
	"github.com/quintans/eventstore/common"
	"github.com/quintans/eventstore/store"
	"github.com/quintans/toolkit/faults"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const (
	mongoUniqueViolation = 11000
	eventsCollection     = "events"
	snapshotCollection   = "snapshots"
)

// Event is the event data stored in the database
type Event struct {
	ID               string        `bson:"_id,omitempty"`
	AggregateID      string        `bson:"aggregate_id,omitempty"`
	AggregateIDHash  uint32        `bson:"aggregate_id_hash,omitempty"`
	AggregateVersion uint32        `bson:"aggregate_version,omitempty"`
	AggregateType    string        `bson:"aggregate_type,omitempty"`
	Details          []EventDetail `bson:"details,omitempty"`
	IdempotencyKey   string        `bson:"idempotency_key,omitempty"`
	Labels           bson.M        `bson:"labels,omitempty"`
	CreatedAt        time.Time     `bson:"created_at,omitempty"`
}

type EventDetail struct {
	Kind string `bson:"kind,omitempty"`
	Body []byte `bson:"body,omitempty"`
}

type Snapshot struct {
	ID               string    `bson:"_id,omitempty"`
	AggregateID      string    `bson:"aggregate_id,omitempty"`
	AggregateVersion uint32    `bson:"aggregate_version,omitempty"`
	AggregateType    string    `bson:"aggregate_type,omitempty"`
	Body             []byte    `bson:"body,omitempty"`
	CreatedAt        time.Time `bson:"created_at,omitempty"`
}

var _ eventstore.EsRepository = (*EsRepository)(nil)

type StoreOption func(*EsRepository)

type ProjectorFactory func(mongo.SessionContext) store.Projector

func WithProjectorFactory(fn ProjectorFactory) StoreOption {
	return func(r *EsRepository) {
		r.projectorFactory = fn
	}
}

type EsRepository struct {
	dbName           string
	client           *mongo.Client
	projectorFactory ProjectorFactory
}

func NewStore(connString string, dbName string, opts ...StoreOption) (*EsRepository, error) {
	ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(connString))
	if err != nil {
		return nil, err
	}

	return NewStoreDB(client, dbName, opts...), nil
}

// NewMongoEsRepositoryDB creates a new instance of MongoEsRepository
func NewStoreDB(client *mongo.Client, dbName string, options ...StoreOption) *EsRepository {
	r := &EsRepository{
		dbName: dbName,
		client: client,
	}
	for _, o := range options {
		o(r)
	}

	return r
}

func (r *EsRepository) collection(coll string) *mongo.Collection {
	return r.client.Database(r.dbName).Collection(coll)
}

func (r *EsRepository) eventsCollection() *mongo.Collection {
	return r.collection(eventsCollection)
}

func (r *EsRepository) snapshotCollection() *mongo.Collection {
	return r.collection(snapshotCollection)
}

func (r *EsRepository) SaveEvent(ctx context.Context, eRec eventstore.EventRecord) (string, uint32, error) {
	if len(eRec.Details) == 0 {
		return "", 0, faults.New("No events to be saved")
	}
	details := make([]EventDetail, 0, len(eRec.Details))
	for _, e := range eRec.Details {
		details = append(details, EventDetail{
			Kind: e.Kind,
			Body: e.Body,
		})
	}

	version := eRec.Version + 1
	id := common.NewEventID(eRec.CreatedAt, eRec.AggregateID, version)
	doc := Event{
		ID:               id,
		AggregateID:      eRec.AggregateID,
		AggregateType:    eRec.AggregateType,
		Details:          details,
		AggregateVersion: version,
		IdempotencyKey:   eRec.IdempotencyKey,
		Labels:           eRec.Labels,
		CreatedAt:        eRec.CreatedAt,
		AggregateIDHash:  common.Hash(eRec.AggregateID),
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
		return "", 0, faults.Errorf("Unable to insert event: %w", err)
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

func (r *EsRepository) withTx(ctx context.Context, callback func(mongo.SessionContext) (interface{}, error)) (err error) {
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

func (r *EsRepository) GetSnapshot(ctx context.Context, aggregateID string) (eventstore.Snapshot, error) {
	snap := Snapshot{}
	opts := options.FindOne()
	opts.SetSort(bson.D{{"aggregate_version", -1}})
	if err := r.snapshotCollection().FindOne(ctx, bson.D{{"aggregate_id", aggregateID}}, opts).Decode(&snap); err != nil {
		if err == mongo.ErrNoDocuments {
			return eventstore.Snapshot{}, nil
		}
		return eventstore.Snapshot{}, faults.Errorf("Unable to get snapshot for aggregate '%s': %w", aggregateID, err)
	}
	return eventstore.Snapshot{
		ID:               snap.ID,
		AggregateID:      snap.AggregateID,
		AggregateVersion: snap.AggregateVersion,
		AggregateType:    snap.AggregateType,
		Body:             snap.Body,
		CreatedAt:        snap.CreatedAt,
	}, nil
}

func (r *EsRepository) SaveSnapshot(ctx context.Context, snapshot eventstore.Snapshot) error {
	snap := Snapshot{
		ID:               snapshot.ID,
		AggregateID:      snapshot.AggregateID,
		AggregateVersion: snapshot.AggregateVersion,
		AggregateType:    snapshot.AggregateType,
		Body:             snapshot.Body,
		CreatedAt:        snapshot.CreatedAt,
	}
	_, err := r.snapshotCollection().InsertOne(ctx, snap)

	return err
}

func (r *EsRepository) GetAggregateEvents(ctx context.Context, aggregateID string, snapVersion int) ([]eventstore.Event, error) {
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
		return nil, faults.Errorf("Unable to get events for Aggregate '%s': %w", aggregateID, err)
	}
	if len(events) == 0 {
		return nil, faults.Errorf("Aggregate '%s' events were not found: %w", aggregateID, err)
	}

	return events, nil
}

func (r *EsRepository) HasIdempotencyKey(ctx context.Context, aggregateID, idempotencyKey string) (bool, error) {
	filter := bson.D{{"idempotency_key", idempotencyKey}, {"aggregate_type", aggregateID}}
	opts := options.FindOne().SetProjection(bson.D{{"_id", 1}})
	evt := Event{}
	if err := r.eventsCollection().FindOne(ctx, filter, opts).Decode(&evt); err != nil {
		if err == mongo.ErrNoDocuments {
			return false, nil
		}
		return false, faults.Errorf("Unable to verify the existence of the idempotency key: %w", err)
	}

	return true, nil
}

func (r *EsRepository) Forget(ctx context.Context, request eventstore.ForgetRequest, forget func(kind string, body []byte) ([]byte, error)) error {
	// When Forget() is called, the aggregate is no longer used, therefore if it fails, it can be called again.

	// for events
	filter := bson.D{
		{"aggregate_id", bson.D{{"$eq", request.AggregateID}}},
		{"details.kind", bson.D{{"$eq", request.EventKind}}},
	}
	cursor, err := r.eventsCollection().Find(ctx, filter)
	if err != nil && err != mongo.ErrNoDocuments {
		return err
	}
	events := []Event{}
	if err = cursor.All(ctx, &events); err != nil {
		return faults.Errorf("Unable to get events for Aggregate '%s' and event kind '%s': %w", request.AggregateID, request.EventKind, err)
	}
	for _, evt := range events {
		for k, d := range evt.Details {
			body, err := forget(d.Kind, d.Body)
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
				return faults.Errorf("Unable to forget event ID %s: %w", evt.ID, err)
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
	snaps := []Snapshot{}
	if err = cursor.All(ctx, &snaps); err != nil {
		return faults.Errorf("Unable to get snapshot for aggregate '%s': %w", request.AggregateID, err)
	}

	for _, s := range snaps {
		body, err := forget(s.AggregateType, s.Body)
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
			return faults.Errorf("Unable to forget snapshot with ID %s: %w", s.ID, err)
		}
	}

	return nil
}

func (r *EsRepository) GetLastEventID(ctx context.Context, trailingLag time.Duration, filter store.Filter) (string, error) {
	flt := bson.D{}

	if trailingLag != time.Duration(0) {
		safetyMargin := time.Now().UTC().Add(-trailingLag)
		flt = append(flt, bson.E{"created_at", bson.D{{"$lte", safetyMargin}}})
	}
	flt = buildFilter(filter, flt)

	opts := options.FindOne().
		SetSort(bson.D{{"_id", -1}}).
		SetProjection(bson.D{{"_id", 1}})
	evt := Event{}
	if err := r.eventsCollection().FindOne(ctx, flt, opts).Decode(&evt); err != nil {
		if err == mongo.ErrNoDocuments {
			return "", nil
		}
		return "", faults.Errorf("Unable to get the last event ID: %w", err)
	}

	return evt.ID, nil
}

func (r *EsRepository) GetEvents(ctx context.Context, afterMessageID string, batchSize int, trailingLag time.Duration, filter store.Filter) ([]eventstore.Event, error) {
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
	flt = buildFilter(filter, flt)

	opts := options.Find().SetSort(bson.D{{"_id", 1}})
	if batchSize > 0 {
		opts.SetBatchSize(int32(batchSize))
	} else {
		opts.SetBatchSize(-1)
	}

	rows, err := r.queryEvents(ctx, flt, opts, eventID, count)
	if err != nil {
		return nil, faults.Errorf("Unable to get events after '%s' for filter %+v: %w", eventID, filter, err)
	}
	return rows, nil
}

func buildFilter(filter store.Filter, flt bson.D) bson.D {
	if len(filter.AggregateTypes) > 0 {
		flt = append(flt, bson.E{"aggregate_type", bson.D{{"$in", filter.AggregateTypes}}})
	}

	flt = append(flt, partitionMatch("aggregate_id_hash", filter.Partitions, filter.PartitionsLow, filter.PartitionsHi)...)

	if len(filter.Labels) > 0 {
		for k, v := range filter.Labels {
			flt = append(flt, bson.E{"labels." + k, bson.D{{"$in", v}}})
		}
	}
	return flt
}

func (r *EsRepository) queryEvents(ctx context.Context, filter bson.D, opts *options.FindOptions, afterEventID string, afterCount uint8) ([]eventstore.Event, error) {
	cursor, err := r.eventsCollection().Find(ctx, filter, opts)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return []eventstore.Event{}, nil
		}
		return nil, err
	}

	evts := []Event{}
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
				})
			}
		}
	}

	return events, nil
}
