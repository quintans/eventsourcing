package repo

import (
	"context"
	"encoding/json"
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
	Body bson.M `bson:"body,omitempty"`
}

type MongoSnapshot struct {
	ID               string    `bson:"_id,omitempty"`
	AggregateID      string    `bson:"aggregate_id,omitempty"`
	AggregateVersion uint32    `bson:"aggregate_version,omitempty"`
	Body             bson.M    `bson:"body,omitempty"`
	CreatedAt        time.Time `bson:"created_at,omitempty"`
}

var _ eventstore.EsRepository = (*MongoEsRepository)(nil)

type MongoEsRepository struct {
	dbName string
	client *mongo.Client
}

func NewMongoEsRepository(connString string, dbName string) (*MongoEsRepository, error) {
	ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(connString))
	if err != nil {
		return nil, err
	}

	return NewMongoEsRepositoryDB(client, dbName), nil
}

// NewMongoEsRepositoryDB creates a new instance of MongoEsRepository
func NewMongoEsRepositoryDB(client *mongo.Client, dbName string) *MongoEsRepository {
	return &MongoEsRepository{
		dbName: dbName,
		client: client,
	}
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
		body := bson.M{}
		err := bson.UnmarshalExtJSON(e.Body, true, &body)
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

	_, err := r.eventsCollection().InsertOne(ctx, doc)
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

func (r *MongoEsRepository) GetSnapshot(ctx context.Context, aggregateID string) (eventstore.Snapshot, error) {
	// FIXME it should return eventstore.Snapshot because it forces json encoding on the caller. The encoding should belong only to the repository layer

	snap := MongoSnapshot{}
	opts := options.FindOne()
	opts.SetSort(bson.D{{"aggregate_version", -1}})
	if err := r.snapshotCollection().FindOne(ctx, bson.D{{"aggregate_id", aggregateID}}, opts).Decode(&snap); err != nil {
		if err == mongo.ErrNoDocuments {
			return eventstore.Snapshot{}, nil
		}
		return eventstore.Snapshot{}, fmt.Errorf("Unable to get snapshot for aggregate '%s': %w", aggregateID, err)
	}
	body, err := json.Marshal(snap.Body)
	if err != nil {
		return eventstore.Snapshot{}, err
	}
	return eventstore.Snapshot{
		AggregateID:      snap.AggregateID,
		AggregateVersion: snap.AggregateVersion,
		Body:             body,
	}, nil
}

func (r *MongoEsRepository) SaveSnapshot(ctx context.Context, aggregate eventstore.Aggregater, eventID string) error {
	bodyJson, err := json.Marshal(aggregate)
	if err != nil {
		return fmt.Errorf("Failed to create serialize snapshot: %w", err)
	}
	body := bson.M{}
	err = bson.UnmarshalExtJSON(bodyJson, true, &body)
	if err != nil {
		return err
	}

	snap := MongoSnapshot{
		ID:               eventID,
		AggregateID:      aggregate.GetID(),
		AggregateVersion: aggregate.GetVersion(),
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

func (r *MongoEsRepository) Forget(ctx context.Context, request eventstore.ForgetRequest) error {
	for _, evt := range request.Events {
		filter := bson.D{
			{"aggregate_id", bson.D{{"$eq", request.AggregateID}}},
			{"kind", bson.D{{"$eq", evt.Kind}}},
		}
		fields := bson.D{}
		for _, v := range evt.Fields {
			fields = append(fields, bson.E{"details.body." + v, ""})
		}
		update := bson.D{
			{"$unset", fields},
		}
		_, err := r.eventsCollection().UpdateMany(ctx, filter, update)
		if err != nil {
			return fmt.Errorf("Unable to forget events: %w", err)
		}
	}

	if len(request.AggregateFields) > 0 {
		filter := bson.D{
			{"aggregate_id", bson.D{{"$eq", request.AggregateID}}},
		}
		fields := bson.D{}
		for _, v := range request.AggregateFields {
			fields = append(fields, bson.E{"body." + v, ""})
		}
		update := bson.D{
			{"$unset", fields},
		}
		_, err := r.snapshotCollection().UpdateMany(ctx, filter, update)
		if err != nil {
			return fmt.Errorf("Unable to forget snapshots: %w", err)
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
		flt = append(flt, bson.E{"labels", bson.D{{"$in", filter.Labels}}})
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
				body, err := json.Marshal(d.Body)
				if err != nil {
					return nil, err
				}
				labels, err := bson.MarshalExtJSON(v.Labels, true, false)
				if err != nil {
					return nil, err
				}
				events = append(events, eventstore.Event{
					ID:               common.NewMessageID(v.ID, uint8(k)),
					AggregateID:      v.AggregateID,
					AggregateVersion: v.AggregateVersion,
					AggregateType:    v.AggregateType,
					Kind:             d.Kind,
					Body:             body,
					IdempotencyKey:   v.IdempotencyKey,
					Labels:           labels,
					CreatedAt:        v.CreatedAt,
				})
			}
		}
	}

	return events, nil
}
