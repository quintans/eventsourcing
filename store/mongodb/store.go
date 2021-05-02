package mongodb

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/quintans/faults"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/quintans/eventsourcing"
	"github.com/quintans/eventsourcing/common"
	"github.com/quintans/eventsourcing/eventid"
	"github.com/quintans/eventsourcing/store"
)

const (
	mongoUniqueViolation       = 11000
	defaultEventsCollection    = "events"
	defaultSnapshotsCollection = "snapshots"
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
	Metadata         bson.M        `bson:"metadata,omitempty"`
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

var _ eventsourcing.EsRepository = (*EsRepository)(nil)

type StoreOption func(*EsRepository)

type ProjectorFactory func(mongo.SessionContext) store.Projector

func WithProjectorFactory(fn ProjectorFactory) StoreOption {
	return func(r *EsRepository) {
		r.projectorFactory = fn
	}
}

func WithEventsCollection(eventsCollection string) StoreOption {
	return func(r *EsRepository) {
		r.eventsCollectionName = eventsCollection
	}
}

func WithSnapshotsCollection(snapshotsCollection string) StoreOption {
	return func(r *EsRepository) {
		r.snapshotsCollectionName = snapshotsCollection
	}
}

type EsRepository struct {
	dbName                  string
	client                  *mongo.Client
	projectorFactory        ProjectorFactory
	eventsCollectionName    string
	snapshotsCollectionName string
}

// NewStore creates a new instance of MongoEsRepository
func NewStore(connString, database string, opts ...StoreOption) (*EsRepository, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	client, err := mongo.Connect(ctx, options.Client().ApplyURI(connString))
	if err != nil {
		return nil, faults.Wrap(err)
	}

	r := &EsRepository{
		dbName:                  database,
		client:                  client,
		eventsCollectionName:    defaultEventsCollection,
		snapshotsCollectionName: defaultSnapshotsCollection,
	}

	for _, o := range opts {
		o(r)
	}

	return r, nil
}

func (r *EsRepository) Close(ctx context.Context) {
	r.client.Disconnect(ctx)
}

func (r *EsRepository) collection(coll string) *mongo.Collection {
	return r.client.Database(r.dbName).Collection(coll)
}

func (r *EsRepository) eventsCollection() *mongo.Collection {
	return r.collection(r.eventsCollectionName)
}

func (r *EsRepository) snapshotCollection() *mongo.Collection {
	return r.collection(r.snapshotsCollectionName)
}

func (r *EsRepository) SaveEvent(ctx context.Context, eRec eventsourcing.EventRecord) (string, uint32, error) {
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
	id := eventid.NewEventID(eRec.CreatedAt, eRec.AggregateID, version)
	doc := Event{
		ID:               id,
		AggregateID:      eRec.AggregateID,
		AggregateType:    eRec.AggregateType,
		Details:          details,
		AggregateVersion: version,
		IdempotencyKey:   eRec.IdempotencyKey,
		Metadata:         eRec.Labels,
		CreatedAt:        eRec.CreatedAt,
		AggregateIDHash:  common.Hash(eRec.AggregateID),
	}

	var err error
	if r.projectorFactory != nil {
		r.withTx(ctx, func(mCtx mongo.SessionContext) (interface{}, error) {
			res, err := r.eventsCollection().InsertOne(mCtx, doc)
			if err != nil {
				return nil, faults.Wrap(err)
			}

			projector := r.projectorFactory(mCtx)
			for _, d := range doc.Details {
				evt := eventsourcing.Event{
					ID:               doc.ID,
					AggregateID:      doc.AggregateID,
					AggregateIDHash:  doc.AggregateIDHash,
					AggregateVersion: doc.AggregateVersion,
					AggregateType:    doc.AggregateType,
					IdempotencyKey:   doc.IdempotencyKey,
					Kind:             d.Kind,
					Body:             d.Body,
					Metadata:         doc.Metadata,
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
			return "", 0, eventsourcing.ErrConcurrentModification
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
		return faults.Wrap(err)
	}
	defer session.EndSession(ctx)

	_, err = session.WithTransaction(ctx, callback)
	if err != nil {
		return faults.Wrap(err)
	}

	return nil
}

func (r *EsRepository) GetSnapshot(ctx context.Context, aggregateID string) (eventsourcing.Snapshot, error) {
	snap := Snapshot{}
	opts := options.FindOne()
	opts.SetSort(bson.D{{"aggregate_version", -1}})
	if err := r.snapshotCollection().FindOne(ctx, bson.D{{"aggregate_id", aggregateID}}, opts).Decode(&snap); err != nil {
		if err == mongo.ErrNoDocuments {
			return eventsourcing.Snapshot{}, nil
		}
		return eventsourcing.Snapshot{}, faults.Errorf("Unable to get snapshot for aggregate '%s': %w", aggregateID, err)
	}
	return eventsourcing.Snapshot{
		ID:               snap.ID,
		AggregateID:      snap.AggregateID,
		AggregateVersion: snap.AggregateVersion,
		AggregateType:    snap.AggregateType,
		Body:             snap.Body,
		CreatedAt:        snap.CreatedAt,
	}, nil
}

func (r *EsRepository) SaveSnapshot(ctx context.Context, snapshot eventsourcing.Snapshot) error {
	snap := Snapshot{
		ID:               snapshot.ID,
		AggregateID:      snapshot.AggregateID,
		AggregateVersion: snapshot.AggregateVersion,
		AggregateType:    snapshot.AggregateType,
		Body:             snapshot.Body,
		CreatedAt:        snapshot.CreatedAt,
	}
	_, err := r.snapshotCollection().InsertOne(ctx, snap)

	return faults.Wrap(err)
}

func (r *EsRepository) GetAggregateEvents(ctx context.Context, aggregateID string, snapVersion int) ([]eventsourcing.Event, error) {
	filter := bson.D{
		{"aggregate_id", bson.D{{"$eq", aggregateID}}},
	}
	if snapVersion > -1 {
		filter = append(filter, bson.E{"aggregate_version", bson.D{{"$gt", snapVersion}}})
	}

	opts := options.Find()
	opts.SetSort(bson.D{{"aggregate_version", 1}})

	events, _, _, err := r.queryEvents(ctx, filter, opts, "", 0)
	if err != nil {
		return nil, faults.Errorf("Unable to get events for Aggregate '%s': %w", aggregateID, err)
	}

	return events, nil
}

func (r *EsRepository) HasIdempotencyKey(ctx context.Context, aggregateType, idempotencyKey string) (bool, error) {
	filter := bson.D{{"aggregate_type", aggregateType}, {"idempotency_key", idempotencyKey}}
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

func (r *EsRepository) Forget(ctx context.Context, request eventsourcing.ForgetRequest, forget func(kind string, body []byte) ([]byte, error)) error {
	// When Forget() is called, the aggregate is no longer used, therefore if it fails, it can be called again.

	// for events
	filter := bson.D{
		{"aggregate_id", bson.D{{"$eq", request.AggregateID}}},
		{"details.kind", bson.D{{"$eq", request.EventKind}}},
	}
	cursor, err := r.eventsCollection().Find(ctx, filter)
	if err != nil && err != mongo.ErrNoDocuments {
		return faults.Wrap(err)
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
		return faults.Wrap(err)
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

func (r *EsRepository) GetEvents(ctx context.Context, afterMessageID string, batchSize int, trailingLag time.Duration, filter store.Filter) ([]eventsourcing.Event, error) {
	eventID, count, err := common.SplitMessageID(afterMessageID)
	if err != nil {
		return nil, err
	}

	var records []eventsourcing.Event
	for len(records) < batchSize {
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

		rows, lastEventID, lastCount, err := r.queryEvents(ctx, flt, opts, eventID, count)
		if err != nil {
			return nil, faults.Errorf("Unable to get events after '%s' for filter %+v: %w", eventID, filter, err)
		}
		if len(rows) == 0 {
			return records, nil
		}

		eventID = lastEventID
		count = lastCount
		records = rows
	}

	return records, nil
}

func buildFilter(filter store.Filter, flt bson.D) bson.D {
	if len(filter.AggregateTypes) > 0 {
		flt = append(flt, bson.E{"aggregate_type", bson.D{{"$in", filter.AggregateTypes}}})
	}

	if filter.Partitions > 1 {
		flt = append(flt, partitionFilter("aggregate_id_hash", filter.Partitions, filter.PartitionLow, filter.PartitionHi))
	}

	if len(filter.Metadata) > 0 {
		for k, v := range filter.Metadata {
			flt = append(flt, bson.E{"metadata." + k, bson.D{{"$in", v}}})
		}
	}
	return flt
}

func partitionFilter(field string, partitions, partitionsLow, partitionsHi uint32) bson.E {
	field = "$" + field
	// aggregate: { $expr: {"$eq": [{"$mod" : [$field, m.partitions]}],  m.partitionsLow - 1]} }
	if partitionsLow == partitionsHi {
		return bson.E{
			"$expr",
			bson.D{
				{"$eq", bson.A{
					bson.D{
						{"$mod", bson.A{field, partitions}},
					},
					partitionsLow - 1,
				}},
			},
		}
	}

	// {$expr: {$and: [{"$gte": [ { "$mod" : [$field, m.partitions] }, m.partitionsLow - 1 ]}, {$lte: [ { $mod : [$field, m.partitions] }, partitionsHi - 1 ]}  ] }});
	return bson.E{
		"$expr",
		bson.D{
			{"$and", bson.A{
				bson.D{
					{"$gte", bson.A{
						bson.D{
							{"$mod", bson.A{field, partitions}},
						},
						partitionsLow - 1,
					}},
				},
				bson.D{
					{"$lte", bson.A{
						bson.D{
							{"$mod", bson.A{field, partitions}},
						},
						partitionsHi - 1,
					}},
				},
			}},
		},
	}
}

func (r *EsRepository) queryEvents(ctx context.Context, filter bson.D, opts *options.FindOptions, afterEventID string, afterCount uint8) ([]eventsourcing.Event, string, uint8, error) {
	cursor, err := r.eventsCollection().Find(ctx, filter, opts)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return []eventsourcing.Event{}, "", 0, nil
		}
		return nil, "", 0, faults.Wrap(err)
	}

	evts := []Event{}
	if err = cursor.All(ctx, &evts); err != nil {
		return nil, "", 0, faults.Wrap(err)
	}

	events := []eventsourcing.Event{}
	after := int(afterCount)
	var lastEventID string
	var lastCount uint8
	for _, v := range evts {
		for k, d := range v.Details {
			// only collect events that are greater than afterEventID-afterCount
			if v.ID > afterEventID || k > after {
				lastEventID = v.ID
				lastCount = uint8(k)
				events = append(events, eventsourcing.Event{
					ID:               common.NewMessageID(lastEventID, lastCount),
					AggregateID:      v.AggregateID,
					AggregateIDHash:  v.AggregateIDHash,
					AggregateVersion: v.AggregateVersion,
					AggregateType:    v.AggregateType,
					Kind:             d.Kind,
					Body:             d.Body,
					IdempotencyKey:   v.IdempotencyKey,
					Metadata:         v.Metadata,
					CreatedAt:        v.CreatedAt,
				})
			}
		}
	}

	return events, lastEventID, lastCount, nil
}
