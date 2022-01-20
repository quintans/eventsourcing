package mongodb

import (
	"context"
	"errors"
	"time"

	"github.com/quintans/faults"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/quintans/eventsourcing"
	"github.com/quintans/eventsourcing/common"
	"github.com/quintans/eventsourcing/encoding"
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
	ID               string                      `bson:"_id,omitempty"`
	AggregateID      string                      `bson:"aggregate_id,omitempty"`
	AggregateIDHash  uint32                      `bson:"aggregate_id_hash,omitempty"`
	AggregateVersion uint32                      `bson:"aggregate_version,omitempty"`
	AggregateType    eventsourcing.AggregateType `bson:"aggregate_type,omitempty"`
	Kind             eventsourcing.EventKind     `bson:"kind,omitempty"`
	Body             []byte                      `bson:"body,omitempty"`
	IdempotencyKey   string                      `bson:"idempotency_key,omitempty"`
	Metadata         bson.M                      `bson:"metadata,omitempty"`
	CreatedAt        time.Time                   `bson:"created_at,omitempty"`
	Migrated         int                         `bson:"migrated"`
}

type Snapshot struct {
	ID               string                      `bson:"_id,omitempty"`
	AggregateID      string                      `bson:"aggregate_id,omitempty"`
	AggregateVersion uint32                      `bson:"aggregate_version,omitempty"`
	AggregateType    eventsourcing.AggregateType `bson:"aggregate_type,omitempty"`
	Body             []byte                      `bson:"body,omitempty"`
	CreatedAt        time.Time                   `bson:"created_at,omitempty"`
}

var _ eventsourcing.EsRepository = (*EsRepository)(nil)

type StoreOption func(*EsRepository)

type Listener func(mongo.SessionContext, eventsourcing.Event) error

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
	listeners               []Listener
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

// AddListener adds a listener to event sourcing events
func (r *EsRepository) AddListener(listener Listener) {
	r.listeners = append(r.listeners, listener)
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

func (r *EsRepository) SaveEvent(ctx context.Context, eRec eventsourcing.EventRecord) (eventid.EventID, uint32, error) {
	if len(eRec.Details) == 0 {
		return eventid.Zero, 0, faults.New("No events to be saved")
	}

	var id eventid.EventID
	version := eRec.Version
	idempotencyKey := eRec.IdempotencyKey
	err := r.withTx(ctx, func(mCtx mongo.SessionContext) error {
		entropy := eventid.NewEntropy()
		for _, e := range eRec.Details {
			version++
			var err error
			id, err = entropy.NewID(eRec.CreatedAt)
			if err != nil {
				return faults.Wrap(err)
			}
			err = r.saveEvent(
				mCtx,
				Event{
					ID:               id.String(),
					AggregateID:      eRec.AggregateID,
					AggregateIDHash:  common.Hash(eRec.AggregateID),
					AggregateType:    eRec.AggregateType,
					Kind:             e.Kind,
					Body:             e.Body,
					AggregateVersion: version,
					IdempotencyKey:   idempotencyKey,
					Metadata:         eRec.Metadata,
					CreatedAt:        eRec.CreatedAt,
				},
				id,
			)
			if err != nil {
				return err
			}
			// for a batch of events, the idempotency key is only applied on the first record
			idempotencyKey = ""
		}

		return nil
	})
	if err != nil {
		if isMongoDup(err) {
			return eventid.Zero, 0, faults.Wrap(eventsourcing.ErrConcurrentModification)
		}
		return eventid.Zero, 0, faults.Errorf("Unable to insert event: %w", err)
	}

	return id, version, nil
}

func (r *EsRepository) saveEvent(mCtx mongo.SessionContext, doc Event, id eventid.EventID) error {
	doc.ID = id.String()
	_, err := r.eventsCollection().InsertOne(mCtx, doc)
	if err != nil {
		return faults.Wrap(err)
	}

	if r.listeners != nil {
		evt := toEventsourcingEvent(doc, id)
		for _, listener := range r.listeners {
			err := listener(mCtx, evt)
			if err != nil {
				return err
			}
		}
	}
	return nil
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

func (r *EsRepository) withTx(ctx context.Context, callback func(mongo.SessionContext) error) (err error) {
	session, err := r.client.StartSession()
	if err != nil {
		return faults.Wrap(err)
	}
	defer session.EndSession(ctx)

	fn := func(sessCtx mongo.SessionContext) (interface{}, error) {
		err := callback(sessCtx)
		return nil, err
	}
	_, err = session.WithTransaction(ctx, fn)
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
		return eventsourcing.Snapshot{}, faults.Errorf("unable to get snapshot for aggregate '%s': %w", aggregateID, err)
	}
	id, err := eventid.Parse(snap.ID)
	if err != nil {
		return eventsourcing.Snapshot{}, faults.Errorf("unable to parse snapshot ID '%s': %w", snap.ID, err)
	}
	return eventsourcing.Snapshot{
		ID:               id,
		AggregateID:      aggregateID,
		AggregateVersion: snap.AggregateVersion,
		AggregateType:    eventsourcing.AggregateType(snap.AggregateType),
		Body:             snap.Body,
		CreatedAt:        snap.CreatedAt,
	}, nil
}

func (r *EsRepository) SaveSnapshot(ctx context.Context, snapshot eventsourcing.Snapshot) error {
	return r.saveSnapshot(ctx, Snapshot{
		ID:               snapshot.ID.String(),
		AggregateID:      snapshot.AggregateID,
		AggregateVersion: snapshot.AggregateVersion,
		AggregateType:    snapshot.AggregateType,
		Body:             snapshot.Body,
		CreatedAt:        snapshot.CreatedAt,
	})
}

func (r *EsRepository) saveSnapshot(ctx context.Context, snapshot Snapshot) error {
	// TODO instead of adding we could replace UPDATE/INSERT
	_, err := r.snapshotCollection().InsertOne(ctx, snapshot)

	return faults.Wrap(err)
}

func (r *EsRepository) GetAggregateEvents(ctx context.Context, aggregateID string, snapVersion int) ([]eventsourcing.Event, error) {
	filter := bson.D{
		{"aggregate_id", bson.D{{"$eq", aggregateID}}},
		{"migrated", bson.D{{"$eq", 0}}},
	}
	if snapVersion > -1 {
		filter = append(filter, bson.E{"aggregate_version", bson.D{{"$gt", snapVersion}}})
	}

	opts := options.Find()
	opts.SetSort(bson.D{{"aggregate_version", 1}})

	events, _, err := r.queryEvents(ctx, filter, opts)
	if err != nil {
		return nil, faults.Errorf("Unable to get events for Aggregate '%s': %w", aggregateID, err)
	}

	return events, nil
}

func (r *EsRepository) HasIdempotencyKey(ctx context.Context, idempotencyKey string) (bool, error) {
	filter := bson.D{
		{"idempotency_key", idempotencyKey},
		{"migrated", bson.D{{"$eq", 0}}},
	}
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

func (r *EsRepository) Forget(ctx context.Context, request eventsourcing.ForgetRequest, forget func(kind string, body []byte, snapshot bool) ([]byte, error)) error {
	// When Forget() is called, the aggregate is no longer used, therefore if it fails, it can be called again.

	// for events
	filter := bson.D{
		{"aggregate_id", bson.D{{"$eq", request.AggregateID}}},
		{"kind", bson.D{{"$eq", request.EventKind}}},
	}
	cursor, err := r.eventsCollection().Find(ctx, filter)
	if err != nil && err != mongo.ErrNoDocuments {
		return faults.Wrap(err)
	}
	events := []Event{}
	if err = cursor.All(ctx, &events); err != nil {
		return faults.Errorf("unable to get events for Aggregate '%s' and event kind '%s': %w", request.AggregateID, request.EventKind, err)
	}
	for _, evt := range events {
		body, err := forget(evt.Kind.String(), evt.Body, false)
		if err != nil {
			return err
		}

		filter := bson.D{
			{"_id", evt.ID},
		}
		update := bson.M{
			"$set": bson.M{"body": body},
		}
		_, err = r.eventsCollection().UpdateOne(ctx, filter, update)
		if err != nil {
			return faults.Errorf("Unable to forget event ID %s: %w", evt.ID, err)
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
		body, err := forget(s.AggregateType.String(), s.Body, true)
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

func (r *EsRepository) GetLastEventID(ctx context.Context, trailingLag time.Duration, filter store.Filter) (eventid.EventID, error) {
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
			return eventid.Zero, nil
		}
		return eventid.Zero, faults.Errorf("Unable to get the last event ID: %w", err)
	}

	eID, err := eventid.Parse(evt.ID)
	if err != nil {
		return eventid.Zero, err
	}

	return eID, nil
}

func (r *EsRepository) GetEvents(ctx context.Context, afterEventID eventid.EventID, batchSize int, trailingLag time.Duration, filter store.Filter) ([]eventsourcing.Event, error) {
	lastMessageID := afterEventID
	var records []eventsourcing.Event
	for len(records) < batchSize {
		flt := bson.D{
			{"_id", bson.D{{"$gt", lastMessageID.String()}}},
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

		rows, eID, err := r.queryEvents(ctx, flt, opts)
		if err != nil {
			return nil, faults.Errorf("Unable to get events after '%s' for filter %+v: %w", lastMessageID, filter, err)
		}
		if len(rows) == 0 {
			return records, nil
		}

		lastMessageID = eID
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

func (r *EsRepository) queryEvents(ctx context.Context, filter bson.D, opts *options.FindOptions) ([]eventsourcing.Event, eventid.EventID, error) {
	cursor, err := r.eventsCollection().Find(ctx, filter, opts)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return []eventsourcing.Event{}, eventid.Zero, nil
		}
		return nil, eventid.Zero, faults.Wrap(err)
	}

	evts := []Event{}
	if err = cursor.All(ctx, &evts); err != nil {
		return nil, eventid.Zero, faults.Wrap(err)
	}

	events := []eventsourcing.Event{}
	var lastEventID eventid.EventID
	for _, evt := range evts {
		lastEventID, err = eventid.Parse(evt.ID)
		if err != nil {
			return nil, eventid.Zero, faults.Errorf("unable to parse message ID '%s': %w", evt.ID, err)
		}
		events = append(events, toEventsourcingEvent(evt, lastEventID))
	}

	return events, lastEventID, nil
}

func toEventsourcingEvent(e Event, id eventid.EventID) eventsourcing.Event {
	return eventsourcing.Event{
		ID:               id,
		AggregateID:      e.AggregateID,
		AggregateIDHash:  e.AggregateIDHash,
		AggregateVersion: e.AggregateVersion,
		AggregateType:    e.AggregateType,
		IdempotencyKey:   e.IdempotencyKey,
		Kind:             e.Kind,
		Body:             e.Body,
		Metadata:         encoding.JsonOfMap(e.Metadata),
		CreatedAt:        e.CreatedAt,
	}
}
