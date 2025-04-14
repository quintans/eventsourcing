package mongodb

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/avast/retry-go/v3"
	"github.com/oklog/ulid/v2"
	"github.com/quintans/faults"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"go.mongodb.org/mongo-driver/mongo/writeconcern"

	"github.com/quintans/eventsourcing"
	"github.com/quintans/eventsourcing/eventid"
	"github.com/quintans/eventsourcing/projection"
	"github.com/quintans/eventsourcing/store"
	"github.com/quintans/eventsourcing/util"
)

const (
	mongoUniqueViolation       = 11000
	defaultEventsCollection    = "events"
	defaultSnapshotsCollection = "snapshots"
)

type InTxHandler[K eventsourcing.ID] func(*InTxHandlerContext[K]) error

type InTxHandlerContext[K eventsourcing.ID] struct {
	ctx   context.Context
	event *eventsourcing.Event[K]
}

func NewInTxHandlerContext[K eventsourcing.ID](ctx context.Context, event *eventsourcing.Event[K]) *InTxHandlerContext[K] {
	return &InTxHandlerContext[K]{
		ctx:   ctx,
		event: event,
	}
}

func (c *InTxHandlerContext[K]) Context() context.Context {
	return c.ctx
}

func (c *InTxHandlerContext[K]) Event() *eventsourcing.Event[K] {
	return c.event
}

// Event is the event data stored in the database
type Event struct {
	ID               string             `bson:"_id,omitempty"`
	AggregateID      string             `bson:"aggregate_id,omitempty"`
	AggregateIDHash  int32              `bson:"aggregate_id_hash,omitempty"`
	AggregateVersion uint32             `bson:"aggregate_version,omitempty"`
	AggregateKind    eventsourcing.Kind `bson:"aggregate_kind,omitempty"`
	Kind             eventsourcing.Kind `bson:"kind,omitempty"`
	Body             []byte             `bson:"body,omitempty"`
	Discriminator    bson.M             `bson:"discriminator,omitempty"`
	CreatedAt        time.Time          `bson:"created_at,omitempty"`
	Migration        int                `bson:"migration"`
	Migrated         bool               `bson:"migrated"`
}

type Snapshot struct {
	ID               string             `bson:"_id,omitempty"`
	AggregateID      string             `bson:"aggregate_id,omitempty"`
	AggregateVersion uint32             `bson:"aggregate_version,omitempty"`
	AggregateKind    eventsourcing.Kind `bson:"aggregate_kind,omitempty"`
	Body             []byte             `bson:"body,omitempty"`
	CreatedAt        time.Time          `bson:"created_at,omitempty"`
	Discriminator    bson.M
}

var _ eventsourcing.EsRepository[ulid.ULID] = (*EsRepository[ulid.ULID, *ulid.ULID])(nil)

type Option[K eventsourcing.ID, PK eventsourcing.IDPt[K]] func(f *EsRepository[K, PK])

func WithEventsCollection[K eventsourcing.ID, PK eventsourcing.IDPt[K]](eventsCollection string) Option[K, PK] {
	return func(r *EsRepository[K, PK]) {
		r.eventsCollectionName = eventsCollection
	}
}

func WithSnapshotsCollection[K eventsourcing.ID, PK eventsourcing.IDPt[K]](snapshotsCollection string) Option[K, PK] {
	return func(r *EsRepository[K, PK]) {
		r.snapshotsCollectionName = snapshotsCollection
	}
}

func WithTxHandler[K eventsourcing.ID, PK eventsourcing.IDPt[K]](txHandler store.InTxHandler[K]) Option[K, PK] {
	return func(r *EsRepository[K, PK]) {
		r.txHandlers = append(r.txHandlers, txHandler)
	}
}

func WithDiscriminatorKeys[K eventsourcing.ID, PK eventsourcing.IDPt[K]](keys ...string) Option[K, PK] {
	return func(r *EsRepository[K, PK]) {
		for _, v := range keys {
			r.allowedKeys[v] = struct{}{}
		}
	}
}

// WithDiscriminator defines the discriminator to be save on every event. Data keys will be converted to lower case
func WithDiscriminator[K eventsourcing.ID, PK eventsourcing.IDPt[K]](disc eventsourcing.Discriminator) Option[K, PK] {
	return func(r *EsRepository[K, PK]) {
		m := eventsourcing.Discriminator{}
		for k, v := range disc {
			m[strings.ToLower(k)] = v
		}
		r.discriminator = m
	}
}

// WithDiscriminatorHook defines the hook that will return the discriminator.
// This discriminator will override any discriminator defined at the repository level
func WithDiscriminatorHook[K eventsourcing.ID, PK eventsourcing.IDPt[K]](fn store.DiscriminatorHook[K]) Option[K, PK] {
	return func(r *EsRepository[K, PK]) {
		r.discriminatorHook = fn
	}
}

func WithSkipSchemaCreation[K eventsourcing.ID, PK eventsourcing.IDPt[K]](skip bool) Option[K, PK] {
	return func(r *EsRepository[K, PK]) {
		r.skipSchemaCreation = skip
	}
}

type Repository struct {
	client *mongo.Client
}

func TxRunner(client *mongo.Client) store.Tx {
	return Repository{client}.TxRunner()
}

func (r Repository) TxRunner() store.Tx {
	return func(ctx context.Context, fn func(context.Context) error) error {
		return r.WithTx(ctx, func(c context.Context) error {
			return fn(c)
		})
	}
}

func (r Repository) WithTx(ctx context.Context, callback func(context.Context) error) (err error) {
	sess := mongo.SessionFromContext(ctx)
	if sess != nil {
		return callback(ctx)
	}

	return r.wrapWithTx(ctx, callback)
}

func (r Repository) wrapWithTx(ctx context.Context, callback func(context.Context) error) (err error) {
	session, err := r.client.StartSession()
	if err != nil {
		return faults.Wrap(err)
	}
	defer session.EndSession(ctx)

	fn := func(sessCtx mongo.SessionContext) (any, error) {
		er := callback(sessCtx)
		return nil, er
	}
	_, err = session.WithTransaction(ctx, fn, &options.TransactionOptions{
		WriteConcern: writeconcern.Majority(),
	})
	if err != nil {
		return faults.Wrap(err)
	}

	return nil
}

var (
	_ eventsourcing.EsRepository[ulid.ULID]  = (*EsRepository[ulid.ULID, *ulid.ULID])(nil)
	_ projection.EventsRepository[ulid.ULID] = (*EsRepository[ulid.ULID, *ulid.ULID])(nil)
)

type EsRepository[K eventsourcing.ID, PK eventsourcing.IDPt[K]] struct {
	Repository

	dbName                  string
	txHandlers              []store.InTxHandler[K]
	eventsCollectionName    string
	snapshotsCollectionName string
	discriminator           eventsourcing.Discriminator
	discriminatorHook       store.DiscriminatorHook[K]
	skipSchemaCreation      bool
	allowedKeys             map[string]struct{}
}

func TryPing(ctx context.Context, client *mongo.Client) error {
	err := retry.Do(
		func() error {
			err := client.Ping(ctx, readpref.Primary())
			if err != nil && errors.Is(err, ctx.Err()) {
				return retry.Unrecoverable(err)
			}
			return faults.Wrapf(err, "pinging")
		},
		retry.Attempts(3),
		retry.Delay(time.Second),
	)
	return faults.Wrapf(err, "database did not respond to ping")
}

// NewStoreWithURI creates a new instance of MongoEsRepository
func NewStoreWithURI[K eventsourcing.ID, PK eventsourcing.IDPt[K]](ctx context.Context, connString, database string, opts ...Option[K, PK]) (*EsRepository[K, PK], error) {
	ctx2, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	client, err := mongo.Connect(ctx2, options.Client().ApplyURI(connString))
	if err != nil {
		return nil, faults.Wrap(err)
	}

	err = TryPing(ctx, client)
	if err != nil {
		return nil, faults.Wrap(err)
	}

	return NewStore[K, PK](ctx, client, database, opts...)
}

// NewStore creates a new instance of MongoEsRepository
func NewStore[K eventsourcing.ID, PK eventsourcing.IDPt[K]](ctx context.Context, client *mongo.Client, database string, opts ...Option[K, PK]) (*EsRepository[K, PK], error) {
	r := &EsRepository[K, PK]{
		Repository: Repository{
			client: client,
		},
		dbName:                  database,
		eventsCollectionName:    defaultEventsCollection,
		snapshotsCollectionName: defaultSnapshotsCollection,
		allowedKeys:             make(map[string]struct{}),
	}

	for _, o := range opts {
		o(r)
	}

	if len(r.allowedKeys) == 0 {
		r.discriminatorHook = nil
	}

	if r.skipSchemaCreation {
		return r, nil
	}

	err := r.createSchema(ctx)
	if err != nil {
		return nil, faults.Wrap(err)
	}

	return r, nil
}

func (r *EsRepository[K, PK]) Client() *mongo.Client {
	return r.client
}

func (r *EsRepository[K, PK]) Close(ctx context.Context) {
	_ = r.client.Disconnect(ctx)
}

func (r *EsRepository[K, PK]) collection(coll string) *mongo.Collection {
	return r.client.Database(r.dbName).Collection(coll)
}

func (r *EsRepository[K, PK]) eventsCollection() *mongo.Collection {
	return r.collection(r.eventsCollectionName)
}

func (r *EsRepository[K, PK]) snapshotCollection() *mongo.Collection {
	return r.collection(r.snapshotsCollectionName)
}

func (r *EsRepository[K, PK]) SaveEvent(ctx context.Context, eRec *eventsourcing.EventRecord[K]) (eventid.EventID, uint32, error) {
	if len(eRec.Details) == 0 {
		return eventid.Zero, 0, faults.New("No events to be saved")
	}

	var id eventid.EventID
	version := eRec.Version
	err := r.WithTx(ctx, func(ctx context.Context) error {
		for _, e := range eRec.Details {
			version++
			id = e.ID
			aggIDStr := eRec.AggregateID.String()
			disc := r.discriminatorMerge(ctx, store.OnPersist)
			err := r.saveEvent(
				ctx,
				&Event{
					ID:               id.String(),
					AggregateID:      aggIDStr,
					AggregateIDHash:  util.HashToInt(aggIDStr),
					AggregateKind:    eRec.AggregateKind,
					Kind:             e.Kind,
					Body:             e.Body,
					AggregateVersion: version,
					Discriminator:    fromDiscriminator(disc),
					CreatedAt:        eRec.CreatedAt,
				},
				id,
			)
			if err != nil {
				return err
			}
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

func (r *EsRepository[K, PK]) discriminatorMerge(ctx context.Context, kind store.DiscriminatorHookKind) eventsourcing.Discriminator {
	return store.DiscriminatorMerge(
		ctx,
		r.allowedKeys,
		r.discriminator,
		r.discriminatorHook,
		kind,
	)
}

func (r *EsRepository[K, PK]) saveEvent(ctx context.Context, doc *Event, id eventid.EventID) error {
	doc.ID = id.String()
	_, err := r.eventsCollection().InsertOne(ctx, doc)
	if err != nil {
		return faults.Wrap(err)
	}

	return r.applyTxHandlers(ctx, doc, id)
}

func (r *EsRepository[K, PK]) applyTxHandlers(ctx context.Context, doc *Event, id eventid.EventID) error {
	if len(r.txHandlers) == 0 {
		return nil
	}

	e, err := toEventsourcingEvent[K, PK](doc, id)
	if err != nil {
		return err
	}
	for _, handler := range r.txHandlers {
		err := handler(store.NewInTxHandlerContext(ctx, e))
		if err != nil {
			return faults.Wrap(err)
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

func (r *EsRepository[K, PK]) GetSnapshot(ctx context.Context, aggregateID K) (eventsourcing.Snapshot[K], error) {
	snap := Snapshot{}
	opts := options.FindOne()
	opts.SetSort(bson.D{{Key: "aggregate_version", Value: -1}})
	filter := bson.D{{Key: "aggregate_id", Value: aggregateID.String()}}

	disc := r.discriminatorMerge(ctx, store.OnRetrieve)
	for k, v := range disc {
		filter = append(filter, bson.E{Key: "discriminator." + k, Value: bson.D{{Key: "$eq", Value: v}}})
	}

	if err := r.snapshotCollection().FindOne(ctx, filter, opts).Decode(&snap); err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return eventsourcing.Snapshot[K]{}, nil
		}
		return eventsourcing.Snapshot[K]{}, faults.Errorf("unable to get snapshot for aggregate '%s': %w", aggregateID, err)
	}
	id, err := eventid.Parse(snap.ID)
	if err != nil {
		return eventsourcing.Snapshot[K]{}, faults.Errorf("unable to parse snapshot ID '%s': %w", snap.ID, err)
	}

	return eventsourcing.Snapshot[K]{
		ID:               id,
		AggregateID:      aggregateID,
		AggregateVersion: snap.AggregateVersion,
		AggregateKind:    snap.AggregateKind,
		Body:             snap.Body,
		CreatedAt:        snap.CreatedAt,
		Discriminator:    toDiscriminator(snap.Discriminator),
	}, nil
}

func (r *EsRepository[K, PK]) IsSnapshotEnabled() bool {
	return r.snapshotsCollectionName != ""
}

func (r *EsRepository[K, PK]) SaveSnapshot(ctx context.Context, snapshot *eventsourcing.Snapshot[K]) error {
	disc := r.discriminatorMerge(ctx, store.OnPersist)
	return r.saveSnapshot(ctx, &Snapshot{
		ID:               snapshot.ID.String(),
		AggregateID:      snapshot.AggregateID.String(),
		AggregateVersion: snapshot.AggregateVersion,
		AggregateKind:    snapshot.AggregateKind,
		Body:             snapshot.Body,
		CreatedAt:        snapshot.CreatedAt,
		Discriminator:    fromDiscriminator(disc),
	})
}

func (r *EsRepository[K, PK]) saveSnapshot(ctx context.Context, snapshot *Snapshot) error {
	if r.snapshotsCollectionName == "" {
		return faults.New("snapshot collection is undefined")
	}

	// TODO instead of adding we could replace UPDATE/INSERT
	_, err := r.snapshotCollection().InsertOne(ctx, snapshot)

	return faults.Wrap(err)
}

func (r *EsRepository[K, PK]) GetAggregateEvents(ctx context.Context, aggregateID K, snapVersion int) ([]*eventsourcing.Event[K], error) {
	filter := bson.D{
		{Key: "aggregate_id", Value: bson.D{{Key: "$eq", Value: aggregateID.String()}}},
		{Key: "migration", Value: bson.D{{Key: "$eq", Value: 0}}},
	}
	if snapVersion > -1 {
		filter = append(filter, bson.E{Key: "aggregate_version", Value: bson.D{{Key: "$gt", Value: snapVersion}}})
	}

	disc := r.discriminatorMerge(ctx, store.OnRetrieve)
	for k, v := range disc {
		filter = append(filter, bson.E{Key: "discriminator." + k, Value: bson.D{{Key: "$eq", Value: v}}})
	}

	opts := options.Find()
	opts.SetSort(bson.D{{Key: "aggregate_version", Value: 1}})

	events, err := r.queryEvents(ctx, filter, opts)
	if err != nil {
		return nil, faults.Errorf("Unable to get events for Aggregate '%s': %w", aggregateID, err)
	}

	return events, nil
}

func (r *EsRepository[K, PK]) Forget(ctx context.Context, request eventsourcing.ForgetRequest[K], forget func(kind eventsourcing.Kind, body []byte) ([]byte, error)) error {
	// When Forget() is called, the aggregate is no longer used, therefore if it fails, it can be called again.

	// for events
	filter := bson.D{
		{Key: "aggregate_id", Value: bson.D{{Key: "$eq", Value: request.AggregateID.String()}}},
		{Key: "kind", Value: bson.D{{Key: "$eq", Value: request.EventKind}}},
	}
	cursor, err := r.eventsCollection().Find(ctx, filter)
	if err != nil && err != mongo.ErrNoDocuments {
		return faults.Wrap(err)
	}
	events := []Event{}
	if err = cursor.All(ctx, &events); err != nil {
		return faults.Errorf("unable to get events for Aggregate '%s' and event kind '%s': %w", request.AggregateID, request.EventKind, err)
	}
	for k := range events {
		evt := events[k]
		body, er := forget(evt.Kind, evt.Body)
		if er != nil {
			return er
		}

		fltr := bson.D{
			{Key: "_id", Value: evt.ID},
		}
		update := bson.M{
			"$set": bson.M{"body": body},
		}
		_, er = r.eventsCollection().UpdateOne(ctx, fltr, update)
		if er != nil {
			return faults.Errorf("Unable to forget event ID %s: %w", evt.ID, er)
		}
	}

	// for snapshots
	filter = bson.D{
		{Key: "aggregate_id", Value: bson.D{{Key: "$eq", Value: request.AggregateID.String()}}},
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
		body, err := forget(s.AggregateKind, s.Body)
		if err != nil {
			return err
		}

		filter := bson.D{
			{Key: "_id", Value: s.ID},
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

func (r *EsRepository[K, PK]) GetEvents(ctx context.Context, after, until eventid.EventID, batchSize int, filter store.Filter) ([]*eventsourcing.Event[K], error) {
	flt := bson.D{
		{Key: "_id", Value: bson.D{{Key: "$gt", Value: after.String()}}},
		{Key: "_id", Value: bson.D{{Key: "$lte", Value: until.String()}}},
		{Key: "migration", Value: bson.D{{Key: "$eq", Value: 0}}},
	}

	disc := r.discriminatorMerge(ctx, store.OnRetrieve)
	flt = buildFilter(disc, filter, flt)

	opts := options.Find().SetSort(bson.D{{Key: "_id", Value: 1}})
	if batchSize > 0 {
		opts.SetBatchSize(int32(batchSize))
	} else {
		opts.SetBatchSize(-1)
	}

	rows, err := r.queryEvents(ctx, flt, opts)
	if err != nil {
		return nil, faults.Errorf("getting events between ('%d', '%s'] for filter %+v: %w", after, until, filter, err)
	}
	if len(rows) == 0 {
		return nil, nil
	}

	return rows, nil
}

func buildFilter(disc eventsourcing.Discriminator, filter store.Filter, flt bson.D) bson.D {
	if len(filter.AggregateKinds) > 0 {
		flt = append(flt, bson.E{Key: "aggregate_kind", Value: bson.D{{Key: "$in", Value: filter.AggregateKinds}}})
	}

	if filter.Splits > 1 && len(filter.SplitIDs) != int(filter.Splits) {
		flt = append(flt, splitFilter("aggregate_id_hash", filter.Splits, filter.SplitIDs))
	}

	for k, v := range disc {
		flt = append(flt, bson.E{Key: "discriminator." + k, Value: bson.D{{Key: "$eq", Value: v}}})
	}

	if len(filter.Discriminator) > 0 {
		for _, kv := range filter.Discriminator {
			// ignore if already set by the discriminator
			if disc != nil {
				_, ok := disc[kv.Key]
				if ok {
					continue
				}
			}

			flt = append(flt, bson.E{Key: "discriminator." + kv.Key, Value: bson.D{{Key: "$in", Value: kv.Values}}})
		}
	}
	return flt
}

func splitFilter(field string, splits uint32, splitIDs []uint32) bson.E {
	field = "$" + field
	// aggregate: { $expr: {"$eq": [{"$mod" : [$field, splits]}],  split]} }
	return bson.E{
		Key: "$expr",
		Value: bson.D{
			{Key: "$in", Value: bson.A{
				bson.D{
					{Key: "$mod", Value: bson.A{field, splits}},
				},
				splitIDs,
			}},
		},
	}
}

func (r *EsRepository[K, PK]) queryEvents(ctx context.Context, filter bson.D, opts *options.FindOptions) ([]*eventsourcing.Event[K], error) {
	cursor, err := r.eventsCollection().Find(ctx, filter, opts)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return nil, nil
		}
		return nil, faults.Wrap(err)
	}

	evts := []*Event{}
	if err = cursor.All(ctx, &evts); err != nil {
		return nil, faults.Wrap(err)
	}

	events := []*eventsourcing.Event[K]{}
	for _, evt := range evts {
		lastEventID, err := eventid.Parse(evt.ID)
		if err != nil {
			return nil, faults.Errorf("unable to parse message ID '%s': %w", evt.ID, err)
		}
		evt, err := toEventsourcingEvent[K, PK](evt, lastEventID)
		if err != nil {
			return nil, err
		}
		events = append(events, evt)
	}

	return events, nil
}

func toEventsourcingEvent[K eventsourcing.ID, PK eventsourcing.IDPt[K]](e *Event, id eventid.EventID) (*eventsourcing.Event[K], error) {
	aggID := PK(new(K))
	err := aggID.UnmarshalText([]byte(e.AggregateID))
	if err != nil {
		return nil, faults.Errorf("unmarshaling id '%s': %w", e.AggregateID, err)
	}

	return &eventsourcing.Event[K]{
		ID:               id,
		AggregateID:      *aggID,
		AggregateIDHash:  uint32(e.AggregateIDHash),
		AggregateVersion: e.AggregateVersion,
		AggregateKind:    e.AggregateKind,
		Kind:             e.Kind,
		Body:             e.Body,
		Discriminator:    toDiscriminator(e.Discriminator),
		CreatedAt:        e.CreatedAt,
		Migrated:         e.Migrated,
	}, nil
}

func (r *EsRepository[K, PK]) GetEventsByRawIDs(ctx context.Context, ids []string) ([]*eventsourcing.Event[K], error) {
	opts := options.Find().SetSort(bson.D{{Key: "_id", Value: 1}})
	return r.queryEvents(ctx, bson.D{bson.E{Key: "_id", Value: bson.D{{Key: "$in", Value: ids}}}}, opts)
}

func fromDiscriminator(disc eventsourcing.Discriminator) primitive.M {
	m := primitive.M{}
	for k, v := range disc {
		m[k] = v
	}
	return m
}

func toDiscriminator(disc primitive.M) eventsourcing.Discriminator {
	m := eventsourcing.Discriminator{}
	for k, v := range disc {
		m[k] = v.(string)
	}
	return m
}
