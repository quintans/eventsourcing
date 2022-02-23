package eventsourcing

import (
	"context"
	"errors"
	"time"

	"github.com/quintans/faults"

	"github.com/quintans/eventsourcing/encoding"
	"github.com/quintans/eventsourcing/eventid"
	"github.com/quintans/eventsourcing/util"
)

const (
	EmptyIdempotencyKey           = ""
	InvalidatedKind     EventKind = "Invalidated"
)

var (
	ErrConcurrentModification = errors.New("concurrent modification")
	ErrUnknownAggregateID     = errors.New("unknown aggregate ID")
)

type Factory interface {
	AggregateFactory
	EventFactory
}

type AggregateFactory interface {
	NewAggregate(typ AggregateType) (Aggregater, error)
}

type EventFactory interface {
	NewEvent(kind EventKind) (Typer, error)
}

type Upcaster interface {
	Upcast(Typer) Typer
}

type Codec interface {
	Encoder
	Decoder
}

type Encoder interface {
	Encode(v interface{}) ([]byte, error)
}

type Decoder interface {
	Decode(data []byte, v interface{}) error
}

type Aggregater interface {
	Typer
	GetID() string
	SetVersion(uint32)
	GetVersion() uint32
	// GetEventsCounter used to determine snapshots threshold.
	// It returns the number of events since the last snapshot
	GetEventsCounter() uint32
	GetEvents() []Eventer
	ClearEvents()
	ApplyChangeFromHistory(event Eventer)
	SetUpdatedAt(time.Time)
	// GetUpdatedAt always return the time of the last event applied
	GetUpdatedAt() time.Time
}

// Event represents the event data
type Event struct {
	ID               eventid.EventID
	ResumeToken      encoding.Base64
	AggregateID      string
	AggregateIDHash  uint32
	AggregateVersion uint32
	AggregateType    AggregateType
	Kind             EventKind
	Body             encoding.Base64
	IdempotencyKey   string
	Metadata         *encoding.Json
	CreatedAt        time.Time
}

func (e Event) IsZero() bool {
	return e.ID.IsZero()
}

type Snapshot struct {
	ID               eventid.EventID
	AggregateID      string
	AggregateVersion uint32
	AggregateType    AggregateType
	Body             []byte
	CreatedAt        time.Time
}

type EsRepository interface {
	SaveEvent(ctx context.Context, eRec EventRecord) (id eventid.EventID, version uint32, err error)
	GetSnapshot(ctx context.Context, aggregateID string) (Snapshot, error)
	SaveSnapshot(ctx context.Context, snapshot Snapshot) error
	GetAggregateEvents(ctx context.Context, aggregateID string, snapVersion int) ([]Event, error)
	HasIdempotencyKey(ctx context.Context, idempotencyKey string) (bool, error)
	Forget(ctx context.Context, request ForgetRequest, forget func(kind string, body []byte, snapshot bool) ([]byte, error)) error
	MigrateInPlaceCopyReplace(
		ctx context.Context,
		revision int,
		snapshotThreshold uint32,
		aggregateFactory func() (Aggregater, error), // called only if snapshot threshold is reached
		rehydrateFunc func(Aggregater, Event) error, // called only if snapshot threshold is reached
		encoder Encoder,
		handler MigrationHandler,
		aggregateType AggregateType,
		eventTypeCriteria ...EventKind,
	) error
}

// Event is the event data stored in the database
type EventMigration struct {
	Kind           EventKind
	Body           []byte
	IdempotencyKey string
	Metadata       *encoding.Json
}

func DefaultEventMigration(e *Event) *EventMigration {
	return &EventMigration{
		Kind:           e.Kind,
		Body:           e.Body,
		IdempotencyKey: e.IdempotencyKey,
		Metadata:       e.Metadata,
	}
}

var KindNoOpEvent = EventKind("NoOp")

// NoOpEvent is used as marker for consistent projection migration
// making sure that no other event was added while recreating the state of an aggregate
type NoOpEvent struct{}

func (e NoOpEvent) GetType() string {
	return KindNoOpEvent.String()
}

// MigrationHandler receives the list of events for a stream and transforms the list of events.
// if the returned list is nil, it means no changes where made
type MigrationHandler func(events []*Event) ([]*EventMigration, error)

type EventRecord struct {
	AggregateID    string
	Version        uint32
	AggregateType  AggregateType
	IdempotencyKey string
	Metadata       map[string]interface{}
	CreatedAt      time.Time
	Details        []EventRecordDetail
}

type EventRecordDetail struct {
	Kind EventKind
	Body []byte
}

type Options struct {
	IdempotencyKey string
	// Labels tags the event. eg: {"geo": "EU"}
	Labels map[string]interface{}
	clock  util.Clocker
}

type SaveOption func(*Options)

func WithIdempotencyKey(key string) SaveOption {
	return func(o *Options) {
		o.IdempotencyKey = key
	}
}

func WithMetadata(metadata map[string]interface{}) SaveOption {
	return func(o *Options) {
		o.Labels = metadata
	}
}

// WithClock allows to set a logical clock and time relate two aggregates
func WithClock(clock util.Clocker) SaveOption {
	return func(o *Options) {
		o.clock = clock
	}
}

type EventStorer interface {
	GetByID(ctx context.Context, aggregateID string) (Aggregater, error)
	Save(ctx context.Context, aggregate Aggregater, options ...SaveOption) error
	HasIdempotencyKey(ctx context.Context, idempotencyKey string) (bool, error)
	// Forget erases the values of the specified fields
	Forget(ctx context.Context, request ForgetRequest, forget func(interface{}) interface{}) error
}

var _ EventStorer = (*EventStore)(nil)

type EsOptions func(*EventStore)

func WithCodec(codec Codec) EsOptions {
	return func(r *EventStore) {
		r.codec = codec
	}
}

func WithUpcaster(upcaster Upcaster) EsOptions {
	return func(r *EventStore) {
		r.upcaster = upcaster
	}
}

func WithSnapshotThreshold(snapshotThreshold uint32) EsOptions {
	return func(r *EventStore) {
		r.snapshotThreshold = snapshotThreshold
	}
}

// EventStore represents the event store
type EventStore struct {
	store             EsRepository
	snapshotThreshold uint32
	upcaster          Upcaster
	factory           Factory
	codec             Codec
}

// NewEventStore creates a new instance of ESPostgreSQL
func NewEventStore(repo EsRepository, factory Factory, options ...EsOptions) EventStore {
	es := EventStore{
		store:             repo,
		snapshotThreshold: 100,
		factory:           factory,
		codec:             JSONCodec{},
	}
	for _, v := range options {
		v(&es)
	}
	return es
}

// Exec loads the aggregate from the event store and handles it to the handler function, saving the returning Aggregater in the event store.
// If no aggregate is found for the provided ID the error ErrUnknownAggregateID is returned.
// If the handler function returns nil for the Aggregater or an error, the save action is ignored.
func (es EventStore) Exec(ctx context.Context, id string, do func(Aggregater) (Aggregater, error), options ...SaveOption) error {
	a, err := es.GetByID(ctx, id)
	if err != nil {
		return err
	}
	if a == nil {
		return ErrUnknownAggregateID
	}
	a, err = do(a)
	if err != nil {
		return err
	}
	if a == nil {
		return nil
	}

	return es.Save(ctx, a, options...)
}

func (es EventStore) GetByID(ctx context.Context, aggregateID string) (Aggregater, error) {
	snap, err := es.store.GetSnapshot(ctx, aggregateID)
	if err != nil {
		return nil, err
	}
	var aggregate Aggregater
	if len(snap.Body) != 0 {
		aggregate, err = es.RehydrateAggregate(snap.AggregateType, snap.Body)
		if err != nil {
			return nil, err
		}
		aggregate.SetVersion(snap.AggregateVersion)
		aggregate.SetUpdatedAt(snap.CreatedAt)
	}

	var events []Event
	if snap.AggregateID == "" {
		events, err = es.store.GetAggregateEvents(ctx, aggregateID, -1)
	} else {
		events, err = es.store.GetAggregateEvents(ctx, aggregateID, int(snap.AggregateVersion))
	}
	if err != nil {
		return nil, err
	}

	for _, v := range events {
		// if the aggregate was not instantiated because the snap was not found
		if aggregate == nil {
			aggregate, err = es.RehydrateAggregate(v.AggregateType, nil)
			if err != nil {
				return nil, err
			}
		}
		if err := es.ApplyChangeFromHistory(aggregate, v); err != nil {
			return nil, err
		}
	}

	return aggregate, nil
}

func (es EventStore) ApplyChangeFromHistory(agg Aggregater, e Event) error {
	evt, err := es.RehydrateEvent(e.Kind, e.Body)
	if err != nil {
		return err
	}
	agg.ApplyChangeFromHistory(evt)
	agg.SetVersion(e.AggregateVersion)
	agg.SetUpdatedAt(e.CreatedAt)

	return nil
}

func (es EventStore) RehydrateAggregate(aggregateType AggregateType, body []byte) (Aggregater, error) {
	return RehydrateAggregate(es.factory, es.codec, es.upcaster, aggregateType, body)
}

func (es EventStore) RehydrateEvent(kind EventKind, body []byte) (Typer, error) {
	return RehydrateEvent(es.factory, es.codec, es.upcaster, kind, body)
}

// Save saves the events of the aggregater into the event store
func (es EventStore) Save(ctx context.Context, aggregate Aggregater, options ...SaveOption) (err error) {
	events := aggregate.GetEvents()
	eventsLen := len(events)
	if eventsLen == 0 {
		return nil
	}

	opts := Options{
		clock: util.NewClock(),
	}
	for _, fn := range options {
		fn(&opts)
	}

	now := opts.clock.After(aggregate.GetUpdatedAt())

	tName := aggregate.GetType()
	details := make([]EventRecordDetail, eventsLen)
	for i := 0; i < eventsLen; i++ {
		e := events[i]
		body, err := es.codec.Encode(e)
		if err != nil {
			return err
		}
		details[i] = EventRecordDetail{
			Kind: EventKind(e.GetType()),
			Body: body,
		}
	}

	rec := EventRecord{
		AggregateID:    aggregate.GetID(),
		Version:        aggregate.GetVersion(),
		AggregateType:  AggregateType(tName),
		IdempotencyKey: opts.IdempotencyKey,
		Metadata:       opts.Labels,
		CreatedAt:      now,
		Details:        details,
	}

	id, lastVersion, err := es.store.SaveEvent(ctx, rec)
	if err != nil {
		return err
	}
	aggregate.SetVersion(lastVersion)

	eventsCounter := aggregate.GetEventsCounter()
	if eventsCounter >= es.snapshotThreshold {
		body, err := es.codec.Encode(aggregate)
		if err != nil {
			return faults.Errorf("Failed to create serialize snapshot: %w", err)
		}

		snap := Snapshot{
			ID:               id,
			AggregateID:      aggregate.GetID(),
			AggregateVersion: aggregate.GetVersion(),
			AggregateType:    AggregateType(aggregate.GetType()),
			Body:             body,
			CreatedAt:        now,
		}

		err = es.store.SaveSnapshot(ctx, snap)
		if err != nil {
			return err
		}
	}

	aggregate.ClearEvents()
	return nil
}

func (es EventStore) HasIdempotencyKey(ctx context.Context, idempotencyKey string) (bool, error) {
	if idempotencyKey == EmptyIdempotencyKey {
		return false, nil
	}
	return es.store.HasIdempotencyKey(ctx, idempotencyKey)
}

type ForgetRequest struct {
	AggregateID string
	EventKind   EventKind
}

func (es EventStore) Forget(ctx context.Context, request ForgetRequest, forget func(interface{}) interface{}) error {
	fun := func(kind string, body []byte, snapshot bool) ([]byte, error) {
		var e Typer
		var err error
		if snapshot {
			e, err = es.factory.NewAggregate(AggregateType(kind))
		} else {
			e, err = es.factory.NewEvent(EventKind(kind))
		}
		if err != nil {
			return nil, err
		}

		err = es.codec.Decode(body, e)
		if err != nil {
			return nil, err
		}
		e2 := util.Dereference(e)
		e2 = forget(e2)
		body, err = es.codec.Encode(e2)
		if err != nil {
			return nil, err
		}

		return body, nil
	}

	return es.store.Forget(ctx, request, fun)
}

func (es EventStore) MigrateInPlaceCopyReplace(
	ctx context.Context,
	revision int,
	snapshotThreshold uint32,
	handler MigrationHandler,
	aggregateType AggregateType,
	eventTypeCriteria ...EventKind,
) error {
	return es.store.MigrateInPlaceCopyReplace(ctx,
		revision,
		snapshotThreshold,
		func() (Aggregater, error) {
			return es.factory.NewAggregate(aggregateType)
		},
		es.ApplyChangeFromHistory,
		es.codec,
		handler,
		aggregateType,
		eventTypeCriteria...)
}
