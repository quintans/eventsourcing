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
	EmptyIdempotencyKey      = ""
	InvalidatedKind     Kind = "Invalidated"
)

var (
	ErrConcurrentModification = errors.New("concurrent modification")
	ErrUnknownAggregateID     = errors.New("unknown aggregate ID")
)

type Codec interface {
	Encoder
	Decoder
}

type Encoder interface {
	Encode(v Kinder) ([]byte, error)
}

type Decoder interface {
	Decode(data []byte, kind Kind) (Kinder, error)
}

type Aggregater interface {
	Kinder
	GetID() string
	PopEvents() []Eventer
	HandleEvent(Eventer) error
}

// Event represents the event data
type Event struct {
	ID               eventid.EventID
	ResumeToken      encoding.Base64
	AggregateID      string
	AggregateIDHash  uint32
	AggregateVersion uint32
	AggregateKind    Kind
	Kind             Kind
	Body             encoding.Base64
	IdempotencyKey   string
	Metadata         *encoding.JSON
	CreatedAt        time.Time
	Migrated         bool
}

func (e *Event) IsZero() bool {
	return e.ID.IsZero()
}

type Snapshot struct {
	ID               eventid.EventID
	AggregateID      string
	AggregateVersion uint32
	AggregateKind    Kind
	Body             []byte
	CreatedAt        time.Time
}

type EsRepository interface {
	SaveEvent(ctx context.Context, eRec *EventRecord) (id eventid.EventID, version uint32, err error)
	GetSnapshot(ctx context.Context, aggregateID string) (Snapshot, error)
	SaveSnapshot(ctx context.Context, snapshot *Snapshot) error
	GetAggregateEvents(ctx context.Context, aggregateID string, snapVersion int) ([]*Event, error)
	HasIdempotencyKey(ctx context.Context, idempotencyKey string) (bool, error)
	Forget(ctx context.Context, request ForgetRequest, forget func(kind Kind, body []byte, snapshot bool) ([]byte, error)) error
	MigrateInPlaceCopyReplace(
		ctx context.Context,
		revision int,
		snapshotThreshold uint32,
		rehydrateFunc func(Aggregater, *Event) error, // called only if snapshot threshold is reached
		codec Codec,
		handler MigrationHandler,
		targetAggregateKind Kind,
		aggregateKind Kind,
		eventTypeCriteria ...Kind,
	) error
}

// Event is the event data stored in the database
type EventMigration struct {
	Kind           Kind
	Body           []byte
	IdempotencyKey string
	Metadata       *encoding.JSON
}

func DefaultEventMigration(e *Event) *EventMigration {
	return &EventMigration{
		Kind:           e.Kind,
		Body:           e.Body,
		IdempotencyKey: e.IdempotencyKey,
		Metadata:       e.Metadata,
	}
}

var KindNoOpEvent = Kind("NoOp")

// NoOpEvent is used as marker for consistent projection migration
// making sure that no other event was added while recreating the state of an aggregate
type NoOpEvent struct{}

func (e NoOpEvent) GetKind() string {
	return KindNoOpEvent.String()
}

// MigrationHandler receives the list of events for a stream and transforms the list of events.
// if the returned list is nil, it means no changes where made
type MigrationHandler func(events []*Event) ([]*EventMigration, error)

type EventRecord struct {
	AggregateID    string
	Version        uint32
	AggregateKind  Kind
	IdempotencyKey string
	Metadata       map[string]interface{}
	CreatedAt      time.Time
	Details        []EventRecordDetail
}

type EventRecordDetail struct {
	Kind Kind
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

type EventStorer[T Aggregater] interface {
	Create(ctx context.Context, aggregate T, options ...SaveOption) error
	Retrieve(ctx context.Context, aggregateID string) (T, error)
	Update(ctx context.Context, id string, do func(T) (T, error), options ...SaveOption) error
	HasIdempotencyKey(ctx context.Context, idempotencyKey string) (bool, error)
	// Forget erases the values of the specified fields
	Forget(ctx context.Context, request ForgetRequest, forget func(Kinder) (Kinder, error)) error
}

var _ EventStorer[Aggregater] = (*EventStore[Aggregater])(nil)

type EsOptions struct {
	SnapshotThreshold uint32
}

// EventStore represents the event store
type EventStore[T Aggregater] struct {
	store             EsRepository
	snapshotThreshold uint32
	codec             Codec
}

// NewEventStore creates a new instance of ESPostgreSQL
func NewEventStore[T Aggregater](repo EsRepository, codec Codec, options *EsOptions) EventStore[T] {
	es := EventStore[T]{
		store:             repo,
		snapshotThreshold: 100,
		codec:             codec,
	}

	if options == nil {
		return es
	}

	if options.SnapshotThreshold != 0 {
		es.snapshotThreshold = options.SnapshotThreshold
	}

	return es
}

// Update loads the aggregate from the event store and handles it to the handler function, saving the returning Aggregater in the event store.
// If no aggregate is found for the provided ID the error ErrUnknownAggregateID is returned.
// If the handler function returns nil for the Aggregater or an error, the save action is ignored.
func (es EventStore[T]) Update(ctx context.Context, id string, do func(T) (T, error), options ...SaveOption) error {
	a, version, updatedAt, eventsCounter, err := es.retrieve(ctx, id)
	if err != nil {
		return err
	}

	a, err = do(a)
	if err != nil {
		return err
	}

	return es.save(ctx, a, version, updatedAt, eventsCounter, options...)
}

func (es EventStore[T]) Retrieve(ctx context.Context, aggregateID string) (T, error) {
	agg, _, _, _, err := es.retrieve(ctx, aggregateID)
	return agg, err
}

func (es EventStore[T]) retrieve(ctx context.Context, aggregateID string) (T, uint32, time.Time, uint32, error) {
	var zero T
	snap, err := es.store.GetSnapshot(ctx, aggregateID)
	if err != nil {
		return zero, 0, time.Time{}, 0, err
	}
	var aggregate T
	var aggregateVersion uint32
	var updatedAt time.Time
	if len(snap.Body) != 0 {
		aggregate, err = es.RehydrateAggregate(snap.AggregateKind, snap.Body)
		if err != nil {
			return zero, 0, time.Time{}, 0, err
		}
		aggregateVersion = snap.AggregateVersion
		updatedAt = snap.CreatedAt
	}

	var events []*Event
	if snap.AggregateID == "" {
		events, err = es.store.GetAggregateEvents(ctx, aggregateID, -1)
	} else {
		events, err = es.store.GetAggregateEvents(ctx, aggregateID, int(snap.AggregateVersion))
	}
	if err != nil {
		return zero, 0, time.Time{}, 0, err
	}

	var eventsCounter uint32
	for _, event := range events {
		// if the aggregate was not instantiated because the snap was not found
		if aggregate.GetID() == "" {
			aggregate, err = es.RehydrateAggregate(event.AggregateKind, nil)
			if err != nil {
				return zero, 0, time.Time{}, 0, err
			}
		}
		if err := es.ApplyChangeFromHistory(aggregate, event); err != nil {
			return zero, 0, time.Time{}, 0, err
		}
		aggregateVersion = event.AggregateVersion
		updatedAt = event.CreatedAt
		eventsCounter++
	}

	if aggregate.GetID() == "" {
		return zero, 0, time.Time{}, 0, ErrUnknownAggregateID
	}

	return aggregate, aggregateVersion, updatedAt, eventsCounter, nil
}

func (es EventStore[T]) ApplyChangeFromHistory(agg Aggregater, e *Event) error {
	evt, err := es.RehydrateEvent(e.Kind, e.Body)
	if err != nil {
		return err
	}
	return agg.HandleEvent(evt)
}

func (es EventStore[T]) RehydrateAggregate(aggregateKind Kind, body []byte) (T, error) {
	return RehydrateAggregate[T](es.codec, aggregateKind, body)
}

func (es EventStore[T]) RehydrateEvent(kind Kind, body []byte) (Kinder, error) {
	return RehydrateEvent(es.codec, kind, body)
}

// Create saves the events of the aggregater into the event store
func (es EventStore[T]) Create(ctx context.Context, aggregate Aggregater, options ...SaveOption) (err error) {
	return es.save(ctx, aggregate, 0, time.Now(), 0, options...)
}

func (es EventStore[T]) save(
	ctx context.Context,
	aggregate Aggregater,
	version uint32,
	updatedAt time.Time,
	eventsCounter uint32,
	options ...SaveOption,
) (err error) {
	events := aggregate.PopEvents()
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

	now := opts.clock.After(updatedAt)

	tName := aggregate.GetKind()
	details := make([]EventRecordDetail, eventsLen)
	for i := 0; i < eventsLen; i++ {
		e := events[i]
		body, er := es.codec.Encode(e)
		if er != nil {
			return er
		}
		details[i] = EventRecordDetail{
			Kind: e.GetKind(),
			Body: body,
		}
	}

	rec := &EventRecord{
		AggregateID:    aggregate.GetID(),
		Version:        version,
		AggregateKind:  tName,
		IdempotencyKey: opts.IdempotencyKey,
		Metadata:       opts.Labels,
		CreatedAt:      now,
		Details:        details,
	}

	id, lastVersion, err := es.store.SaveEvent(ctx, rec)
	if err != nil {
		return err
	}

	if (eventsCounter + uint32(eventsLen)) >= es.snapshotThreshold {
		body, err := es.codec.Encode(aggregate)
		if err != nil {
			return faults.Errorf("Failed to create serialize snapshot: %w", err)
		}

		snap := &Snapshot{
			ID:               id,
			AggregateID:      aggregate.GetID(),
			AggregateVersion: lastVersion,
			AggregateKind:    tName,
			Body:             body,
			CreatedAt:        now,
		}

		err = es.store.SaveSnapshot(ctx, snap)
		if err != nil {
			return err
		}
	}

	return nil
}

func (es EventStore[T]) HasIdempotencyKey(ctx context.Context, idempotencyKey string) (bool, error) {
	if idempotencyKey == EmptyIdempotencyKey {
		return false, nil
	}
	return es.store.HasIdempotencyKey(ctx, idempotencyKey)
}

type ForgetRequest struct {
	AggregateID string
	EventKind   Kind
}

func (es EventStore[T]) Forget(ctx context.Context, request ForgetRequest, forget func(Kinder) (Kinder, error)) error {
	fun := func(kind Kind, body []byte, snapshot bool) ([]byte, error) {
		k, err := es.codec.Decode(body, kind)
		if err != nil {
			return nil, err
		}
		k, err = forget(k)
		if err != nil {
			return nil, err
		}
		body, err = es.codec.Encode(k)
		if err != nil {
			return nil, err
		}

		return body, nil
	}

	return es.store.Forget(ctx, request, fun)
}

func (es EventStore[T]) MigrateInPlaceCopyReplace(
	ctx context.Context,
	revision int,
	snapshotThreshold uint32,
	handler MigrationHandler,
	targetAggregateKind Kind,
	originalAggregateKind Kind,
	originalEventTypeCriteria []Kind,
) error {
	return es.store.MigrateInPlaceCopyReplace(ctx,
		revision,
		snapshotThreshold,
		es.ApplyChangeFromHistory,
		es.codec,
		handler,
		targetAggregateKind,
		originalAggregateKind,
		originalEventTypeCriteria...)
}
