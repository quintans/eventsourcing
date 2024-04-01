package eventsourcing

import (
	"context"
	goenc "encoding"
	"errors"
	"fmt"
	"time"

	"github.com/quintans/eventsourcing/encoding"
	"github.com/quintans/eventsourcing/eventid"
	"github.com/quintans/faults"
)

const (
	InvalidatedKind Kind = "Invalidated"
)

var (
	ErrConcurrentModification = errors.New("concurrent modification")
	ErrUnknownAggregateID     = errors.New("unknown aggregate ID")
)

type Codec[K ID] interface {
	Encoder
	Decoder[K]
}

type Encoder interface {
	Encode(v Kinder) ([]byte, error)
}

type Decoder[K ID] interface {
	Decode(data []byte, meta DecoderMeta[K]) (Kinder, error)
}

type DecoderMeta[K ID] struct {
	Kind        Kind
	AggregateID K
}

type ID interface {
	comparable
	fmt.Stringer
}

type IDPt[T ID] interface {
	*T
	goenc.TextUnmarshaler
}

type Aggregater[K ID] interface {
	Kinder
	GetID() K
	PopEvents() []Eventer
	HandleEvent(Eventer) error
}

// Event represents the event data stored in the event store
type Event[K ID] struct {
	ID               eventid.EventID
	AggregateID      K
	AggregateIDHash  uint32
	AggregateVersion uint32
	AggregateKind    Kind
	Kind             Kind
	Body             encoding.Base64
	Metadata         Metadata
	CreatedAt        time.Time
	Migrated         bool
}

func (e *Event[K]) IsZero() bool {
	return e.ID.IsZero()
}

type Snapshot[K ID] struct {
	ID               eventid.EventID
	AggregateID      K
	AggregateVersion uint32
	AggregateKind    Kind
	Body             []byte
	CreatedAt        time.Time
	Metadata         Metadata
}

type EsRepository[K ID] interface {
	SaveEvent(ctx context.Context, eRec *EventRecord[K]) (id eventid.EventID, version uint32, err error)
	GetSnapshot(ctx context.Context, aggregateID K) (Snapshot[K], error)
	SaveSnapshot(ctx context.Context, snapshot *Snapshot[K]) error
	GetAggregateEvents(ctx context.Context, aggregateID K, snapVersion int) ([]*Event[K], error)
	Forget(ctx context.Context, request ForgetRequest[K], forget func(kind Kind, body []byte) ([]byte, error)) error
	MigrateInPlaceCopyReplace(
		ctx context.Context,
		revision int,
		snapshotThreshold uint32,
		rehydrateFunc func(Aggregater[K], *Event[K]) error, // called only if snapshot threshold is reached
		codec Codec[K],
		handler MigrationHandler[K],
		targetAggregateKind Kind,
		aggregateKind Kind,
		eventTypeCriteria ...Kind,
	) error
}

// Event is the event data stored in the database
type EventMigration struct {
	Kind     Kind
	Body     []byte
	Metadata Metadata
}

func DefaultEventMigration[K ID](e *Event[K]) *EventMigration {
	return &EventMigration{
		Kind:     e.Kind,
		Body:     e.Body,
		Metadata: e.Metadata,
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
type MigrationHandler[K ID] func(events []*Event[K]) ([]*EventMigration, error)

type EventRecord[K ID] struct {
	AggregateID   K
	Version       uint32
	AggregateKind Kind
	CreatedAt     time.Time
	Details       []EventRecordDetail
}

type EventRecordDetail struct {
	ID   eventid.EventID
	Kind Kind
	Body []byte
}

type Metadata map[string]string

type EventStorer[T Aggregater[K], K ID] interface {
	Create(ctx context.Context, aggregate T) error
	Retrieve(ctx context.Context, aggregateID K) (T, error)
	Update(ctx context.Context, aggregateID K, do func(T) (T, error)) error
	// Forget erases the values of the specified fields
	Forget(ctx context.Context, request ForgetRequest[K], forget func(Kinder) (Kinder, error)) error
}

type EsOptions struct {
	SnapshotThreshold uint32
}

// EventStore represents the event store
type EventStore[T Aggregater[K], K ID, PK IDPt[K]] struct {
	store             EsRepository[K]
	snapshotThreshold uint32
	codec             Codec[K]
}

// NewEventStore creates a new instance of ESPostgreSQL
func NewEventStore[T Aggregater[K], K ID, PK IDPt[K]](repo EsRepository[K], codec Codec[K], options *EsOptions) EventStore[T, K, PK] {
	es := EventStore[T, K, PK]{
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
func (es EventStore[T, K, PK]) Update(ctx context.Context, aggregateID K, do func(T) (T, error)) error {
	a, version, updatedAt, eventsCounter, err := es.retrieve(ctx, aggregateID)
	if err != nil {
		return err
	}

	a, err = do(a)
	if err != nil {
		return err
	}

	return es.save(ctx, a, version, updatedAt, eventsCounter)
}

func (es EventStore[T, K, PK]) Retrieve(ctx context.Context, aggregateID K) (T, error) {
	agg, _, _, _, err := es.retrieve(ctx, aggregateID)
	return agg, err
}

func (es EventStore[T, K, PK]) retrieve(ctx context.Context, aggregateID K) (T, uint32, time.Time, uint32, error) {
	var zero T
	snap, err := es.store.GetSnapshot(ctx, aggregateID)
	if err != nil {
		return zero, 0, time.Time{}, 0, err
	}
	var aggregate T
	var aggregateVersion uint32
	var updatedAt time.Time
	if len(snap.Body) != 0 {
		aggregate, err = es.RehydrateAggregate(snap.AggregateKind, aggregateID, snap.Body)
		if err != nil {
			return zero, 0, time.Time{}, 0, err
		}
		aggregateVersion = snap.AggregateVersion
		updatedAt = snap.CreatedAt
	}

	var zeroID K
	var events []*Event[K]
	if snap.AggregateID == zeroID {
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
		if aggregateVersion == 0 {
			aggregate, err = es.RehydrateAggregate(event.AggregateKind, aggregateID, nil)
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

	if aggregateVersion == 0 {
		return zero, 0, time.Time{}, 0, ErrUnknownAggregateID
	}

	return aggregate, aggregateVersion, updatedAt, eventsCounter, nil
}

func (es EventStore[T, K, PK]) ApplyChangeFromHistory(agg Aggregater[K], e *Event[K]) error {
	evt, err := es.RehydrateEvent(e.Kind, e.Body)
	if err != nil {
		return err
	}
	return agg.HandleEvent(evt)
}

func (es EventStore[T, K, PK]) RehydrateAggregate(aggregateKind Kind, id K, body []byte) (T, error) {
	a, err := es.codec.Decode(body, DecoderMeta[K]{
		Kind:        aggregateKind,
		AggregateID: id,
	})
	if err != nil {
		var zero T
		return zero, faults.Errorf("decoding kind '%s' into %T: %w", aggregateKind, a, err)
	}
	return a.(T), nil
}

func (es EventStore[T, K, PK]) RehydrateEvent(kind Kind, body []byte) (Kinder, error) {
	e, err := es.codec.Decode(body, DecoderMeta[K]{
		Kind: kind,
	})
	if err != nil {
		return nil, faults.Errorf("decoding kind '%s' into %T: %w", kind, e, err)
	}

	return e, nil
}

// Create saves the events of the aggregater into the event store
func (es EventStore[T, K, PK]) Create(ctx context.Context, aggregate T) (err error) {
	return es.save(ctx, aggregate, 0, time.Now(), 0)
}

func (es EventStore[T, K, PK]) save(
	ctx context.Context,
	aggregate T,
	version uint32,
	updatedAt time.Time,
	eventsCounter uint32,
) (err error) {
	events := aggregate.PopEvents()
	eventsLen := len(events)
	if eventsLen == 0 {
		return nil
	}

	gen := eventid.NewGenerator(updatedAt)
	tName := aggregate.GetKind()
	details := make([]EventRecordDetail, eventsLen)
	for i := 0; i < eventsLen; i++ {
		e := events[i]
		body, er := es.codec.Encode(e)
		if er != nil {
			return er
		}
		details[i] = EventRecordDetail{
			ID:   gen.NewID(),
			Kind: e.GetKind(),
			Body: body,
		}
	}

	now := time.Now()
	rec := &EventRecord[K]{
		AggregateID:   aggregate.GetID(),
		Version:       version,
		AggregateKind: tName,
		CreatedAt:     now,
		Details:       details,
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

		snap := &Snapshot[K]{
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

type ForgetRequest[K ID] struct {
	AggregateID K
	EventKind   Kind
}

func (es EventStore[T, K, PK]) Forget(ctx context.Context, request ForgetRequest[K], forget func(Kinder) (Kinder, error)) error {
	fun := func(kind Kind, body []byte) ([]byte, error) {
		k, err := es.codec.Decode(body, DecoderMeta[K]{
			Kind:        kind,
			AggregateID: request.AggregateID,
		})
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

func (es EventStore[T, K, PK]) MigrateInPlaceCopyReplace(
	ctx context.Context,
	revision int,
	snapshotThreshold uint32,
	handler MigrationHandler[K],
	targetAggregateKind Kind,
	originalAggregateKind Kind,
	originalEventTypeCriteria []Kind,
) error {
	return es.store.MigrateInPlaceCopyReplace(
		ctx,
		revision,
		snapshotThreshold,
		es.ApplyChangeFromHistory,
		es.codec,
		handler,
		targetAggregateKind,
		originalAggregateKind,
		originalEventTypeCriteria...,
	)
}
