package eventsourcing

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/quintans/faults"

	"github.com/quintans/eventsourcing/common"
	"github.com/quintans/eventsourcing/encoding"
	"github.com/quintans/eventsourcing/eventid"
)

var (
	ErrConcurrentModification = errors.New("concurrent modification")
	ErrUnknownAggregateID     = errors.New("unknown aggregate ID")
)

type Factory interface {
	New(kind string) (Typer, error)
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
	GetID() uuid.UUID
	GetVersion() uint32
	SetVersion(uint32)
	// GetEventsCounter used to determine snapshots threshold
	GetEventsCounter() uint32
	GetEvents() []Eventer
	ClearEvents()
	ApplyChangeFromHistory(m EventMetadata, event Eventer)
	UpdatedAt() time.Time
}

// Event represents the event data
type Event struct {
	ID               eventid.EventID
	ResumeToken      encoding.Base64
	AggregateID      uuid.UUID
	AggregateIDHash  uint32
	AggregateVersion uint32
	AggregateType    AggregateType
	Kind             EventKind
	Body             encoding.Base64
	IdempotencyKey   string
	Metadata         map[string]interface{}
	CreatedAt        time.Time
}

func (e Event) IsZero() bool {
	return e.ID.IsZero()
}

type Snapshot struct {
	ID               eventid.EventID
	AggregateID      uuid.UUID
	AggregateVersion uint32
	AggregateType    AggregateType
	Body             []byte
	CreatedAt        time.Time
}

type EsRepository interface {
	SaveEvent(ctx context.Context, eRec EventRecord) (id eventid.EventID, version uint32, err error)
	GetSnapshot(ctx context.Context, aggregateID uuid.UUID) (Snapshot, error)
	SaveSnapshot(ctx context.Context, snapshot Snapshot) error
	GetAggregateEvents(ctx context.Context, aggregateID uuid.UUID, snapVersion int) ([]Event, error)
	HasIdempotencyKey(ctx context.Context, aggregateType AggregateType, idempotencyKey string) (bool, error)
	Forget(ctx context.Context, request ForgetRequest, forget func(kind string, body []byte) ([]byte, error)) error
}

type EventRecord struct {
	AggregateID    uuid.UUID
	Version        uint32
	AggregateType  AggregateType
	IdempotencyKey string
	Labels         map[string]interface{}
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

type EventStorer interface {
	GetByID(ctx context.Context, aggregateID uuid.UUID) (Aggregater, error)
	Save(ctx context.Context, aggregate Aggregater, options ...SaveOption) error
	HasIdempotencyKey(ctx context.Context, aggregateType AggregateType, idempotencyKey string) (bool, error)
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

// EventStore represents the event store
type EventStore struct {
	store             EsRepository
	snapshotThreshold uint32
	upcaster          Upcaster
	factory           Factory
	codec             Codec
}

// NewEventStore creates a new instance of ESPostgreSQL
func NewEventStore(repo EsRepository, snapshotThreshold uint32, factory Factory, options ...EsOptions) EventStore {
	es := EventStore{
		store:             repo,
		snapshotThreshold: snapshotThreshold,
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
func (es EventStore) Exec(ctx context.Context, id uuid.UUID, do func(Aggregater) (Aggregater, error), options ...SaveOption) error {
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

func (es EventStore) GetByID(ctx context.Context, aggregateID uuid.UUID) (Aggregater, error) {
	snap, err := es.store.GetSnapshot(ctx, aggregateID)
	if err != nil {
		return nil, err
	}
	var aggregate Aggregater
	if len(snap.Body) != 0 {
		a, err := es.RehydrateAggregate(snap.AggregateType, snap.Body)
		if err != nil {
			return nil, err
		}
		aggregate = a.(Aggregater)
	}

	var events []Event
	if snap.AggregateID == uuid.Nil {
		events, err = es.store.GetAggregateEvents(ctx, aggregateID, -1)
	} else {
		events, err = es.store.GetAggregateEvents(ctx, aggregateID, int(snap.AggregateVersion))
	}
	if err != nil {
		return nil, err
	}

	for _, v := range events {
		if aggregate == nil {
			a, err := es.RehydrateAggregate(v.AggregateType, nil)
			if err != nil {
				return nil, err
			}
			aggregate = a.(Aggregater)
		}
		m := EventMetadata{
			AggregateVersion: v.AggregateVersion,
			CreatedAt:        v.CreatedAt,
		}
		e, err := es.RehydrateEvent(v.Kind, v.Body)
		if err != nil {
			return nil, err
		}
		aggregate.ApplyChangeFromHistory(m, e)
	}

	return aggregate, nil
}

func (es EventStore) RehydrateAggregate(aggregateType AggregateType, body []byte) (Typer, error) {
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

	opts := Options{}
	for _, fn := range options {
		fn(&opts)
	}

	now := time.Now().UTC()
	// we only need millisecond precision
	now = now.Truncate(time.Millisecond)
	// due to clock skews, now can be less than the last aggregate update
	// so we make sure that it will be att least the same.
	// Version will break the tie when generating the ID
	if now.Before(aggregate.UpdatedAt()) {
		now = aggregate.UpdatedAt()
	}

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
		Labels:         opts.Labels,
		CreatedAt:      now,
		Details:        details,
	}

	id, lastVersion, err := es.store.SaveEvent(ctx, rec)
	if err != nil {
		return err
	}
	aggregate.SetVersion(lastVersion)

	newCounter := aggregate.GetEventsCounter()
	oldCounter := newCounter - uint32(eventsLen)
	if newCounter > es.snapshotThreshold-1 {
		// TODO this could be done asynchronously. Beware that aggregate holds a reference and not a copy.
		mod := oldCounter % es.snapshotThreshold
		delta := newCounter - (oldCounter - mod)
		if delta >= es.snapshotThreshold {
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
				CreatedAt:        time.Now().UTC(),
			}

			err = es.store.SaveSnapshot(ctx, snap)
			if err != nil {
				return err
			}
		}
	}

	aggregate.ClearEvents()
	return nil
}

func (es EventStore) HasIdempotencyKey(ctx context.Context, aggregateType AggregateType, idempotencyKey string) (bool, error) {
	return es.store.HasIdempotencyKey(ctx, aggregateType, idempotencyKey)
}

type ForgetRequest struct {
	AggregateID uuid.UUID
	EventKind   EventKind
}

func (es EventStore) Forget(ctx context.Context, request ForgetRequest, forget func(interface{}) interface{}) error {
	fun := func(kind string, body []byte) ([]byte, error) {
		e, err := es.factory.New(kind)
		if err != nil {
			return nil, err
		}
		err = es.codec.Decode(body, e)
		if err != nil {
			return nil, err
		}
		e2 := common.Dereference(e)
		e2 = forget(e2)
		body, err = es.codec.Encode(e2)
		if err != nil {
			return nil, err
		}

		return body, nil
	}

	return es.store.Forget(ctx, request, fun)
}
