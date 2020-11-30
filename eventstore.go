package eventstore

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/quintans/eventstore/common"
)

var (
	ErrConcurrentModification = errors.New("Concurrent Modification")
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
	GetID() string
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
	ID               string
	ResumeToken      common.Base64
	AggregateID      string
	AggregateVersion uint32
	AggregateType    string
	Kind             string
	Body             common.Base64
	IdempotencyKey   string
	Labels           map[string]interface{}
	CreatedAt        time.Time
}

func (e Event) IsZero() bool {
	return e.ID == ""
}

type Snapshot struct {
	ID               string
	AggregateID      string
	AggregateVersion uint32
	AggregateType    string
	Body             []byte
	CreatedAt        time.Time
}

type EsRepository interface {
	SaveEvent(ctx context.Context, eRec EventRecord) (id string, version uint32, err error)
	GetSnapshot(ctx context.Context, aggregateID string) (Snapshot, error)
	SaveSnapshot(ctx context.Context, snapshot Snapshot) error
	GetAggregateEvents(ctx context.Context, aggregateID string, snapVersion int) ([]Event, error)
	HasIdempotencyKey(ctx context.Context, aggregateID, idempotencyKey string) (bool, error)
	Forget(ctx context.Context, request ForgetRequest, forget func(kind string, body []byte) ([]byte, error)) error
}

type EventRecord struct {
	AggregateID    string
	Version        uint32
	AggregateType  string
	IdempotencyKey string
	Labels         map[string]interface{}
	CreatedAt      time.Time
	Details        []EventRecordDetail
}

type EventRecordDetail struct {
	Kind string
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

func WithLabels(labels map[string]interface{}) SaveOption {
	return func(o *Options) {
		o.Labels = labels
	}
}

type EventStorer interface {
	GetByID(ctx context.Context, aggregateID string) (Aggregater, error)
	Save(ctx context.Context, aggregate Aggregater, options ...SaveOption) error
	HasIdempotencyKey(ctx context.Context, aggregateID, idempotencyKey string) (bool, error)
	// Forget erases the values of the specified fields
	Forget(ctx context.Context, request ForgetRequest, forget func(interface{}) interface{}) error
}

var _ EventStorer = (*EventStore)(nil)

type Option func(*EventStore)

func WithCodec(codec Codec) Option {
	return func(r *EventStore) {
		r.codec = codec
	}
}

func WithUpcaster(upcaster Upcaster) Option {
	return func(r *EventStore) {
		r.upcaster = upcaster
	}
}

// NewEventStore creates a new instance of ESPostgreSQL
func NewEventStore(repo EsRepository, snapshotThreshold uint32, factory Factory, options ...Options) EventStore {
	return EventStore{
		store:             repo,
		snapshotThreshold: snapshotThreshold,
		factory:           factory,
		codec:             JsonCodec{},
	}
}

// EventSore -
type EventStore struct {
	store             EsRepository
	snapshotThreshold uint32
	upcaster          Upcaster
	factory           Factory
	codec             Codec
}

func (es EventStore) GetByID(ctx context.Context, aggregateID string) (Aggregater, error) {
	snap, err := es.store.GetSnapshot(ctx, aggregateID)
	if err != nil {
		return nil, err
	}
	var aggregate Aggregater
	if len(snap.Body) != 0 {
		a, err := es.DecodeAggregate(snap.AggregateType, snap.Body)
		if err != nil {
			return nil, err
		}
		aggregate = a.(Aggregater)
	}

	var events []Event
	if snap.AggregateID != "" {
		events, err = es.store.GetAggregateEvents(ctx, aggregateID, int(snap.AggregateVersion))
	} else {
		events, err = es.store.GetAggregateEvents(ctx, aggregateID, -1)
	}
	if err != nil {
		return nil, err
	}

	for _, v := range events {
		if aggregate == nil {
			a, err := es.DecodeAggregate(v.AggregateType, nil)
			if err != nil {
				return nil, err
			}
			aggregate = a.(Aggregater)
		}
		m := EventMetadata{
			AggregateVersion: v.AggregateVersion,
			CreatedAt:        v.CreatedAt,
		}
		e, err := es.DecodeEvent(v.Kind, v.Body)
		if err != nil {
			return nil, err
		}
		aggregate.ApplyChangeFromHistory(m, e)
	}

	return aggregate, nil
}

func (es EventStore) DecodeAggregate(kind string, body []byte) (Typer, error) {
	return Decode(es.factory, es.codec, es.upcaster, kind, body, false)
}

func (es EventStore) DecodeEvent(kind string, body []byte) (Typer, error) {
	return Decode(es.factory, es.codec, es.upcaster, kind, body, true)
}

func (es EventStore) Save(ctx context.Context, aggregate Aggregater, options ...SaveOption) (err error) {
	opts := Options{}
	for _, fn := range options {
		fn(&opts)
	}

	events := aggregate.GetEvents()
	eventsLen := len(events)
	if eventsLen == 0 {
		return nil
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
			Kind: e.GetType(),
			Body: body,
		}
	}

	rec := EventRecord{
		AggregateID:    aggregate.GetID(),
		Version:        aggregate.GetVersion(),
		AggregateType:  tName,
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
				return fmt.Errorf("Failed to create serialize snapshot: %w", err)
			}

			snap := Snapshot{
				ID:               id,
				AggregateID:      aggregate.GetID(),
				AggregateVersion: aggregate.GetVersion(),
				AggregateType:    aggregate.GetType(),
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

func (es EventStore) HasIdempotencyKey(ctx context.Context, aggregateID, idempotencyKey string) (bool, error) {
	return es.store.HasIdempotencyKey(ctx, aggregateID, idempotencyKey)
}

type ForgetRequest struct {
	AggregateID string
	EventKind   string
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
