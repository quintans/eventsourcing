package eventstore

import (
	"context"
	"errors"
	"time"

	"github.com/quintans/eventstore/common"
)

var (
	ErrConcurrentModification = errors.New("Concurrent Modification")
)

type Factory interface {
	New(kind string) (interface{}, error)
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
	Decode           func() (Eventer, error)
}

func (e Event) IsZero() bool {
	return e.ID == ""
}

type EsRepository interface {
	SaveEvent(ctx context.Context, eRec EventRecord) (id string, version uint32, err error)
	GetSnapshot(ctx context.Context, aggregateID string, aggregate Aggregater) error
	SaveSnapshot(ctx context.Context, aggregate Aggregater, eventID string) error
	GetAggregateEvents(ctx context.Context, aggregateID string, snapVersion int) ([]Event, error)
	HasIdempotencyKey(ctx context.Context, aggregateID, idempotencyKey string) (bool, error)
	Forget(ctx context.Context, request ForgetRequest, forget func(interface{}) interface{}) error
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
	Body Eventer
}

type Options struct {
	IdempotencyKey string
	// Labels tags the event. eg: {"geo": "EU"}
	Labels map[string]interface{}
}

type EventStorer interface {
	GetByID(ctx context.Context, aggregateID string, aggregate Aggregater) error
	Save(ctx context.Context, aggregate Aggregater, options Options) error
	HasIdempotencyKey(ctx context.Context, aggregateID, idempotencyKey string) (bool, error)
	// Forget erases the values of the specified fields
	Forget(ctx context.Context, request ForgetRequest, forget func(interface{}) interface{}) error
}

var _ EventStorer = (*EventStore)(nil)

// NewEventStore creates a new instance of ESPostgreSQL
func NewEventStore(repo EsRepository, snapshotThreshold uint32) EventStore {
	return EventStore{
		store:             repo,
		snapshotThreshold: snapshotThreshold,
	}
}

// EventSore -
type EventStore struct {
	store             EsRepository
	snapshotThreshold uint32
	upcaster          Upcaster
}

func (es *EventStore) SetUpcaster(upcaster Upcaster) {
	es.upcaster = upcaster
}

func (es EventStore) GetByID(ctx context.Context, aggregateID string, aggregate Aggregater) error {
	err := es.store.GetSnapshot(ctx, aggregateID, aggregate)
	if err != nil {
		return err
	}

	var events []Event
	if aggregate.GetID() != "" {
		events, err = es.store.GetAggregateEvents(ctx, aggregateID, int(aggregate.GetVersion()))
	} else {
		events, err = es.store.GetAggregateEvents(ctx, aggregateID, -1)
	}
	if err != nil {
		return err
	}

	for _, v := range events {
		m := EventMetadata{
			AggregateVersion: v.AggregateVersion,
			CreatedAt:        v.CreatedAt,
		}
		e, err := v.Decode()
		if err != nil {
			return err
		}
		if es.upcaster != nil {
			e = es.upcaster.Upcast(e)
		}
		aggregate.ApplyChangeFromHistory(m, e)
	}

	return nil
}

func (es EventStore) Save(ctx context.Context, aggregate Aggregater, options Options) (err error) {
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
		details[i] = EventRecordDetail{
			Kind: e.GetType(),
			Body: e,
		}
	}

	rec := EventRecord{
		AggregateID:    aggregate.GetID(),
		Version:        aggregate.GetVersion(),
		AggregateType:  tName,
		IdempotencyKey: options.IdempotencyKey,
		Labels:         options.Labels,
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
			err := es.store.SaveSnapshot(ctx, aggregate, id)
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
	return es.store.Forget(ctx, request, forget)
}
