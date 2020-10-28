package eventstore

import (
	"context"
	"errors"
	"reflect"
	"time"

	"github.com/quintans/eventstore/common"
)

var (
	ErrConcurrentModification = errors.New("Concurrent Modification")
)

type DecodeFunc func(v interface{}) error

type Aggregater interface {
	GetType() string
	GetID() string
	GetVersion() uint32
	SetVersion(uint32)
	// GetEventsCounter used to determine snapshots threshold
	GetEventsCounter() uint32
	GetEvents() []interface{}
	ClearEvents()
	ApplyChangeFromHistory(event Event) error
	UpdatedAt() time.Time
}

// Event represents the event data
type Event struct {
	ID               string      `json:"id,omitempty"`
	ResumeToken      string      `json:"resume_token,omitempty"`
	AggregateID      string      `json:"aggregate_id,omitempty"`
	AggregateVersion uint32      `json:"aggregate_version,omitempty"`
	AggregateType    string      `json:"aggregate_type,omitempty"`
	Kind             string      `json:"kind,omitempty"`
	Body             common.Json `json:"body,omitempty"`
	IdempotencyKey   string      `json:"idempotency_key,omitempty"`
	Labels           common.Json `json:"labels,omitempty"`
	CreatedAt        time.Time   `json:"created_at,omitempty"`
	Decode           DecodeFunc  `json:"-"`
}

func (e Event) IsZero() bool {
	return e.ID == ""
}

type EsRepository interface {
	SaveEvent(ctx context.Context, eRec EventRecord) (id string, version uint32, err error)
	GetSnapshot(ctx context.Context, aggregateID string) (Snapshot, error)
	SaveSnapshot(ctx context.Context, agregate Aggregater, eventID string) error
	GetAggregateEvents(ctx context.Context, aggregateID string, snapVersion int) ([]Event, error)
	HasIdempotencyKey(ctx context.Context, aggregateID, idempotencyKey string) (bool, error)
	Forget(ctx context.Context, request ForgetRequest) error
}

type Snapshot struct {
	AggregateID      string
	AggregateVersion uint32
	Decode           DecodeFunc
}

func (s Snapshot) IsValid() bool {
	return s.AggregateID != ""
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
	Body interface{}
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
	Forget(ctx context.Context, request ForgetRequest) error
}

var _ EventStorer = (*EventStore)(nil)

// NewEventStore creates a new instance of ESPostgreSQL
func NewEventStore(repo EsRepository, snapshotThreshold uint32) EventStore {
	return EventStore{
		repo:              repo,
		snapshotThreshold: snapshotThreshold,
	}
}

// EventSore -
type EventStore struct {
	repo              EsRepository
	snapshotThreshold uint32
}

func (es EventStore) GetByID(ctx context.Context, aggregateID string, aggregate Aggregater) error {
	snap, err := es.repo.GetSnapshot(ctx, aggregateID)
	if err != nil {
		return err
	}

	var events []Event
	if snap.IsValid() {
		// lazy decode
		err = snap.Decode(aggregate)
		if err != nil {
			return err
		}
		events, err = es.repo.GetAggregateEvents(ctx, aggregateID, int(snap.AggregateVersion))
	} else {
		events, err = es.repo.GetAggregateEvents(ctx, aggregateID, -1)
	}
	if err != nil {
		return err
	}

	for _, v := range events {
		err = aggregate.ApplyChangeFromHistory(v)
		if err != nil {
			return err
		}
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
			Kind: nameFor(e),
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

	id, lastVersion, err := es.repo.SaveEvent(ctx, rec)
	if err != nil {
		return err
	}
	aggregate.SetVersion(lastVersion)

	newCounter := aggregate.GetEventsCounter()
	oldCounter := newCounter - uint32(eventsLen)
	if newCounter > es.snapshotThreshold-1 {
		mod := oldCounter % es.snapshotThreshold
		delta := newCounter - (oldCounter - mod)
		if delta >= es.snapshotThreshold {
			err := es.repo.SaveSnapshot(ctx, aggregate, id)
			if err != nil {
				return err
			}
		}
	}

	aggregate.ClearEvents()
	return nil
}

func nameFor(x interface{}) string {
	t := reflect.TypeOf(x)
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	return t.Name()
}

func (es EventStore) HasIdempotencyKey(ctx context.Context, aggregateID, idempotencyKey string) (bool, error) {
	return es.repo.HasIdempotencyKey(ctx, aggregateID, idempotencyKey)
}

type ForgetRequest struct {
	AggregateID     string
	AggregateFields []string
	Events          []EventKind
}

type EventKind struct {
	Kind   string
	Fields []string
}

func (es EventStore) Forget(ctx context.Context, request ForgetRequest) error {
	return es.repo.Forget(ctx, request)
}
