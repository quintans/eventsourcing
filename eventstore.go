package eventstore

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"time"

	"github.com/google/uuid"
	"github.com/quintans/eventstore/common"
	"github.com/quintans/eventstore/eventid"
)

var (
	ErrConcurrentModification = errors.New("Concurrent Modification")
)

type Aggregater interface {
	GetType() string
	GetID() string
	GetVersion() uint32
	SetVersion(uint32)
	GetEvents() []interface{}
	ClearEvents()
	ApplyChangeFromHistory(event Event) error
	UpdatedAt() time.Time
}

// Event represents the event data
type Event struct {
	ID               string      `json:"id,omitempty"`
	AggregateID      string      `json:"aggregate_id,omitempty"`
	AggregateVersion uint32      `json:"aggregate_version,omitempty"`
	AggregateType    string      `json:"aggregate_type,omitempty"`
	Kind             string      `json:"kind,omitempty"`
	Body             common.Json `json:"body,omitempty"`
	IdempotencyKey   string      `json:"idempotency_key,omitempty"`
	Labels           common.Json `json:"labels,omitempty"`
	CreatedAt        time.Time   `json:"created_at,omitempty"`
}

func (e Event) IsZero() bool {
	return e.ID == ""
}

type EsRepository interface {
	SaveEvent(ctx context.Context, eRec []EventRecord) error
	GetSnapshot(ctx context.Context, aggregateID string) (Snapshot, error)
	SaveSnapshot(ctx context.Context, agregate Aggregater, eventID string)
	GetAggregateEvents(ctx context.Context, aggregateID string, snapVersion int) ([]Event, error)
	HasIdempotencyKey(ctx context.Context, aggregateID, idempotencyKey string) (bool, error)
	Forget(ctx context.Context, request ForgetRequest) error
}

type Snapshot struct {
	AggregateID      string
	AggregateVersion int
	Body             []byte
}

func (s Snapshot) IsValid() bool {
	return s.AggregateID != ""
}

type EventRecord struct {
	AggregateID    string
	Version        uint32
	AggregateType  string
	Name           string
	Body           []byte
	IdempotencyKey string
	Labels         []byte
	CreatedAt      time.Time
}

func (e EventRecord) ID() string {
	id, _ := uuid.Parse(e.AggregateID)
	eid := eventid.New(e.CreatedAt, id, e.Version)
	return eid.String()
}

type Options struct {
	IdempotencyKey string
	// Labels tags the event. eg: {"geo": "EU"}
	Labels map[string]string
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
		err = json.Unmarshal(snap.Body, aggregate)
		if err != nil {
			return err
		}
		events, err = es.repo.GetAggregateEvents(ctx, aggregateID, snap.AggregateVersion)
	} else {
		events, err = es.repo.GetAggregateEvents(ctx, aggregateID, -1)
	}
	if err != nil {
		return err
	}

	for _, v := range events {
		aggregate.ApplyChangeFromHistory(Event{
			AggregateID:      v.AggregateID,
			AggregateVersion: v.AggregateVersion,
			AggregateType:    v.AggregateType,
			Kind:             v.Kind,
			Body:             v.Body,
			CreatedAt:        v.CreatedAt,
		})
	}

	return nil
}

func (es EventStore) Save(ctx context.Context, aggregate Aggregater, options Options) (err error) {
	events := aggregate.GetEvents()
	eventsLen := len(events)
	if eventsLen == 0 {
		return nil
	}

	labels, err := json.Marshal(options.Labels)
	if err != nil {
		return err
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
	version := aggregate.GetVersion()
	oldVersion := version

	var eventID string
	ers := make([]EventRecord, eventsLen)
	for i := 0; i < eventsLen; i++ {
		e := events[i]
		version++
		aggregate.SetVersion(version)
		body, err := json.Marshal(e)
		if err != nil {
			return err
		}
		ers[i] = EventRecord{
			AggregateID:    aggregate.GetID(),
			Version:        aggregate.GetVersion(),
			AggregateType:  tName,
			Name:           nameFor(e),
			Body:           body,
			IdempotencyKey: options.IdempotencyKey,
			Labels:         labels,
			CreatedAt:      now,
		}
		eventID = ers[i].ID()
	}

	err = es.repo.SaveEvent(ctx, ers)
	if err != nil {
		aggregate.SetVersion(oldVersion)
		return err
	}

	if version > es.snapshotThreshold-1 {
		mod := oldVersion % es.snapshotThreshold
		delta := version - oldVersion + mod
		if delta >= es.snapshotThreshold {
			es.repo.SaveSnapshot(ctx, aggregate, eventID)
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
