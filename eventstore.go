package eventstore

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"time"

	"github.com/quintans/eventstore/common"
)

var (
	ErrConcurrentModification = errors.New("Concurrent Modification")
)

type Aggregater interface {
	GetID() string
	GetVersion() int
	SetVersion(int)
	GetEvents() []interface{}
	ClearEvents()
	ApplyChangeFromHistory(event Event) error
}

// Event represents the event data
type Event struct {
	ID               string      `json:"id,omitempty"`
	AggregateID      string      `json:"aggregate_id,omitempty"`
	AggregateVersion int         `json:"aggregate_version,omitempty"`
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
	SaveEvent(ctx context.Context, tName string, eRec []EventRecord, idempotencyKey string, labels []byte) (string, error)
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
	AggregateID string
	Version     int
	Name        string
	Body        []byte
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

var _ EventStorer = (*EventSore)(nil)

// NewEventStore creates a new instance of ESPostgreSQL
func NewEventStore(repo EsRepository, snapshotThreshold int) *EventSore {
	return &EventSore{
		repo:              repo,
		snapshotThreshold: snapshotThreshold,
	}
}

// EventSore -
type EventSore struct {
	repo              EsRepository
	snapshotThreshold int
}

func (es *EventSore) GetByID(ctx context.Context, aggregateID string, aggregate Aggregater) error {
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

func (es *EventSore) Save(ctx context.Context, aggregate Aggregater, options Options) (err error) {
	events := aggregate.GetEvents()
	if len(events) == 0 {
		return nil
	}

	tName := nameFor(aggregate)
	version := aggregate.GetVersion()
	oldVersion := version

	var eventID string
	defer func() {
		if err != nil {
			aggregate.SetVersion(oldVersion)
			return
		}
		if version > es.snapshotThreshold-1 {
			mod := oldVersion % es.snapshotThreshold
			delta := version - oldVersion + mod
			if delta >= es.snapshotThreshold {
				es.repo.SaveSnapshot(ctx, aggregate, eventID)
			}
		}
	}()

	labels, err := json.Marshal(options.Labels)
	if err != nil {
		return err
	}

	size := len(events)
	if size == 0 {
		return nil
	}
	ers := make([]EventRecord, size)
	for i := 0; i < size; i++ {
		e := events[i]
		version++
		aggregate.SetVersion(version)
		body, err := json.Marshal(e)
		if err != nil {
			return err
		}
		ers[i] = EventRecord{
			AggregateID: aggregate.GetID(),
			Version:     aggregate.GetVersion(),
			Name:        nameFor(e),
			Body:        body,
		}
	}
	eventID, err = es.repo.SaveEvent(ctx, tName, ers, options.IdempotencyKey, labels)
	if err != nil {
		return err
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

func (es *EventSore) HasIdempotencyKey(ctx context.Context, aggregateID, idempotencyKey string) (bool, error) {
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

func (es *EventSore) Forget(ctx context.Context, request ForgetRequest) error {
	return es.repo.Forget(ctx, request)
}
