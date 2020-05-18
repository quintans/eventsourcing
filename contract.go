package eventstore

import (
	"context"
	"errors"
	"time"
)

var (
	ErrConcurrentModification = errors.New("Concurrent Modification")
)

type EventStore interface {
	GetByID(ctx context.Context, aggregateID string, aggregate Aggregater) error
	Save(ctx context.Context, aggregate Aggregater) error
	GetEventsStartingAt(ctx context.Context, eventId string) ([]Event, error)
	GetEventsStartingAtFor(ctx context.Context, eventId string, agregateTypes ...string) ([]Event, error)
}

type Aggregater interface {
	GetID() string
	GetVersion() int
	SetVersion(int)
	GetEvents() []interface{}
	ClearEvents()
	ApplyChangeFromHistory(event Event) error
}

type Event struct {
	ID               string
	AggregateID      string
	AggregateVersion int
	AggregateType    string
	Kind             string
	Body             []byte
	CreatedAt        time.Time
}
