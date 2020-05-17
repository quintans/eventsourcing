package eventstore

import (
	"errors"
	"time"
)

var (
	ErrConcurrentModification = errors.New("Concurrent Modification")
)

type EventStore interface {
	GetByID(aggregateID string, aggregate Aggregater) error
	Save(aggregate Aggregater) error
	GetEventsStartingAt(eventId string) ([]Event, error)
	GetEventsStartingAtFor(eventId string, agregateTypes ...string) ([]Event, error)
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
	ID               int64
	AggregateID      string
	AggregateVersion int
	AggregateType    string
	Kind             string
	Body             []byte
	CreatedAt        time.Time
}
