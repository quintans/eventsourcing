package eventstore

import (
	"time"
)

type EventMetadata struct {
	AggregateVersion uint32
	CreatedAt        time.Time
}

type Typer interface {
	GetType() string
}

type Eventer interface {
	Typer
}

type EventHandler interface {
	HandleEvent(event Eventer)
}

func NewRootAggregate(aggregate EventHandler) RootAggregate {
	return RootAggregate{
		events:       []Eventer{},
		eventHandler: aggregate,
	}
}

type RootAggregate struct {
	ID            string `json:"id,omitempty"`
	Version       uint32 `json:"version,omitempty"`
	EventsCounter uint32 `json:"events_counter,omitempty"`

	events       []Eventer
	eventHandler EventHandler
	updatedAt    time.Time
}

func (a RootAggregate) GetID() string {
	return a.ID
}

func (a RootAggregate) GetVersion() uint32 {
	return a.Version
}

func (a *RootAggregate) SetVersion(version uint32) {
	a.Version = version
}

func (a RootAggregate) GetEventsCounter() uint32 {
	return a.EventsCounter
}

func (a RootAggregate) GetEvents() []Eventer {
	return a.events
}

func (a *RootAggregate) ClearEvents() {
	a.events = []Eventer{}
}

func (a *RootAggregate) ApplyChangeFromHistory(m EventMetadata, event Eventer) {
	a.eventHandler.HandleEvent(event)

	a.Version = m.AggregateVersion
	a.updatedAt = m.CreatedAt
	a.EventsCounter++
}

func (a *RootAggregate) ApplyChange(event Eventer) {
	a.eventHandler.HandleEvent(event)
	a.EventsCounter++

	a.events = append(a.events, event)
}

func (a RootAggregate) IsZero() bool {
	return a.ID == ""
}

func (a RootAggregate) UpdatedAt() time.Time {
	return a.updatedAt
}
