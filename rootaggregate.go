package eventsourcing

import (
	"time"
)

type EventMetadata struct {
	AggregateVersion uint32
	CreatedAt        time.Time
}

type AggregateType string

func (a AggregateType) String() string {
	return string(a)
}

type EventKind string

func (e EventKind) String() string {
	return string(e)
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
	version       uint32
	eventsCounter uint32
	events        []Eventer
	eventHandler  EventHandler
	updatedAt     time.Time
}

func (a RootAggregate) GetVersion() uint32 {
	return a.version
}

func (a *RootAggregate) SetVersion(version uint32) {
	a.version = version
}

func (a RootAggregate) GetEventsCounter() uint32 {
	return a.eventsCounter
}

func (a RootAggregate) GetEvents() []Eventer {
	return a.events
}

func (a *RootAggregate) ClearEvents() {
	a.eventsCounter = 0
	a.events = []Eventer{}
}

func (a *RootAggregate) ApplyChangeFromHistory(m EventMetadata, event Eventer) {
	a.eventHandler.HandleEvent(event)

	a.version = m.AggregateVersion
	a.updatedAt = m.CreatedAt
	a.eventsCounter++
}

func (a *RootAggregate) ApplyChange(event Eventer) {
	a.eventHandler.HandleEvent(event)
	a.eventsCounter++

	a.events = append(a.events, event)
}

func (a RootAggregate) UpdatedAt() time.Time {
	return a.updatedAt
}
