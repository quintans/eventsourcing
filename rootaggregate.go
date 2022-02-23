package eventsourcing

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
	events       []Eventer
	eventHandler EventHandler
}

func (a RootAggregate) PopEvents() []Eventer {
	evs := a.events
	a.events = []Eventer{}
	return evs
}

func (a *RootAggregate) ApplyChangeFromHistory(event Eventer) {
	a.eventHandler.HandleEvent(event)
}

func (a *RootAggregate) ApplyChange(event Eventer) {
	a.ApplyChangeFromHistory(event)

	a.events = append(a.events, event)
}
