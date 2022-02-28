package eventsourcing

type Kind string

func (a Kind) String() string {
	return string(a)
}

type Kinder interface {
	GetType() Kind
}

type Eventer interface {
	Kinder
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

func (a *RootAggregate) ApplyChange(event Eventer) {
	a.eventHandler.HandleEvent(event)

	a.events = append(a.events, event)
}
