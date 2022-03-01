package eventsourcing

type Kind string

func (a Kind) String() string {
	return string(a)
}

type Kinder interface {
	GetKind() Kind
}

type Eventer interface {
	Kinder
}

type EventHandler interface {
	HandleEvent(Eventer) error
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

func (a *RootAggregate) ApplyChange(event Eventer) error {
	if err := a.eventHandler.HandleEvent(event); err != nil {
		return err
	}

	a.events = append(a.events, event)
	return nil
}
