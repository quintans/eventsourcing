package eventsourcing

import (
	"errors"
	"reflect"

	"github.com/quintans/faults"
)

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

func NewRootAggregate[K ID](aggregate EventHandler, id K) RootAggregate[K] {
	return RootAggregate[K]{
		id:           id,
		eventHandler: aggregate,
	}
}

type RootAggregate[K ID] struct {
	id           K
	events       []Eventer
	eventHandler EventHandler
}

func (a *RootAggregate[K]) GetID() K {
	if a == nil {
		var zero K
		return zero
	}
	return a.id
}

func (a *RootAggregate[K]) PopEvents() []Eventer {
	evs := a.events
	a.events = nil
	return evs
}

func (a *RootAggregate[K]) ApplyChange(event Eventer) error {
	if err := a.eventHandler.HandleEvent(event); err != nil {
		return err
	}

	a.events = append(a.events, event)
	return nil
}

var (
	ErrHandlerNotFound    = errors.New("handler not found")
	ErrWrongHandlerInput  = errors.New("handler input must be one of type eventsourcing.Eventer")
	ErrWrongHandlerOutput = errors.New("handler output must be one of type error")
)

var (
	errorInterface   = reflect.TypeOf((*error)(nil)).Elem()
	eventerInterface = reflect.TypeOf((*Eventer)(nil)).Elem()
)

// HandlerCall calls event handlers method using reflection.
// The handler methods must have the signature Handle<Event>(Event) error.
//
//	type Created struct{}
//
//	type Foo struct{}
//	func (*Foo) HandleCreated(Created) error {
//		...
//	}
func HandlerCall[K ID](a Aggregater[K], event Eventer) error {
	var name string
	if t := reflect.TypeOf(event); t.Kind() == reflect.Ptr {
		name = t.Elem().Name()
	} else {
		name = t.Name()
	}
	methodName := "Handle" + name
	method := reflect.ValueOf(a).MethodByName(methodName)
	if (method == reflect.Value{}) {
		return faults.Errorf("handler method found for %s: %w", methodName, ErrHandlerNotFound)
	}
	mt := method.Type()
	if mt.NumIn() != 1 || !mt.In(0).Implements(eventerInterface) {
		return faults.Errorf("handler method %s: %w", methodName, ErrWrongHandlerInput)
	}
	if mt.NumOut() != 1 || !mt.Out(0).Implements(errorInterface) {
		return faults.Errorf("handler method %s: %w", methodName, ErrWrongHandlerOutput)
	}
	errVal := method.Call([]reflect.Value{reflect.ValueOf(event)})
	out := errVal[0]
	if out.IsZero() {
		return nil
	}
	return out.Interface().(error)
}
