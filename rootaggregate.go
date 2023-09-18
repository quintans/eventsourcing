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

type Handler[E any] func(E)

func NewRootAggregate[K ID](id K) RootAggregate[K] {
	return RootAggregate[K]{
		_id:       id,
		_registry: map[Kind]func(Eventer){},
	}
}

type RootAggregate[K ID] struct {
	_id       K
	_events   []Eventer
	_registry map[Kind]func(Eventer)
}

func (a *RootAggregate[K]) GetID() K {
	if a == nil {
		var zero K
		return zero
	}
	return a._id
}

func (a *RootAggregate[K]) PopEvents() []Eventer {
	evs := a._events
	a._events = nil
	return evs
}

func EventHandler[E Eventer](r EventRegister, handler Handler[E]) {
	var zero E
	r.RegisterEvent(zero.GetKind(), func(e Eventer) {
		handler(e.(E))
	})
}

type EventRegister interface {
	RegisterEvent(kind Kind, handler func(e Eventer))
}

func (a *RootAggregate[K]) RegisterEvent(kind Kind, handler func(e Eventer)) {
	a._registry[kind] = handler
}

func (a *RootAggregate[K]) handlerCall(event Eventer) error {
	handler := a._registry[event.GetKind()]
	if handler == nil {
		return faults.Errorf("handler method found for %s: %w", event.GetKind(), ErrHandlerNotFound)
	}
	handler(event)

	return nil
}

func (a *RootAggregate[K]) ApplyChange(event Eventer) error {
	if err := a.handlerCall(event); err != nil {
		return err
	}

	a._events = append(a._events, event)
	return nil
}

func (a *RootAggregate[K]) HandleEvent(event Eventer) error {
	return a.handlerCall(event)
}

var (
	ErrHandlerNotFound    = errors.New("handler not found")
	ErrWrongHandlerInput  = errors.New("handler input must be one of type eventsourcing.Eventer")
	ErrWrongHandlerOutput = errors.New("handler should not return anything")
)

var eventerInterface = reflect.TypeOf((*Eventer)(nil)).Elem()

// HandlerCall calls event handlers method using reflection.
// The handler methods must have the signature Handle<Event>(Event).
//
//	type Created struct{}
//
//	type Foo struct{}
//	func (*Foo) HandleCreated(Created) {
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
	if mt.NumOut() != 0 {
		return faults.Errorf("handler method %s: %w", methodName, ErrWrongHandlerOutput)
	}
	_ = method.Call([]reflect.Value{reflect.ValueOf(event)})
	return nil
}
