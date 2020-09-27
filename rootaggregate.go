package eventstore

import (
	"encoding/json"
	"reflect"
	"strings"
	"time"
)

var methodHandlerPrefix = "Handle"

type Handler struct {
	eventType reflect.Type
	handle    func(event interface{})
}

// HandlersCache is a map of types to functions that will be used to route event sourcing events
type HandlersCache map[string]Handler

func NewRootAggregate(aggregate interface{}, id string, version uint32) RootAggregate {
	return RootAggregate{
		ID:       id,
		Version:  version,
		events:   []interface{}{},
		handlers: createHandlersCache(aggregate),
	}
}

func createHandlersCache(source interface{}) HandlersCache {
	sourceType := reflect.TypeOf(source)
	sourceValue := reflect.ValueOf(source)
	handlers := make(HandlersCache)

	methodCount := sourceType.NumMethod()
	for i := 0; i < methodCount; i++ {
		method := sourceType.Method(i)

		if strings.HasPrefix(method.Name, methodHandlerPrefix) {
			//   func (source *MySource) HandleMyEvent(e MyEvent).
			if method.Type.NumIn() == 2 {
				eventType := method.Type.In(1)
				handler := func(event interface{}) {
					eventValue := reflect.ValueOf(event)
					method.Func.Call([]reflect.Value{sourceValue, eventValue})
				}

				handlers[eventType.Name()] = Handler{
					eventType: eventType,
					handle:    handler,
				}
			}
		}
	}

	return handlers
}

type RootAggregate struct {
	ID      string `json:"id,omitempty"`
	Version uint32 `json:"version,omitempty"`

	events    []interface{}
	handlers  HandlersCache
	updatedAt time.Time
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

func (a RootAggregate) GetEvents() []interface{} {
	return a.events
}

func (a *RootAggregate) ClearEvents() {
	a.events = []interface{}{}
}

func (a *RootAggregate) ApplyChangeFromHistory(event Event) error {
	h, ok := a.handlers[event.Kind]
	if ok {
		evtPtr := reflect.New(h.eventType)
		if err := json.Unmarshal(event.Body, evtPtr.Interface()); err != nil {
			return err
		}
		h.handle(evtPtr.Elem().Interface())
	}
	a.Version = event.AggregateVersion
	a.updatedAt = event.CreatedAt
	return nil
}

func (a *RootAggregate) ApplyChange(event interface{}) {
	h, ok := a.handlers[nameFor(event)]
	if ok {
		h.handle(event)
	}
	a.events = append(a.events, event)
}

func (a RootAggregate) IsZero() bool {
	return a.ID == ""
}

func (a RootAggregate) UpdatedAt() time.Time {
	return a.updatedAt
}
