package store

import (
	"context"
	"strings"

	"github.com/quintans/eventsourcing"
	"github.com/quintans/faults"
)

type (
	Subscription[K eventsourcing.ID] func(context.Context, *eventsourcing.Event[K]) error
	EventBusMW[K eventsourcing.ID]   func(Subscription[K]) Subscription[K]
)

type subscription[K eventsourcing.ID] struct {
	filter  func(*eventsourcing.Event[K]) bool
	handler Subscription[K]
}

type Subscriber[K eventsourcing.ID] interface {
	Subscribe(string, Subscription[K])
}

type InTxHandler[K eventsourcing.ID] func(context.Context, *eventsourcing.Event[K]) error

type EventBus[K eventsourcing.ID] struct {
	subscribers []subscription[K]
	coreMW      []EventBusMW[K]
}

func New[K eventsourcing.ID](mw ...EventBusMW[K]) *EventBus[K] {
	return &EventBus[K]{
		coreMW: mw,
	}
}

// Subscribe register an handler subscription. The middlewares are executed in the reverse order
func (m *EventBus[K]) Subscribe(filter string, handler Subscription[K], mw ...EventBusMW[K]) {
	for _, m := range mw {
		handler = m(handler)
	}
	for _, m := range m.coreMW {
		handler = m(handler)
	}
	m.subscribers = append(m.subscribers, subscription[K]{filter: match[K](filter), handler: handler})
}

func (m *EventBus[K]) Publish(ctx context.Context, events ...*eventsourcing.Event[K]) error {
	for _, e := range events {
		for _, s := range m.subscribers {
			if !s.filter(e) {
				continue
			}
			if err := s.handler(ctx, e); err != nil {
				return faults.Wrap(err)
			}
		}
	}
	return nil
}

func match[K eventsourcing.ID](filter string) func(*eventsourcing.Event[K]) bool {
	if filter == "*" {
		return func(*eventsourcing.Event[K]) bool {
			return true
		}
	}

	idx := strings.Index(filter, "*")
	if idx > 0 {
		filter = filter[:idx]
		return func(e *eventsourcing.Event[K]) bool {
			test := e.Kind.String()
			if idx < len(test) {
				return filter == test[:idx]
			}
			return false
		}
	}

	return func(e *eventsourcing.Event[K]) bool {
		return filter == e.Kind.String()
	}
}
