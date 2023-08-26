package store

import (
	"context"
	"strings"

	"github.com/quintans/eventsourcing"
	"github.com/quintans/faults"
)

type (
	Subscription func(context.Context, *eventsourcing.Event) error
	EventBusMW   func(Subscription) Subscription
)

type subscription struct {
	filter  func(*eventsourcing.Event) bool
	handler Subscription
}

type Subscriber interface {
	Subscribe(string, Subscription)
}

type InTxHandler interface {
	Handle(context.Context, ...*eventsourcing.Event) error
}

type EventBus struct {
	subscribers []subscription
	coreMW      []EventBusMW
}

func New(mw ...EventBusMW) *EventBus {
	return &EventBus{
		coreMW: mw,
	}
}

// Subscribe register an handler subscription. The middlewares are executed in the reverse order
func (m *EventBus) Subscribe(filter string, handler Subscription, mw ...EventBusMW) {
	for _, m := range mw {
		handler = m(handler)
	}
	for _, m := range m.coreMW {
		handler = m(handler)
	}
	m.subscribers = append(m.subscribers, subscription{filter: match(filter), handler: handler})
}

func (m *EventBus) Publish(ctx context.Context, events ...*eventsourcing.Event) error {
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

func match(filter string) func(*eventsourcing.Event) bool {
	if filter == "*" {
		return func(*eventsourcing.Event) bool {
			return true
		}
	}

	idx := strings.Index(filter, "*")
	if idx > 0 {
		filter = filter[:idx]
		return func(e *eventsourcing.Event) bool {
			test := e.Kind.String()
			if idx < len(test) {
				return filter == test[:idx]
			}
			return false
		}
	}

	return func(e *eventsourcing.Event) bool {
		return filter == e.Kind.String()
	}
}
