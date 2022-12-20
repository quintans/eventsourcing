package store

import (
	"context"
	"strings"

	"github.com/quintans/eventsourcing"
	"github.com/quintans/faults"
)

type (
	Subscription func(context.Context, eventsourcing.Event) error
	EventBusMW   func(Subscription) Subscription
)

type subscription struct {
	filter  string
	handler Subscription
}

type Subscriber interface {
	Subscribe(string, Subscription)
}

type Publisher interface {
	Publish(context.Context, ...eventsourcing.Event) error
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
	m.subscribers = append(m.subscribers, subscription{filter: filter, handler: handler})
}

func (m EventBus) Publish(ctx context.Context, events ...eventsourcing.Event) error {
	for _, e := range events {
		for _, s := range m.subscribers {
			if !match(s.filter, e.Kind.String()) {
				continue
			}
			if err := s.handler(ctx, e); err != nil {
				return faults.Wrap(err)
			}
		}
	}
	return nil
}

func match(filter, test string) bool {
	if filter == "*" {
		return true
	}

	idx := strings.Index(filter, "*")
	if idx > 0 && idx < len(test) {
		return filter[:idx] == test[:idx]
	}

	return filter == test
}
