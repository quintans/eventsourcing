package store

import (
	"context"

	"github.com/quintans/eventsourcing"
	"github.com/quintans/faults"
)

type (
	Subscription func(context.Context, eventsourcing.Event) error
	EventBusMW   func(Subscription) Subscription
)

type Subscriber interface {
	Subscribe(Subscription)
}

type Publisher interface {
	Publish(context.Context, ...eventsourcing.Event) error
}

type EventBus struct {
	subscribers []Subscription
	coreMW      []EventBusMW
}

func New(mw ...EventBusMW) *EventBus {
	return &EventBus{
		coreMW: mw,
	}
}

// Subscribe register an handler subscription. The middlewares are executed in the reverse order
func (m *EventBus) Subscribe(handler Subscription, mw ...EventBusMW) {
	for _, m := range mw {
		handler = m(handler)
	}
	for _, m := range m.coreMW {
		handler = m(handler)
	}
	m.subscribers = append(m.subscribers, handler)
}

func (m EventBus) Publish(ctx context.Context, events ...eventsourcing.Event) error {
	for _, e := range events {
		for _, h := range m.subscribers {
			if err := h(ctx, e); err != nil {
				return faults.Wrap(err)
			}
		}
	}
	return nil
}
