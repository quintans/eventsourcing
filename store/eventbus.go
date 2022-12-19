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

type EventBus interface {
	Subscriber
	Publisher
}

type InMemEventBus struct {
	subscribers []Subscription
}

func New() *InMemEventBus {
	return &InMemEventBus{}
}

func (m *InMemEventBus) Subscribe(handler Subscription, mw ...EventBusMW) {
	for i := len(mw) - 1; i >= 0; i-- {
		handler = mw[i](handler)
	}
	m.subscribers = append(m.subscribers, handler)
}

func (m InMemEventBus) Publish(ctx context.Context, events ...eventsourcing.Event) error {
	for _, e := range events {
		for _, h := range m.subscribers {
			if err := h(ctx, e); err != nil {
				return faults.Wrap(err)
			}
		}
	}
	return nil
}
