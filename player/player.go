package player

import (
	"context"
	"time"

	"github.com/quintans/eventstore"
	log "github.com/sirupsen/logrus"
)

const (
	maxWait = time.Minute
)

type Repository interface {
	GetLastEventID(ctx context.Context, filter Filter) (string, error)
	GetEvents(ctx context.Context, afterEventID string, limit int, filter Filter) ([]eventstore.Event, error)
}

type Filter struct {
	AggregateTypes []string
	// Labels filters on top of labels. Every key of the map is ANDed with every OR of the values
	// eg: [{"geo": "EU"}, {"geo": "USA"}, {"membership": "prime"}] equals to:  geo IN ("EU", "USA") AND membership = "prime"
	Labels []Label
}

func NewLabel(key, value string) Label {
	return Label{
		Key:   key,
		Value: value,
	}
}

type Label struct {
	Key   string
	Value string
}

type Start int

const (
	END Start = iota
	BEGINNING
	SEQUENCE
)

type EventHandler func(ctx context.Context, e eventstore.Event) error

type Cancel func()

type Option func(*Player)
type FilterOption func(*Filter)

func WithLimit(limit int) Option {
	return func(p *Player) {
		if limit > 0 {
			p.limit = limit
		}
	}
}

func New(repo Repository, options ...Option) *Player {
	p := &Player{
		repo:  repo,
		limit: 20,
	}
	for _, o := range options {
		o(p)
	}
	return p
}

type Player struct {
	repo  Repository
	limit int
}

type StartOption struct {
	startFrom    Start
	afterEventID string
}

func StartEnd() StartOption {
	return StartOption{
		startFrom: END,
	}
}

func StartBeginning() StartOption {
	return StartOption{
		startFrom: BEGINNING,
	}
}

func StartAt(afterEventID string) StartOption {
	return StartOption{
		startFrom:    SEQUENCE,
		afterEventID: afterEventID,
	}
}

func WithFilter(filter Filter) FilterOption {
	return func(f *Filter) {
		f = &filter
	}
}

func WithAggregateTypes(at ...string) FilterOption {
	return func(f *Filter) {
		f.AggregateTypes = at
	}
}

func WithLabel(key, value string) FilterOption {
	return func(f *Filter) {
		if f.Labels == nil {
			f.Labels = []Label{}
		}
		f.Labels = append(f.Labels, NewLabel(key, value))
	}
}

type Labels map[string]string

func WithLabels(labels Labels) FilterOption {
	return func(f *Filter) {
		f.Labels = make([]Label, len(labels))
		idx := 0
		for k, v := range labels {
			f.Labels[idx] = NewLabel(k, v)
			idx++
		}
	}
}

func (p *Player) Poll(ctx context.Context, pollInterval time.Duration, startOption StartOption, handler EventHandler, filters ...FilterOption) error {
	filter := Filter{}
	for _, f := range filters {
		f(&filter)
	}

	var afterEventID string
	var err error
	switch startOption.startFrom {
	case END:
		afterEventID, err = p.repo.GetLastEventID(ctx, Filter{})
		if err != nil {
			return err
		}
	case BEGINNING:
	case SEQUENCE:
		afterEventID = startOption.afterEventID
	}
	return p.handle(ctx, pollInterval, afterEventID, handler, filter)
}

func (p *Player) handle(ctx context.Context, pollInterval time.Duration, afterEventID string, handler EventHandler, filter Filter) error {
	wait := pollInterval
	for {
		eid, err := p.retrieve(ctx, filter, handler, afterEventID, "")
		if err != nil {
			wait += 2 * wait
			if wait > maxWait {
				wait = maxWait
			}
			log.WithField("backoff", wait).
				WithError(err).
				Error("Failure retrieving events. Backing off.")
		} else {
			afterEventID = eid
			wait = pollInterval
		}

		select {
		case <-ctx.Done():
			return nil
		case _ = <-time.After(pollInterval):
		}
	}
}

type Sink interface {
	LastEventID(ctx context.Context) (string, error)
	Send(ctx context.Context, e eventstore.Event) error
}

// Forward forwars the handling to a sink.
// eg: a message queue
func (p *Player) Forward(ctx context.Context, pollInterval time.Duration, sink Sink, filters ...FilterOption) error {
	filter := Filter{}
	for _, f := range filters {
		f(&filter)
	}

	id, err := sink.LastEventID(ctx)
	if err != nil {
		return err
	}
	return p.handle(ctx, pollInterval, id, sink.Send, filter)
}

func (p *Player) ReplayUntil(ctx context.Context, handler EventHandler, untilEventID string, filters ...FilterOption) (string, error) {
	filter := Filter{}
	for _, f := range filters {
		f(&filter)
	}
	return p.retrieve(ctx, filter, handler, "", untilEventID)
}

func (p *Player) ReplayFromUntil(ctx context.Context, handler EventHandler, afterEventID, untilEventID string, filters ...FilterOption) (string, error) {
	filter := Filter{}
	for _, f := range filters {
		f(&filter)
	}
	return p.retrieve(ctx, filter, handler, afterEventID, untilEventID)
}

func (p *Player) retrieve(ctx context.Context, filter Filter, handler EventHandler, afterEventID, untilEventID string) (string, error) {
	loop := true
	for loop {
		events, err := p.repo.GetEvents(ctx, afterEventID, p.limit, filter)
		if err != nil {
			return "", err
		}
		for _, evt := range events {
			err := handler(ctx, evt)
			if err != nil {
				return "", err
			}
			afterEventID = evt.ID
			if evt.ID == untilEventID {
				return untilEventID, nil
			}
		}
		loop = len(events) != 0
	}
	return afterEventID, nil
}
