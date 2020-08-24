package player

import (
	"context"

	"github.com/quintans/eventstore"
)

type Connector interface {
	Play(ctx context.Context, startOption StartOption, handler EventHandler, filters ...FilterOption) error
}

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

type FilterOption func(*Filter)

type Player struct {
	repo      Repository
	batchSize int
}

func New(repo Repository, batchSize int) Player {
	if batchSize == 0 {
		batchSize = 20
	}
	return Player{
		repo:      repo,
		batchSize: batchSize,
	}
}

type StartOption struct {
	startFrom    Start
	afterEventID string
}

func (so StartOption) StartFrom() Start {
	return so.startFrom
}

func (so StartOption) AfterEventID() string {
	return so.afterEventID
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

func (p Player) ReplayUntil(ctx context.Context, handler EventHandler, untilEventID string, filters ...FilterOption) (string, error) {
	filter := Filter{}
	for _, f := range filters {
		f(&filter)
	}
	return p.Replay(ctx, filter, handler, "", untilEventID)
}

func (p Player) ReplayFromUntil(ctx context.Context, handler EventHandler, afterEventID, untilEventID string, filters ...FilterOption) (string, error) {
	filter := Filter{}
	for _, f := range filters {
		f(&filter)
	}
	return p.Replay(ctx, filter, handler, afterEventID, untilEventID)
}

func (p Player) Replay(ctx context.Context, filter Filter, handler EventHandler, afterEventID, untilEventID string) (string, error) {
	loop := true
	for loop {
		events, err := p.repo.GetEvents(ctx, afterEventID, p.batchSize, filter)
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
