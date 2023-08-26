package projection

import (
	"context"

	"github.com/quintans/faults"

	"github.com/quintans/eventsourcing"
	"github.com/quintans/eventsourcing/eventid"
	"github.com/quintans/eventsourcing/sink"
	"github.com/quintans/eventsourcing/store"
)

type (
	MessageHandlerFunc func(ctx context.Context, e *sink.Message) error
)

type EventsRepository interface {
	GetEvents(ctx context.Context, afterEventID eventid.EventID, untilEventID eventid.EventID, batchSize int, filter store.Filter) ([]*eventsourcing.Event, error)
}

type Start int

const (
	END Start = iota
	BEGINNING
	SEQUENCE
)

const defEventsBatchSize = 1000

type Cancel func()

type Option func(*Player)

type Player struct {
	store        EventsRepository
	batchSize    int
	customFilter func(*eventsourcing.Event) bool
}

func WithBatchSize(batchSize int) Option {
	return func(p *Player) {
		if batchSize > 0 {
			p.batchSize = batchSize
		}
	}
}

func WithCustomFilter(fn func(events *eventsourcing.Event) bool) Option {
	return func(p *Player) {
		p.customFilter = fn
	}
}

// NewPlayer instantiates a new Player.
func NewPlayer(repository EventsRepository, options ...Option) Player {
	p := Player{
		store:     repository,
		batchSize: defEventsBatchSize,
	}

	for _, f := range options {
		f(&p)
	}

	return p
}

type StartOption struct {
	startFrom Start
	afterSeq  uint64
}

func (so StartOption) StartFrom() Start {
	return so.startFrom
}

func (so StartOption) AfterSeq() uint64 {
	return so.afterSeq
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

func StartAt(sequence uint64) StartOption {
	return StartOption{
		startFrom: SEQUENCE,
		afterSeq:  sequence,
	}
}

func (p Player) Replay(ctx context.Context, handler MessageHandlerFunc, afterEventID, untilEventID eventid.EventID, filters ...store.FilterOption) (eventid.EventID, error) {
	filter := store.Filter{}
	for _, f := range filters {
		f(&filter)
	}
	loop := true
	for loop {
		events, err := p.store.GetEvents(ctx, afterEventID, untilEventID, p.batchSize, filter)
		if err != nil {
			return eventid.Zero, err
		}
		for _, evt := range events {
			if p.customFilter == nil || p.customFilter(evt) {
				err := handler(ctx, sink.ToMessage(evt))
				if err != nil {
					return eventid.Zero, faults.Wrap(err)
				}
			}
			afterEventID = evt.ID

			if untilEventID != eventid.Zero && afterEventID.Compare(untilEventID) >= 0 {
				return afterEventID, nil
			}
		}
		loop = len(events) == p.batchSize
	}
	return afterEventID, nil
}
