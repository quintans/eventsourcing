package projection

import (
	"context"

	"github.com/quintans/faults"

	"github.com/quintans/eventsourcing"
	"github.com/quintans/eventsourcing/sink"
	"github.com/quintans/eventsourcing/store"
)

type (
	MessageHandlerFunc func(ctx context.Context, meta Meta, e *sink.Message) error
)

type Meta struct {
	Sequence uint64
}

type Repository interface {
	GetMaxSeq(ctx context.Context, filter store.Filter) (uint64, error)
	GetEvents(ctx context.Context, afterSeq uint64, batchSize int, filter store.Filter) ([]*eventsourcing.Event, error)
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
	store        Repository
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

// New instantiates a new Player.
func New(repository Repository, options ...Option) Player {
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

func (p Player) ReplayUntil(ctx context.Context, handler MessageHandlerFunc, untilSequence uint64, filters ...store.FilterOption) (uint64, error) {
	return p.ReplayFromUntil(ctx, handler, 0, untilSequence, filters...)
}

func (p Player) Replay(ctx context.Context, handler MessageHandlerFunc, afterEventID uint64, filters ...store.FilterOption) (uint64, error) {
	return p.ReplayFromUntil(ctx, handler, afterEventID, 0, filters...)
}

func (p Player) ReplayFromUntil(ctx context.Context, handler MessageHandlerFunc, afterSequence, untilSequence uint64, filters ...store.FilterOption) (uint64, error) {
	filter := store.Filter{}
	for _, f := range filters {
		f(&filter)
	}
	loop := true
	for loop {
		events, err := p.store.GetEvents(ctx, afterSequence, p.batchSize, filter)
		if err != nil {
			return 0, err
		}
		for _, evt := range events {
			if p.customFilter == nil || p.customFilter(evt) {
				err := handler(ctx, Meta{Sequence: evt.Sequence}, sink.ToMessage(evt, sink.Meta{}))
				if err != nil {
					return 0, faults.Wrap(err)
				}
			}
			afterSequence = evt.Sequence

			if untilSequence != 0 && evt.Sequence >= untilSequence {
				return evt.Sequence, nil
			}
		}
		loop = len(events) == p.batchSize
	}
	return afterSequence, nil
}
