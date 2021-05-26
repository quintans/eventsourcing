package player

import (
	"context"
	"time"

	"github.com/quintans/faults"

	"github.com/quintans/eventsourcing"
	"github.com/quintans/eventsourcing/eventid"
	"github.com/quintans/eventsourcing/store"
)

const (
	TrailingLag = 250 * time.Millisecond
)

type Replayer interface {
	Replay(ctx context.Context, handler EventHandlerFunc, afterEventID eventid.EventID, filters ...store.FilterOption) (string, error)
}

type Repository interface {
	GetLastEventID(ctx context.Context, trailingLag time.Duration, filter store.Filter) (eventid.EventID, error)
	GetEvents(ctx context.Context, afterMessageID eventid.EventID, limit int, trailingLag time.Duration, filter store.Filter) ([]eventsourcing.Event, error)
}

type Start int

const (
	END Start = iota
	BEGINNING
	SEQUENCE
)

type EventHandlerFunc func(ctx context.Context, e eventsourcing.Event) error

type Cancel func()

type Option func(*Player)

type Player struct {
	store     Repository
	batchSize int
	// lag to account for on same millisecond concurrent inserts and clock skews
	trailingLag  time.Duration
	customFilter func(eventsourcing.Event) bool
}

func WithBatchSize(batchSize int) Option {
	return func(p *Player) {
		if batchSize > 0 {
			p.batchSize = batchSize
		}
	}
}

func WithTrailingLag(trailingLag time.Duration) Option {
	return func(r *Player) {
		r.trailingLag = trailingLag
	}
}

func WithCustomFilter(fn func(events eventsourcing.Event) bool) Option {
	return func(p *Player) {
		p.customFilter = fn
	}
}

// New instantiates a new Player.
//
// trailingLag: lag to account for on same millisecond concurrent inserts and clock skews. A good lag is 200ms.
func New(repository Repository, options ...Option) Player {
	p := Player{
		store:       repository,
		batchSize:   20,
		trailingLag: TrailingLag,
	}

	for _, f := range options {
		f(&p)
	}

	return p
}

type StartOption struct {
	startFrom  Start
	afterMsgID eventid.EventID
}

func (so StartOption) StartFrom() Start {
	return so.startFrom
}

func (so StartOption) AfterMsgID() eventid.EventID {
	return so.afterMsgID
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

func StartAt(after eventid.EventID) StartOption {
	return StartOption{
		startFrom:  SEQUENCE,
		afterMsgID: after,
	}
}

func (p Player) ReplayUntil(ctx context.Context, handler EventHandlerFunc, untilEventID eventid.EventID, filters ...store.FilterOption) (eventid.EventID, error) {
	return p.ReplayFromUntil(ctx, handler, eventid.Zero, untilEventID, filters...)
}

func (p Player) Replay(ctx context.Context, handler EventHandlerFunc, afterEventID eventid.EventID, filters ...store.FilterOption) (eventid.EventID, error) {
	return p.ReplayFromUntil(ctx, handler, afterEventID, eventid.Zero, filters...)
}

func (p Player) ReplayFromUntil(ctx context.Context, handler EventHandlerFunc, afterEventID, untilEventID eventid.EventID, filters ...store.FilterOption) (eventid.EventID, error) {
	filter := store.Filter{}
	for _, f := range filters {
		f(&filter)
	}
	loop := true
	for loop {
		events, err := p.store.GetEvents(ctx, afterEventID, p.batchSize, p.trailingLag, filter)
		if err != nil {
			return eventid.Zero, err
		}
		for _, evt := range events {
			if p.customFilter == nil || p.customFilter(evt) {
				err := handler(ctx, evt)
				if err != nil {
					return eventid.Zero, faults.Wrap(err)
				}
			}
			afterEventID = evt.ID

			if !untilEventID.IsZero() && evt.ID.Compare(untilEventID) >= 0 {
				return evt.ID, nil
			}
		}
		loop = len(events) != 0
	}
	return afterEventID, nil
}
