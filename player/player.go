package player

import (
	"context"
	"time"

	"github.com/quintans/eventstore"
	"github.com/quintans/eventstore/repo"
)

const (
	TrailingLag = 200 * time.Millisecond
)

type Replayer interface {
	Replay(ctx context.Context, handler EventHandler, afterEventID string, filters ...repo.FilterOption) (string, error)
}

type Repository interface {
	GetLastEventID(ctx context.Context, trailingLag time.Duration, filter repo.Filter) (string, error)
	GetEvents(ctx context.Context, afterEventID string, limit int, trailingLag time.Duration, filter repo.Filter) ([]eventstore.Event, error)
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

type Player struct {
	repo      Repository
	batchSize int
	// lag to account for on same millisecond concurrent inserts and clock skews
	trailingLag time.Duration
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

// New instantiates a new Player.
//
// trailingLag: lag to account for on same millisecond concurrent inserts and clock skews. A good lag is 200ms.
func New(repository Repository, options ...Option) Player {
	p := Player{
		repo:        repository,
		batchSize:   20,
		trailingLag: TrailingLag,
	}

	for _, f := range options {
		f(&p)
	}

	return p
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

func (p Player) ReplayUntil(ctx context.Context, handler EventHandler, untilEventID string, filters ...repo.FilterOption) (string, error) {
	return p.ReplayFromUntil(ctx, handler, "", untilEventID, filters...)
}

func (p Player) Replay(ctx context.Context, handler EventHandler, afterEventID string, filters ...repo.FilterOption) (string, error) {
	return p.ReplayFromUntil(ctx, handler, afterEventID, "", filters...)
}

func (p Player) ReplayFromUntil(ctx context.Context, handler EventHandler, afterEventID, untilEventID string, filters ...repo.FilterOption) (string, error) {
	filter := repo.Filter{}
	for _, f := range filters {
		f(&filter)
	}
	loop := true
	for loop {
		events, err := p.repo.GetEvents(ctx, afterEventID, p.batchSize, p.trailingLag, filter)
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
