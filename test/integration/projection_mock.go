package integration

import (
	"context"
	"slices"
	"sync"

	"github.com/oklog/ulid/v2"
	"github.com/quintans/eventsourcing"
	"github.com/quintans/eventsourcing/encoding/jsoncodec"
	"github.com/quintans/eventsourcing/projection"
	"github.com/quintans/eventsourcing/sink"
	"github.com/quintans/eventsourcing/test"
	"github.com/quintans/faults"
)

var _ projection.Projection[ulid.ULID] = (*ProjectionMock[ulid.ULID])(nil)

type Balance struct {
	Name   string
	Amount int64
}

type ProjectionMock[K eventsourcing.ID] struct {
	name  string
	codec *jsoncodec.Codec[K]

	mu               sync.Mutex
	balances         map[K]Balance
	aggregateVersion map[K]uint32
	events           []*sink.Message[K]
}

func NewProjectionMock[K eventsourcing.ID](name string, codec *jsoncodec.Codec[K]) *ProjectionMock[K] {
	return &ProjectionMock[K]{
		name:             name,
		balances:         map[K]Balance{},
		codec:            codec,
		aggregateVersion: map[K]uint32{},
	}
}

func (p *ProjectionMock[K]) Name() string {
	return p.name
}

func (*ProjectionMock[K]) CatchUpOptions() projection.CatchUpOptions {
	return projection.CatchUpOptions{}
}

func (p *ProjectionMock[K]) Handle(ctx context.Context, e *sink.Message[K]) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.events = append(p.events, e)

	// check if it was already handled
	if p.aggregateVersion[e.AggregateID] >= e.AggregateVersion {
		return nil
	}

	k, err := p.codec.Decode(e.Body, eventsourcing.DecoderMeta[K]{Kind: e.Kind})
	if err != nil {
		return faults.Wrap(err)
	}

	switch t := k.(type) {
	case *test.AccountCreated:
		p.balances[e.AggregateID] = Balance{
			Name:   t.Owner,
			Amount: t.Money,
		}
	case *test.MoneyDeposited:
		b, ok := p.balances[e.AggregateID]
		if !ok {
			return faults.Errorf("aggregate not found while depositing: %s", e.AggregateID)
		}
		b.Amount += t.Money
		p.balances[e.AggregateID] = b
	case *test.MoneyWithdrawn:
		b, ok := p.balances[e.AggregateID]
		if !ok {
			return faults.Errorf("aggregate not found while withdrawing: %s", e.AggregateID)
		}
		b.Amount -= t.Money
		p.balances[e.AggregateID] = b
	default:
		return faults.Errorf("no handler for: %s -> %T", e.Kind, t)
	}

	// saving aggregates version
	p.aggregateVersion[e.AggregateID] = e.AggregateVersion

	return nil
}

func (p *ProjectionMock[K]) BalanceByID(id K) (Balance, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()

	b, ok := p.balances[id]
	return b, ok
}

func (p *ProjectionMock[K]) Events() []*sink.Message[K] {
	p.mu.Lock()
	defer p.mu.Unlock()

	return slices.Clone(p.events)
}
