package integration

import (
	"context"
	"slices"
	"sync"

	"github.com/quintans/eventsourcing/encoding/jsoncodec"
	"github.com/quintans/eventsourcing/projection"
	"github.com/quintans/eventsourcing/sink"
	"github.com/quintans/eventsourcing/test"
	"github.com/quintans/faults"
)

var _ projection.Projection = (*ProjectionMock)(nil)

type Balance struct {
	Name   string
	Amount int64
}

type ProjectionMock struct {
	name  string
	codec *jsoncodec.Codec

	mu               sync.Mutex
	balances         map[string]Balance
	aggregateVersion map[string]uint32
	events           []*sink.Message
}

func NewProjectionMock(name string) *ProjectionMock {
	return &ProjectionMock{
		name:             name,
		balances:         map[string]Balance{},
		codec:            test.NewJSONCodec(),
		aggregateVersion: map[string]uint32{},
	}
}

func (p *ProjectionMock) Name() string {
	return p.name
}

func (*ProjectionMock) CatchUpOptions() projection.CatchUpOptions {
	return projection.CatchUpOptions{}
}

func (p *ProjectionMock) Handle(ctx context.Context, e *sink.Message) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.events = append(p.events, e)

	// check if it was already handled
	if p.aggregateVersion[e.AggregateID] >= e.AggregateVersion {
		return nil
	}

	k, err := p.codec.Decode(e.Body, e.Kind)
	if err != nil {
		return faults.Wrap(err)
	}

	switch t := k.(type) {
	case *test.AccountCreated:
		p.balances[t.Id.String()] = Balance{
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

func (p *ProjectionMock) BalanceByID(id string) (Balance, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()

	b, ok := p.balances[id]
	return b, ok
}

func (p *ProjectionMock) Events() []*sink.Message {
	p.mu.Lock()
	defer p.mu.Unlock()

	return slices.Clone(p.events)
}
