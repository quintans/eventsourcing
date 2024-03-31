package integration

import (
	"context"
	"fmt"
	"slices"
	"sync"

	"github.com/oklog/ulid/v2"
	"github.com/quintans/eventsourcing"
	"github.com/quintans/eventsourcing/encoding/jsoncodec"
	"github.com/quintans/eventsourcing/eventid"
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

	mu          sync.Mutex
	balances    map[K]Balance
	checkpoints map[string]checkpoint
	events      []*sink.Message[K]
}

type checkpoint struct {
	EventID  eventid.EventID
	Sequence uint64
}

func NewProjectionMock[K eventsourcing.ID](name string, codec *jsoncodec.Codec[K]) *ProjectionMock[K] {
	return &ProjectionMock[K]{
		name:        name,
		balances:    map[K]Balance{},
		codec:       codec,
		checkpoints: map[string]checkpoint{},
	}
}

func (p *ProjectionMock[K]) Name() string {
	return p.name
}

func (*ProjectionMock[K]) CatchUpOptions() projection.CatchUpOptions {
	return projection.CatchUpOptions{}
}

func (p *ProjectionMock[K]) Handle(ctx context.Context, msg projection.Message[K]) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.events = append(p.events, msg.Message)

	e := msg.Message

	if p.reject(msg) {
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

	p.saveCheckpoint(msg)

	return nil
}

func (p *ProjectionMock[K]) reject(msg projection.Message[K]) bool {
	meta := msg.Meta

	if meta.Kind == projection.MessageKindSwitch {
		p.checkpoints = map[string]checkpoint{}
		p.checkpoints[meta.Name] = checkpoint{
			EventID: msg.Message.ID,
		}

		return true
	}

	if meta.Kind == projection.MessageKindCatchup {
		cp := p.checkpoints[fmt.Sprintf("%s-%d", meta.Name, meta.Partition)]
		return msg.Message.ID.Compare(cp.EventID) <= 0
	}

	cp, ok := p.checkpoints[fmt.Sprintf("%s-%d", meta.Name, meta.Partition)]
	if !ok {
		cp2 := p.checkpoints[meta.Name]
		return msg.Message.ID.Compare(cp2.EventID) <= 0
	}

	if msg.Message.ID.Compare(cp.EventID) <= 0 {
		return true
	}

	return msg.Meta.Sequence <= cp.Sequence
}

func (p *ProjectionMock[K]) saveCheckpoint(msg projection.Message[K]) {
	meta := msg.Meta

	switch meta.Kind {
	case projection.MessageKindCatchup:
		p.checkpoints[fmt.Sprintf("%s-%d", meta.Name, meta.Partition)] = checkpoint{
			EventID: msg.Message.ID,
		}

	case projection.MessageKindLive:
		cp := p.checkpoints[meta.Name]
		p.checkpoints[fmt.Sprintf("%s-%d", meta.Name, meta.Partition)] = checkpoint{
			// we save the last event ID from the catchup since it can be used be used to reject the first non catchup event
			EventID:  cp.EventID,
			Sequence: meta.Sequence,
		}
	}
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
