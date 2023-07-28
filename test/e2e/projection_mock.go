package e2e

import (
	"context"
	"sync"

	"github.com/quintans/eventsourcing/encoding/jsoncodec"
	"github.com/quintans/eventsourcing/projection"
	"github.com/quintans/eventsourcing/sink"
	"github.com/quintans/eventsourcing/test"
	"github.com/quintans/eventsourcing/util"
	"github.com/quintans/faults"
)

type Balance struct {
	Name   string
	Amount int64
}

type ProjectionMock struct {
	name  string
	codec *jsoncodec.Codec

	mu       sync.Mutex
	balances map[string]Balance
	resumes  map[string]projection.Token
}

func NewProjectionMock(name string) *ProjectionMock {
	return &ProjectionMock{
		name:     name,
		balances: map[string]Balance{},
		resumes:  map[string]projection.Token{},
		codec:    test.NewJSONCodec(),
	}
}

func (p *ProjectionMock) Name() string {
	return p.name
}

func (*ProjectionMock) Options() projection.Options {
	return projection.Options{}
}

func (p *ProjectionMock) GetStreamResumeToken(ctx context.Context, key projection.ResumeKey) (projection.Token, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	token, ok := p.resumes[key.String()]
	if !ok {
		return projection.Token{}, projection.ErrResumeTokenNotFound
	}

	return token, nil
}

func (p *ProjectionMock) Handle(ctx context.Context, meta projection.MetaData, e *sink.Message) error {
	p.mu.Lock()
	defer p.mu.Unlock()

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

	return p.recordResumeToken(meta)
}

func (p *ProjectionMock) recordResumeToken(meta projection.MetaData) error {
	// saving resume tokens. In a real application, it should be in the transaction

	topic, err := util.NewPartitionedTopic(meta.Topic, meta.Partition)
	if err != nil {
		return faults.Errorf("creating partitioned topic: %s:%d", meta.Topic, meta.Partition)
	}

	key, err := projection.NewResume(topic, p.Name())
	if err != nil {
		return faults.Wrap(err)
	}

	p.resumes[key.String()] = meta.Token

	return nil
}

func (p *ProjectionMock) BalanceByID(id string) (Balance, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()

	b, ok := p.balances[id]
	return b, ok
}
