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

func (p *ProjectionMock) StreamResumeToken(ctx context.Context, topic util.Topic) (projection.Token, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	token, ok := p.resumes[p.Name()+"-"+topic.String()]
	if !ok {
		return projection.Token{}, projection.ErrResumeTokenNotFound
	}

	return token, nil
}

func (p *ProjectionMock) Handler(ctx context.Context, meta projection.Meta, e *sink.Message) error {
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

func (p *ProjectionMock) recordResumeToken(meta projection.Meta) error {
	// saving resume tokens. In a real application, it should be in the transaction
	var topicRoot string
	if meta.Topic == "" {
		// when replaying, the topic is zero and we have to infer the topic
		topicRoot = "accounts"
	} else {
		topicRoot = meta.Topic
	}

	topic, err := util.NewPartitionedTopic(topicRoot, meta.Partition)
	if err != nil {
		return faults.Errorf("creating partitioned topic: %s:%d", topicRoot, meta.Partition)
	}

	p.resumes[p.Name()+"-"+topic.String()] = meta.Token

	return nil
}

func (p *ProjectionMock) BalanceByID(id string) (Balance, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()

	b, ok := p.balances[id]
	return b, ok
}
