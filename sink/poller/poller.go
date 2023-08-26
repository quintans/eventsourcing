package poller

import (
	"context"
	"time"

	"github.com/quintans/eventsourcing"
	"github.com/quintans/eventsourcing/eventid"
	"github.com/quintans/eventsourcing/log"
	"github.com/quintans/eventsourcing/sink"
	"github.com/quintans/eventsourcing/store"
	"github.com/quintans/faults"
)

const (
	maxWait = time.Minute
)

type Repository interface {
	GetPendingEvents(ctx context.Context, batchSize int) ([]*eventsourcing.Event, error)
	SetSinkData(ctx context.Context, eID eventid.EventID) error
}

type Poller struct {
	logger         log.Logger
	store          Repository
	pullInterval   time.Duration
	limit          int
	aggregateKinds []eventsourcing.Kind
	metadata       store.Metadata
	partitionsLow  uint32
	partitionsHi   uint32
}

type Option func(*Poller)

func WithPollInterval(pollInterval time.Duration) Option {
	return func(p *Poller) {
		p.pullInterval = pollInterval
	}
}

func WithLimit(limit int) Option {
	return func(p *Poller) {
		if limit > 0 {
			p.limit = limit
		}
	}
}

func WithPartitions(partitionsLow, partitionsHi uint32) Option {
	return func(p *Poller) {
		p.partitionsLow = partitionsLow
		p.partitionsHi = partitionsHi
	}
}

func WithAggregateKinds(at ...eventsourcing.Kind) Option {
	return func(f *Poller) {
		f.aggregateKinds = at
	}
}

func WithMetadataKV(key, value string) Option {
	return func(f *Poller) {
		if f.metadata == nil {
			f.metadata = store.Metadata{}
		}
		values := f.metadata[key]
		if values == nil {
			values = []string{value}
		} else {
			values = append(values, value)
		}
		f.metadata[key] = values
	}
}

func WithMetadata(metadata store.Metadata) Option {
	return func(f *Poller) {
		f.metadata = metadata
	}
}

func New(logger log.Logger, repository Repository, options ...Option) Poller {
	p := Poller{
		logger:       logger,
		pullInterval: 200 * time.Millisecond,
		limit:        20,
		store:        repository,
	}

	for _, o := range options {
		o(&p)
	}

	return p
}

// Feed forwards the handling to a sink.
// eg: a message queue
func (p *Poller) Feed(ctx context.Context, sinker sink.Sinker) error {
	p.logger.Info("Starting poller feed")
	p.pull(ctx, sinker)
	p.logger.Info("Poller feed stopped")
	return nil
}

func (p *Poller) pull(ctx context.Context, sinker sink.Sinker) {
	wait := p.pullInterval

	for {
		now := time.Now()
		err := p.catchUp(ctx, sinker)
		if err != nil {
			wait += 2 * wait
			if wait > maxWait {
				wait = maxWait
			}
			p.logger.WithTags(log.Tags{"backoff": wait}).
				WithError(err).
				Error("Failure retrieving events. Backing off.")
		} else {
			wait = p.pullInterval - time.Since(now)
			if wait < 0 {
				wait = 0
			}
		}

		t := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			t.Stop()
			return
		case <-t.C:
		}
	}
}

func (p *Poller) catchUp(ctx context.Context, sinker sink.Sinker) error {
	filters := []store.FilterOption{
		store.WithAggregateKinds(p.aggregateKinds...),
		store.WithMetadata(p.metadata),
		store.WithPartitions(p.partitionsLow, p.partitionsHi),
	}
	filter := store.Filter{}
	for _, f := range filters {
		f(&filter)
	}
	loop := true
	for loop {
		events, err := p.store.GetPendingEvents(ctx, p.limit)
		if err != nil {
			return faults.Wrap(err)
		}
		for _, evt := range events {
			err = p.handle(ctx, evt, sinker)
			if err != nil {
				return faults.Wrap(err)
			}
		}
		loop = len(events) != 0
	}
	return nil
}

func (p *Poller) handle(ctx context.Context, e *eventsourcing.Event, sinker sink.Sinker) error {
	err := sinker.Sink(ctx, e, sink.Meta{})
	if err != nil {
		return err
	}

	return p.store.SetSinkData(ctx, e.ID)
}
