package poller

import (
	"context"
	"log/slog"
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

type Repository[K eventsourcing.ID] interface {
	PendingEvents(ctx context.Context, batchSize int, filter store.Filter) ([]*eventsourcing.Event[K], error)
	AfterSink(ctx context.Context, eID eventid.EventID) error
}

type Poller[K eventsourcing.ID] struct {
	logger         *slog.Logger
	store          Repository[K]
	pullInterval   time.Duration
	limit          int
	aggregateKinds []eventsourcing.Kind
	metadata       store.MetadataFilter
	partitionsLow  uint32
	partitionsHi   uint32
}

type Option[K eventsourcing.ID] func(*Poller[K])

func WithPollInterval[K eventsourcing.ID](pollInterval time.Duration) Option[K] {
	return func(p *Poller[K]) {
		p.pullInterval = pollInterval
	}
}

func WithLimit[K eventsourcing.ID](limit int) Option[K] {
	return func(p *Poller[K]) {
		if limit > 0 {
			p.limit = limit
		}
	}
}

func WithPartitions[K eventsourcing.ID](partitionsLow, partitionsHi uint32) Option[K] {
	return func(p *Poller[K]) {
		p.partitionsLow = partitionsLow
		p.partitionsHi = partitionsHi
	}
}

func WithAggregateKinds[K eventsourcing.ID](at ...eventsourcing.Kind) Option[K] {
	return func(f *Poller[K]) {
		f.aggregateKinds = at
	}
}

func WithMetadataKV[K eventsourcing.ID](key string, values ...string) Option[K] {
	return func(f *Poller[K]) {
		if f.metadata == nil {
			f.metadata = store.MetadataFilter{}
		}
		f.metadata.Add(key, values...)
	}
}

func WithMetadata[K eventsourcing.ID](metadata store.MetadataFilter) Option[K] {
	return func(f *Poller[K]) {
		f.metadata = metadata
	}
}

func New[K eventsourcing.ID](logger *slog.Logger, repository Repository[K], options ...Option[K]) Poller[K] {
	p := Poller[K]{
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
func (p *Poller[K]) Feed(ctx context.Context, sinker sink.Sinker[K]) error {
	p.logger.Info("Starting poller feed")
	p.pull(ctx, sinker)
	p.logger.Info("Poller feed stopped")
	return nil
}

func (p *Poller[K]) pull(ctx context.Context, sinker sink.Sinker[K]) {
	wait := p.pullInterval

	for {
		now := time.Now()
		err := p.catchUp(ctx, sinker)
		if err != nil {
			wait += 2 * wait
			if wait > maxWait {
				wait = maxWait
			}
			p.logger.Error("Failure retrieving events. Backing off.",
				"backoff", wait,
				log.Err(err),
			)
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

func (p *Poller[K]) catchUp(ctx context.Context, sinker sink.Sinker[K]) error {
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
		events, err := p.store.PendingEvents(ctx, p.limit, filter)
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

func (p *Poller[K]) handle(ctx context.Context, e *eventsourcing.Event[K], sinker sink.Sinker[K]) error {
	err := sinker.Sink(ctx, e, sink.Meta{})
	if err != nil {
		return err
	}

	return p.store.AfterSink(ctx, e.ID)
}
