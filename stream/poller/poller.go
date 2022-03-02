package poller

import (
	"bytes"
	"context"
	"time"

	"github.com/quintans/eventsourcing"
	"github.com/quintans/eventsourcing/eventid"
	"github.com/quintans/eventsourcing/log"
	"github.com/quintans/eventsourcing/player"
	"github.com/quintans/eventsourcing/projection"
	"github.com/quintans/eventsourcing/sink"
	"github.com/quintans/eventsourcing/store"
)

const (
	maxWait = time.Minute
)

type Poller struct {
	logger       log.Logger
	store        player.Repository
	pollInterval time.Duration
	limit        int
	play         player.Player
	// lag to account for on same millisecond concurrent inserts and clock skews
	trailingLag    time.Duration
	aggregateKinds []eventsourcing.Kind
	metadata       store.Metadata
	partitions     uint32
	partitionsLow  uint32
	partitionsHi   uint32
}

type Option func(*Poller)

func WithTrailingLag(trailingLag time.Duration) Option {
	return func(r *Poller) {
		r.trailingLag = trailingLag
	}
}

func WithPollInterval(pollInterval time.Duration) Option {
	return func(p *Poller) {
		p.pollInterval = pollInterval
	}
}

func WithLimit(limit int) Option {
	return func(p *Poller) {
		if limit > 0 {
			p.limit = limit
		}
	}
}

func WithPartitions(partitions, partitionsLow, partitionsHi uint32) Option {
	return func(p *Poller) {
		p.partitions = partitions
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

func New(logger log.Logger, repository player.Repository, options ...Option) Poller {
	p := Poller{
		logger:       logger,
		pollInterval: 200 * time.Millisecond,
		trailingLag:  player.TrailingLag,
		limit:        20,
		store:        repository,
	}

	for _, o := range options {
		o(&p)
	}

	p.play = player.New(repository, player.WithBatchSize(p.limit), player.WithTrailingLag(p.trailingLag))

	return p
}

func (p Poller) Poll(ctx context.Context, startOption player.StartOption, handler projection.EventHandlerFunc) error {
	var afterMsgID eventid.EventID
	var err error
	switch startOption.StartFrom() {
	case player.END:
		afterMsgID, err = p.store.GetLastEventID(ctx, p.trailingLag, store.Filter{})
		if err != nil {
			return err
		}
	case player.BEGINNING:
	case player.SEQUENCE:
		afterMsgID = startOption.AfterMsgID()
	}
	return p.forward(ctx, afterMsgID, handler)
}

func (p Poller) forward(ctx context.Context, after eventid.EventID, handler projection.EventHandlerFunc) error {
	wait := p.pollInterval
	filters := []store.FilterOption{
		store.WithAggregateKinds(p.aggregateKinds...),
		store.WithMetadata(p.metadata),
		store.WithPartitions(p.partitions, p.partitionsLow, p.partitionsHi),
	}
	for {
		eid, err := p.play.Replay(ctx, handler, after, filters...)
		if err != nil {
			wait += 2 * wait
			if wait > maxWait {
				wait = maxWait
			}
			p.logger.WithTags(log.Tags{"backoff": wait}).
				WithError(err).
				Error("Failure retrieving events. Backing off.")
		} else {
			after = eid
			wait = p.pollInterval
		}

		t := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			t.Stop()
			return nil
		case <-t.C:
		}
	}
}

// Feed forwars the handling to a sink.
// eg: a message queue
func (p Poller) Feed(ctx context.Context, sinker sink.Sinker) error {
	var afterEventID []byte
	err := store.ForEachResumeTokenInSinkPartitions(ctx, sinker, p.partitionsLow, p.partitionsHi, func(message *eventsourcing.Event) error {
		if bytes.Compare(message.ResumeToken, afterEventID) > 0 {
			afterEventID = message.ResumeToken
		}
		return nil
	})
	if err != nil {
		return err
	}

	eID, err := eventid.Parse(string(afterEventID))
	if err != nil {
		return err
	}

	p.logger.Info("Starting to feed from event ID: ", afterEventID)
	return p.forward(ctx, eID, func(ctx context.Context, e eventsourcing.Event) error {
		e.ResumeToken = []byte(e.ID.String())
		return sinker.Sink(ctx, e)
	})
}
