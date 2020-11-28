package poller

import (
	"context"
	"time"

	"github.com/quintans/eventstore/feed"
	"github.com/quintans/eventstore/player"
	"github.com/quintans/eventstore/sink"
	"github.com/quintans/eventstore/store"
	log "github.com/sirupsen/logrus"
)

const (
	maxWait = time.Minute
)

type Poller struct {
	store        player.Repository
	pollInterval time.Duration
	limit        int
	partitions   int
	play         player.Player
	// lag to account for on same millisecond concurrent inserts and clock skews
	trailingLag time.Duration
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

func WithPartitions(partitions int) Option {
	return func(p *Poller) {
		if partitions > 0 {
			p.partitions = partitions
		}
	}
}

func New(repository player.Repository, options ...Option) Poller {
	p := Poller{
		pollInterval: player.TrailingLag,
		limit:        20,
		store:        repository,
	}

	for _, o := range options {
		o(&p)
	}

	p.play = player.New(repository, player.WithBatchSize(p.limit), player.WithTrailingLag(p.trailingLag))

	return p
}

func (p Poller) Poll(ctx context.Context, startOption player.StartOption, handler player.EventHandler, filters ...store.FilterOption) error {
	var afterEventID string
	var err error
	switch startOption.StartFrom() {
	case player.END:
		afterEventID, err = p.store.GetLastEventID(ctx, p.trailingLag, store.Filter{})
		if err != nil {
			return err
		}
	case player.BEGINNING:
	case player.SEQUENCE:
		afterEventID = startOption.AfterEventID()
	}
	return p.forward(ctx, afterEventID, handler, filters...)
}

func (p Poller) forward(ctx context.Context, afterEventID string, handler player.EventHandler, filters ...store.FilterOption) error {
	wait := p.pollInterval
	for {
		eid, err := p.play.Replay(ctx, handler, afterEventID, filters...)
		if err != nil {
			wait += 2 * wait
			if wait > maxWait {
				wait = maxWait
			}
			log.WithField("backoff", wait).
				WithError(err).
				Error("Failure retrieving events. Backing off.")
		} else {
			afterEventID = eid
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
func (p Poller) Feed(ctx context.Context, sinker sink.Sinker, filters ...store.FilterOption) error {
	afterEventID, _, err := feed.LastEventIDInSink(ctx, sinker, p.partitions)
	if err != nil {
		return err
	}

	log.Println("Starting to feed from event ID:", afterEventID)
	return p.forward(ctx, afterEventID, sinker.Sink, filters...)
}
