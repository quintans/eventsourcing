package poller

import (
	"context"
	"time"

	"github.com/quintans/eventstore/player"
	"github.com/quintans/eventstore/sink"
	log "github.com/sirupsen/logrus"
)

const (
	maxWait = time.Minute
)

type Poller struct {
	repo         player.Repository
	pollInterval time.Duration
	limit        int
	partitions   int
	play         player.Player
}

type Option func(*Poller)

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

func New(repo player.Repository, options ...Option) Poller {
	p := Poller{
		pollInterval: 200 * time.Millisecond,
		limit:        20,
		repo:         repo,
	}

	for _, o := range options {
		o(&p)
	}

	p.play = player.New(repo, p.limit)

	return p
}

func (p Poller) Poll(ctx context.Context, startOption player.StartOption, handler player.EventHandler, filters ...player.FilterOption) error {
	filter := player.Filter{}
	for _, f := range filters {
		f(&filter)
	}

	var afterEventID string
	var err error
	switch startOption.StartFrom() {
	case player.END:
		afterEventID, err = p.repo.GetLastEventID(ctx, player.Filter{})
		if err != nil {
			return err
		}
	case player.BEGINNING:
	case player.SEQUENCE:
		afterEventID = startOption.AfterEventID()
	}
	return p.handle(ctx, afterEventID, handler, filter)
}

func (p Poller) handle(ctx context.Context, afterEventID string, handler player.EventHandler, filter player.Filter) error {
	wait := p.pollInterval
	for {
		eid, err := p.play.Replay(ctx, filter, handler, afterEventID, "")
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

		select {
		case <-ctx.Done():
			return nil
		case _ = <-time.After(p.pollInterval):
		}
	}
}

// Forward forwars the handling to a sink.
// eg: a message queue
func (p Poller) Forward(ctx context.Context, sink sink.Sink, filters ...player.FilterOption) error {
	// looking for the lowest ID in all partitions
	var afterEventID string
	var err error
	if p.partitions == 0 {
		afterEventID, err = lastEventID(ctx, sink, 0)
		if err != nil {
			return err
		}
	} else {
		for i := 1; i <= p.partitions; i++ {
			afterEventID, err = lastEventID(ctx, sink, i)
			if err != nil {
				return err
			}
		}
	}

	filter := player.Filter{}
	for _, f := range filters {
		f(&filter)
	}

	return p.handle(ctx, afterEventID, sink.Send, filter)
}

func lastEventID(ctx context.Context, sink sink.Sink, partition int) (string, error) {
	afterEventID := "-"
	message, err := sink.LastMessage(ctx, partition)
	if err != nil {
		return "", err
	}
	eID := message.Event.ID
	if message != nil && (afterEventID == "-" || eID < afterEventID) {
		afterEventID = eID
	} else {
		afterEventID = ""
	}

	return afterEventID, nil
}
