package pglistener

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v4/pgxpool"
	log "github.com/sirupsen/logrus"

	"github.com/quintans/eventstore"
	"github.com/quintans/eventstore/common"
	"github.com/quintans/eventstore/eventid"
	"github.com/quintans/eventstore/feed"
	"github.com/quintans/eventstore/player"
	"github.com/quintans/eventstore/repo"
	"github.com/quintans/eventstore/sink"
)

type PgEvent struct {
	ID               string      `json:"id,omitempty"`
	AggregateID      string      `json:"aggregate_id,omitempty"`
	AggregateVersion uint32      `json:"aggregate_version,omitempty"`
	AggregateType    string      `json:"aggregate_type,omitempty"`
	Kind             string      `json:"kind,omitempty"`
	Body             common.Json `json:"body,omitempty"`
	IdempotencyKey   string      `json:"idempotency_key,omitempty"`
	Labels           common.Json `json:"labels,omitempty"`
	CreatedAt        PgTime      `json:"created_at,omitempty"`
}

type PgTime time.Time

func (pgt *PgTime) UnmarshalJSON(b []byte) error {
	s := string(b)
	// strip quotes
	s = s[1 : len(s)-1]
	if !strings.Contains(s, "Z") {
		s += "Z"
	}
	t, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		return err
	}
	*pgt = PgTime(t)
	return nil
}

type PgListener struct {
	play       player.Player
	repository player.Repository
	limit      int
	partitions int
	pool       *pgxpool.Pool
	offset     time.Duration
	channel    string
}

type Option func(*PgListener)

func WithLimit(limit int) Option {
	return func(p *PgListener) {
		if limit > 0 {
			p.limit = limit
		}
	}
}

func WithOffset(offset time.Duration) Option {
	return func(p *PgListener) {
		p.offset = offset
	}
}

func WithPartitions(partitions int) Option {
	return func(p *PgListener) {
		if partitions > 0 {
			p.partitions = partitions
		}
	}
}

// New instantiates a new PgListener.
// important:repo should NOT implement lag
func New(dbUrl string, repository player.Repository, channel string, options ...Option) (PgListener, error) {
	pool, err := pgxpool.Connect(context.Background(), dbUrl)
	if err != nil {
		return PgListener{}, fmt.Errorf("Unable to connect to database: %w", err)
	}

	p := PgListener{
		offset:     player.TrailingLag,
		limit:      20,
		repository: repository,
		pool:       pool,
		channel:    channel,
	}

	for _, o := range options {
		o(&p)
	}

	p.play = player.New(repository, player.WithBatchSize(p.limit), player.WithTrailingLag(p.offset))

	return p, nil
}

// Feed will forward messages to the sinker
// important: sinker.LastMessage should implement lag
func (p PgListener) Feed(ctx context.Context, sinker sink.Sinker, filters ...repo.FilterOption) error {
	afterEventID, _, err := feed.LastEventIDInSink(ctx, sinker, p.partitions)
	if err != nil {
		return err
	}

	log.Println("Starting to feed from event ID:", afterEventID)
	return p.forward(ctx, afterEventID, sinker.Sink, filters...)
}

func (p PgListener) forward(ctx context.Context, afterEventID string, handler player.EventHandler, filters ...repo.FilterOption) error {
	lastID := afterEventID
	for {
		conn, err := p.pool.Acquire(context.Background())
		if err != nil {
			return fmt.Errorf("Error acquiring connection: %w", err)
		}
		defer conn.Release()

		// start listening for events
		_, err = conn.Exec(context.Background(), "listen "+p.channel)
		if err != nil {
			return fmt.Errorf("Error listening to %s channel: %w", p.channel, err)
		}

		// replay events applying a safety margin, in case we missed events
		lastID, err = eventid.DelayEventID(lastID, p.offset)
		if err != nil {
			return fmt.Errorf("Error offsetting event ID: %w", err)
		}

		log.Infof("Replaying events from %s", lastID)
		lastID, err = p.play.Replay(ctx, handler, lastID, filters...)
		if err != nil {
			return fmt.Errorf("Error replaying events: %w", err)
		}
		filter := repo.Filter{}
		for _, f := range filters {
			f(&filter)
		}
		// remaining records due to the safety margin
		events, err := p.repository.GetEvents(ctx, lastID, 0, p.offset, filter)
		if err != nil {
			return fmt.Errorf("Error getting all events events: %w", err)
		}
		for _, event := range events {
			err = handler(ctx, event)
			if err != nil {
				return fmt.Errorf("Error handling event %+v: %w", event, err)
			}
			lastID = event.ID
		}

		// applying safety margin for messages inserted out of order - lag
		var retry bool
		lastID, retry, err = p.listen(ctx, conn, lastID, handler)
		if !retry {
			return fmt.Errorf("Error while listening PostgreSQL: %w", err)
		}
		log.Warn("Error waiting for PostgreSQL notification: ", err)
	}
}

func (p PgListener) listen(ctx context.Context, conn *pgxpool.Conn, thresholdID string, handler player.EventHandler) (lastID string, retry bool, err error) {
	defer conn.Release()

	log.Infof("Listening for PostgreSQL notifications on channel %s starting at %s", p.channel, thresholdID)
	for {
		msg, err := conn.Conn().WaitForNotification(ctx)
		if err != nil {
			return lastID, true, fmt.Errorf("Error waiting for notification: %w", err)
		}

		pgEvent := PgEvent{}
		err = json.Unmarshal([]byte(msg.Payload), &pgEvent)
		if err != nil {
			return "", false, fmt.Errorf("Error unmarshalling Postgresql Event: %w", err)
		}
		lastID = pgEvent.ID

		if pgEvent.ID <= thresholdID {
			// ignore events already handled
			continue
		}

		event := eventstore.Event{
			ID:               pgEvent.ID,
			AggregateID:      pgEvent.AggregateID,
			AggregateVersion: pgEvent.AggregateVersion,
			AggregateType:    pgEvent.AggregateType,
			Kind:             pgEvent.Kind,
			Body:             pgEvent.Body,
			IdempotencyKey:   pgEvent.IdempotencyKey,
			Labels:           pgEvent.Labels,
			CreatedAt:        time.Time(pgEvent.CreatedAt),
		}
		err = handler(ctx, event)
		if err != nil {
			return "", false, fmt.Errorf("Error handling event %+v: %w", event, err)
		}
	}
}
