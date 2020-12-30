package postgresql

import (
	"context"
	"encoding/json"
	"strings"
	"time"

	"github.com/jackc/pgx/v4/pgxpool"
	log "github.com/sirupsen/logrus"

	"github.com/quintans/eventstore"
	"github.com/quintans/eventstore/common"
	"github.com/quintans/eventstore/encoding"
	"github.com/quintans/eventstore/eventid"
	"github.com/quintans/eventstore/player"
	"github.com/quintans/eventstore/sink"
	"github.com/quintans/eventstore/store"
	"github.com/quintans/toolkit/faults"
)

type FeedEvent struct {
	ID               string        `json:"id,omitempty"`
	AggregateID      string        `json:"aggregate_id,omitempty"`
	AggregateVersion uint32        `json:"aggregate_version,omitempty"`
	AggregateType    string        `json:"aggregate_type,omitempty"`
	Kind             string        `json:"kind,omitempty"`
	Body             encoding.Json `json:"body,omitempty"`
	IdempotencyKey   string        `json:"idempotency_key,omitempty"`
	Labels           encoding.Json `json:"labels,omitempty"`
	CreatedAt        PgTime        `json:"created_at,omitempty"`
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

type Feed struct {
	play           player.Player
	repository     player.Repository
	limit          int
	pool           *pgxpool.Pool
	offset         time.Duration
	channel        string
	aggregateTypes []string
	labels         store.Labels
	partitions     uint32
	partitionsLow  uint32
	partitionsHi   uint32
}

type FeedOption func(*Feed)

func WithLimit(limit int) FeedOption {
	return func(p *Feed) {
		if limit > 0 {
			p.limit = limit
		}
	}
}

func WithOffset(offset time.Duration) FeedOption {
	return func(p *Feed) {
		p.offset = offset
	}
}

func WithPartitions(partitions, partitionsLow, partitionsHi uint32) FeedOption {
	return func(f *Feed) {
		f.partitions = partitions
		f.partitionsLow = partitionsLow
		f.partitionsHi = partitionsHi
	}
}

// NewFeed instantiates a new PgListener.
// important:repo should NOT implement lag
func NewFeed(dbUrl string, repository player.Repository, channel string, options ...FeedOption) (Feed, error) {
	pool, err := pgxpool.Connect(context.Background(), dbUrl)
	if err != nil {
		return Feed{}, faults.Errorf("Unable to connect to database: %w", err)
	}

	p := Feed{
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
func (p Feed) Feed(ctx context.Context, sinker sink.Sinker) error {
	afterEventID, _, err := store.LastEventIDInSink(ctx, sinker, p.partitionsLow, p.partitionsHi)
	if err != nil {
		return err
	}

	log.Println("Starting to feed from event ID:", afterEventID)
	return p.forward(ctx, afterEventID, sinker.Sink)
}

func (p Feed) forward(ctx context.Context, afterEventID string, handler player.EventHandler) error {
	lastID := afterEventID
	for {
		conn, err := p.pool.Acquire(ctx)
		if err != nil {
			return faults.Errorf("Error acquiring connection: %w", err)
		}
		defer conn.Release()

		// start listening for events
		_, err = conn.Exec(ctx, "listen "+p.channel)
		if err != nil {
			return faults.Errorf("Error listening to %s channel: %w", p.channel, err)
		}

		// replay events applying a safety margin, in case we missed events
		lastID, err = eventid.DelayEventID(lastID, p.offset)
		if err != nil {
			return faults.Errorf("Error offsetting event ID: %w", err)
		}

		log.Infof("Replaying events from %s", lastID)
		filters := []store.FilterOption{
			store.WithAggregateTypes(p.aggregateTypes...),
			store.WithLabels(p.labels),
			store.WithPartitions(p.partitions, p.partitionsLow, p.partitionsHi),
		}
		lastID, err = p.play.Replay(ctx, handler, lastID, filters...)
		if err != nil {
			return faults.Errorf("Error replaying events: %w", err)
		}
		filter := store.Filter{}
		for _, f := range filters {
			f(&filter)
		}
		// remaining records due to the safety margin
		events, err := p.repository.GetEvents(ctx, lastID, 0, p.offset, filter)
		if err != nil {
			return faults.Errorf("Error getting all events events: %w", err)
		}
		for _, event := range events {
			err = handler(ctx, event)
			if err != nil {
				return faults.Errorf("Error handling event %+v: %w", event, err)
			}
			lastID = event.ID
		}

		// applying safety margin for messages inserted out of order - lag
		var retry bool
		lastID, retry, err = p.listen(ctx, conn, lastID, handler)
		if !retry {
			if err != nil {
				return faults.Errorf("Error while listening PostgreSQL: %w", err)
			}
			return nil
		}
		log.Warn("Error waiting for PostgreSQL notification: ", err)
	}
}

func (p Feed) listen(ctx context.Context, conn *pgxpool.Conn, thresholdID string, handler player.EventHandler) (lastID string, retry bool, err error) {
	defer conn.Release()

	log.Infof("Listening for PostgreSQL notifications on channel %s starting at %s", p.channel, thresholdID)
	for {
		msg, err := conn.Conn().WaitForNotification(ctx)
		select {
		case <-ctx.Done():
			return lastID, false, nil
		default:
			if err != nil {
				return lastID, true, faults.Errorf("Error waiting for notification: %w", err)
			}
		}

		// the event is JSON encoded
		pgEvent := FeedEvent{}
		err = json.Unmarshal([]byte(msg.Payload), &pgEvent)
		if err != nil {
			return "", false, faults.Errorf("Error unmarshalling Postgresql Event: %w", err)
		}
		lastID = pgEvent.ID

		if pgEvent.ID <= thresholdID {
			// ignore events already handled
			continue
		}

		// check if the event is to be forwarded to the sinker
		part := common.WhichPartition(pgEvent.AggregateID, p.partitions)
		if part < p.partitionsLow || part > p.partitionsHi {
			continue
		}

		labels := map[string]interface{}{}
		err = json.Unmarshal(pgEvent.Labels, &labels)
		if err != nil {
			return "", false, faults.Errorf("Unable unmarshal labels to map: %w", err)
		}
		event := eventstore.Event{
			ID:               pgEvent.ID,
			AggregateID:      pgEvent.AggregateID,
			AggregateVersion: pgEvent.AggregateVersion,
			AggregateType:    pgEvent.AggregateType,
			Kind:             pgEvent.Kind,
			Body:             []byte(pgEvent.Body),
			IdempotencyKey:   pgEvent.IdempotencyKey,
			Labels:           labels,
			CreatedAt:        time.Time(pgEvent.CreatedAt),
		}
		err = handler(ctx, event)
		if err != nil {
			return "", false, faults.Errorf("Error handling event %+v: %w", event, err)
		}
	}
}

func (p Feed) Close() {
	p.pool.Close()
}
