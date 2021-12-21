package postgresql

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"strings"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/quintans/faults"

	"github.com/quintans/eventsourcing"
	"github.com/quintans/eventsourcing/common"
	"github.com/quintans/eventsourcing/encoding"
	"github.com/quintans/eventsourcing/eventid"
	"github.com/quintans/eventsourcing/log"
	"github.com/quintans/eventsourcing/player"
	"github.com/quintans/eventsourcing/sink"
	"github.com/quintans/eventsourcing/store"
	"github.com/quintans/eventsourcing/worker"
)

type FeedEvent struct {
	ID               eventid.EventID             `json:"id,omitempty"`
	AggregateID      string                      `json:"aggregate_id,omitempty"`
	AggregateIDHash  uint32                      `json:"aggregate_id_hash,omitempty"`
	AggregateVersion uint32                      `json:"aggregate_version,omitempty"`
	AggregateType    eventsourcing.AggregateType `json:"aggregate_type,omitempty"`
	Kind             eventsourcing.EventKind     `json:"kind,omitempty"`
	Body             encoding.Json               `json:"body,omitempty"`
	IdempotencyKey   string                      `json:"idempotency_key,omitempty"`
	Metadata         *encoding.Json              `json:"metadata,omitempty"`
	CreatedAt        PgTime                      `json:"created_at,omitempty"`
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
		return faults.Wrap(err)
	}
	*pgt = PgTime(t)
	return nil
}

var _ worker.Tasker = (*Feed)(nil)

type Feed struct {
	logger         log.Logger
	play           player.Player
	repository     player.Repository
	limit          int
	dbURL          string
	offset         time.Duration
	channel        string
	aggregateTypes []eventsourcing.AggregateType
	metadata       store.Metadata
	partitions     uint32
	partitionsLow  uint32
	partitionsHi   uint32
	sinker         sink.Sinker
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
		if partitions <= 1 {
			return
		}
		f.partitions = partitions
		f.partitionsLow = partitionsLow
		f.partitionsHi = partitionsHi
	}
}

// NewFeedListenNotify instantiates a new PgListener.
// important:repo should NOT implement lag
func NewFeedListenNotify(logger log.Logger, connString string, repository player.Repository, channel string, sinker sink.Sinker, options ...FeedOption) Feed {
	p := Feed{
		logger:     logger,
		offset:     player.TrailingLag,
		limit:      20,
		repository: repository,
		dbURL:      connString,
		channel:    channel,
		sinker:     sinker,
	}

	for _, o := range options {
		o(&p)
	}

	p.play = player.New(repository, player.WithBatchSize(p.limit), player.WithTrailingLag(p.offset))

	return p
}

// Run will forward messages to the sinker
// important: sinker.LastMessage should implement lag
func (p Feed) Run(ctx context.Context) error {
	afterEventID := []byte{}
	err := store.ForEachResumeTokenInSinkPartitions(ctx, p.sinker, p.partitionsLow, p.partitionsHi, func(message *eventsourcing.Event) error {
		if bytes.Compare(message.ResumeToken, afterEventID) > 0 {
			afterEventID = message.ResumeToken
		}
		return nil
	})
	if err != nil {
		return err
	}

	pool, err := pgxpool.Connect(context.Background(), p.dbURL)
	if err != nil {
		return faults.Errorf("Unable to connect to '%s': %w", p.dbURL, err)
	}
	defer pool.Close()

	p.logger.Info("Starting to feed from event ID:", afterEventID)

	// TODO should be configured
	b := backoff.NewExponentialBackOff()
	b.MaxElapsedTime = 10 * time.Second

	lastID, err := eventid.Parse(string(afterEventID))
	if err != nil {
		return err
	}
	return backoff.Retry(func() error {
		var err error
		lastID, err = p.forward(ctx, pool, lastID, p.sinker, b)
		if errors.Is(err, context.Canceled) {
			return backoff.Permanent(err)
		}
		return err
	}, b)
}

func (Feed) Cancel(ctx context.Context, hard bool) {}

func (p Feed) forward(ctx context.Context, pool *pgxpool.Pool, afterEventID eventid.EventID, sinker sink.Sinker, b backoff.BackOff) (eventid.EventID, error) {
	lastID := afterEventID
	conn, err := pool.Acquire(ctx)
	if err != nil {
		return lastID, faults.Errorf("Error acquiring connection: %w", err)
	}
	defer conn.Release()

	// start listening for events
	_, err = conn.Exec(ctx, "listen "+p.channel)
	if err != nil {
		return lastID, faults.Errorf("Error listening to %s channel: %w", p.channel, err)
	}

	// replay events applying a safety margin, in case we missed events
	lastID = lastID.OffsetTime(-p.offset)

	p.logger.Infof("Replaying events from %s", lastID)
	filters := []store.FilterOption{
		store.WithAggregateTypes(p.aggregateTypes...),
		store.WithMetadata(p.metadata),
		store.WithPartitions(p.partitions, p.partitionsLow, p.partitionsHi),
	}
	lastID, err = p.play.Replay(ctx, sinker.Sink, lastID, filters...)
	if err != nil {
		return lastID, faults.Errorf("Error replaying events: %w", err)
	}
	filter := store.Filter{}
	for _, f := range filters {
		f(&filter)
	}
	// remaining records due to the safety margin
	events, err := p.repository.GetEvents(ctx, lastID, 0, p.offset, filter)
	if err != nil {
		return lastID, faults.Errorf("Error getting all events events: %w", err)
	}
	for _, event := range events {
		err = sinker.Sink(ctx, event)
		if err != nil {
			return lastID, faults.Errorf("Error handling event %+v: %w", event, backoff.Permanent(err))
		}
		lastID = event.ID
	}

	return p.listen(ctx, conn, lastID, sinker, b)
}

func (p Feed) listen(ctx context.Context, conn *pgxpool.Conn, thresholdID eventid.EventID, sinker sink.Sinker, b backoff.BackOff) (lastID eventid.EventID, err error) {
	defer conn.Release()

	p.logger.Infof("Listening for PostgreSQL notifications on channel %s starting at %s", p.channel, thresholdID)
	for {
		msg, err := conn.Conn().WaitForNotification(ctx)
		select {
		case <-ctx.Done():
			return lastID, backoff.Permanent(context.Canceled)
		default:
			if err != nil {
				return lastID, faults.Errorf("Error waiting for notification: %w", err)
			}
		}

		// the event is JSON encoded
		pgEvent := FeedEvent{}
		err = json.Unmarshal([]byte(msg.Payload), &pgEvent)
		if err != nil {
			return eventid.Zero, faults.Errorf("error unmarshalling Postgresql Event: %w", backoff.Permanent(err))
		}
		lastID = pgEvent.ID

		if pgEvent.ID.Compare(thresholdID) <= 0 {
			// ignore events already handled
			continue
		}

		// check if the event is to be forwarded to the sinker
		part := common.WhichPartition(pgEvent.AggregateIDHash, p.partitions)
		if part < p.partitionsLow || part > p.partitionsHi {
			continue
		}

		body, err := pgEvent.Body.AsBytes()
		if err != nil {
			return eventid.Zero, err
		}

		event := eventsourcing.Event{
			ID:               pgEvent.ID,
			ResumeToken:      []byte(pgEvent.ID.String()),
			AggregateID:      pgEvent.AggregateID,
			AggregateIDHash:  pgEvent.AggregateIDHash,
			AggregateVersion: pgEvent.AggregateVersion,
			AggregateType:    pgEvent.AggregateType,
			Kind:             pgEvent.Kind,
			Body:             body,
			IdempotencyKey:   pgEvent.IdempotencyKey,
			Metadata:         pgEvent.Metadata,
			CreatedAt:        time.Time(pgEvent.CreatedAt),
		}

		err = sinker.Sink(ctx, event)
		if err != nil {
			return eventid.Zero, faults.Errorf("Error handling event %+v: %w", event, backoff.Permanent(err))
		}

		b.Reset()
	}
}
