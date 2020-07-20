package poller

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/quintans/eventstore/common"
	log "github.com/sirupsen/logrus"
)

var _ Repository = (*PgRepository)(nil)

type Locker interface {
	Lock() bool
	Unlock()
}

const (
	maxWait = time.Minute
	lag     = -200 * time.Millisecond
)

type Repository interface {
	GetLastEventID(ctx context.Context) (string, error)
	GetEventsForAggregate(ctx context.Context, afterEventID string, aggregateID string, limit int) ([]common.Event, error)
	GetEvents(ctx context.Context, afterEventID string, limit int, filter common.Filter) ([]common.Event, error)
}

type NoOpLock struct {
}

func (m NoOpLock) Lock() bool {
	return true
}

func (m NoOpLock) Unlock() {}

type Start int

const (
	END Start = iota
	BEGINNING
	SEQUENCE
)

type Cancel func()

type Option func(*Poller)

func WithDistributedLocker(locker Locker) Option {
	return func(p *Poller) {
		p.distLocker = locker
	}
}

func WithPollInterval(pi time.Duration) Option {
	return func(p *Poller) {
		p.pollInterval = pi
	}
}

func WithStartFrom(from Start) Option {
	return func(p *Poller) {
		p.startFrom = from
	}
}

func WithAfterEventID(eventID string) Option {
	return func(p *Poller) {
		p.afterEventID = eventID
		p.startFrom = SEQUENCE
	}
}

func WithFilter(filter common.Filter) Option {
	return func(p *Poller) {
		p.filter = filter
	}
}

func WithAggregateTypes(at ...string) Option {
	return func(p *Poller) {
		p.filter.AggregateTypes = at
	}
}

func WithLabelMap(labels map[string][]string) Option {
	return func(p *Poller) {
		p.filter.Labels = labels
	}
}

func WithLabels(labels ...common.Label) Option {
	return func(p *Poller) {
		if p.filter.Labels == nil {
			p.filter.Labels = map[string][]string{}
		}
		f := p.filter.Labels
		for _, v := range labels {
			a := f[v.Key]
			if a == nil {
				a = []string{}
			}
			f[v.Key] = append(a, v.Value)
		}
	}
}

func WithLimit(limit int) Option {
	return func(p *Poller) {
		if limit > 0 {
			p.limit = limit
		}
	}
}

func New(est Repository, options ...Option) *Poller {
	p := &Poller{
		est:          est,
		pollInterval: 500 * time.Millisecond,
		startFrom:    END,
		limit:        20,
		distLocker:   NoOpLock{},
		filter:       common.Filter{},
	}
	for _, o := range options {
		o(p)
	}
	return p
}

type Poller struct {
	est          Repository
	pollInterval time.Duration
	startFrom    Start
	afterEventID string
	filter       common.Filter
	limit        int
	distLocker   Locker
}

func (p *Poller) Handle(ctx context.Context, handler func(ctx context.Context, e common.Event)) error {
	var afterEventID string
	var err error
	switch p.startFrom {
	case END:
		afterEventID, err = p.est.GetLastEventID(ctx)
		if err != nil {
			return err
		}
	case BEGINNING:
	case SEQUENCE:
		afterEventID = p.afterEventID
	}

	wait := p.pollInterval
	for {
		select {
		case <-ctx.Done():
			return nil
		case _ = <-time.After(p.pollInterval):
			if p.distLocker.Lock() {
				defer p.distLocker.Unlock()

				eid, err := p.retrieve(ctx, handler, afterEventID, "")
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
			}
		}
	}
}

func (p *Poller) ReplayUntil(ctx context.Context, handler func(ctx context.Context, e common.Event), untilEventID string) (string, error) {
	return p.retrieve(ctx, handler, "", untilEventID)
}

func (p *Poller) ReplayFromUntil(ctx context.Context, handler func(ctx context.Context, e common.Event), afterEventID, untilEventID string) (string, error) {
	return p.retrieve(ctx, handler, afterEventID, untilEventID)
}

func (p *Poller) retrieve(ctx context.Context, handler func(ctx context.Context, e common.Event), afterEventID, untilEventID string) (string, error) {
	loop := true
	for loop {
		events, err := p.est.GetEvents(ctx, afterEventID, p.limit, p.filter)
		if err != nil {
			return "", err
		}
		for _, evt := range events {
			handler(ctx, evt)
			afterEventID = evt.ID
			if evt.ID == untilEventID {
				return untilEventID, nil
			}
		}
		loop = len(events) == p.limit
	}
	return afterEventID, nil
}

type PgRepository struct {
	db *sqlx.DB
}

// NewESPostgreSQL creates a new instance of ESPostgreSQL
func NewPgRepository(dburl string) (*PgRepository, error) {
	db, err := sqlx.Open("postgres", dburl)
	if err != nil {
		return nil, err
	}
	if err := db.Ping(); err != nil {
		return nil, err
	}

	return &PgRepository{
		db: db,
	}, nil
}

func (es *PgRepository) GetLastEventID(ctx context.Context) (string, error) {
	var eventID string
	safetyMargin := time.Now().Add(lag)
	if err := es.db.GetContext(ctx, &eventID, `
	SELECT * FROM events
	WHERE created_at <= $1'
	ORDER BY id DESC LIMIT 1
	`, safetyMargin); err != nil {
		if err != sql.ErrNoRows {
			return "", fmt.Errorf("Unable to get the last event ID: %w", err)
		}
	}
	return eventID, nil
}

func (es *PgRepository) GetEventsForAggregate(ctx context.Context, afterEventID string, aggregateID string, batchSize int) ([]common.Event, error) {
	events := []common.Event{}
	safetyMargin := time.Now().Add(lag)
	if err := es.db.SelectContext(ctx, &events, `
	SELECT * FROM events
	WHERE id > $1
	AND aggregate_id = $2
	AND created_at <= $3
	ORDER BY id ASC LIMIT $4
	`, afterEventID, aggregateID, safetyMargin, batchSize); err != nil {
		if err != sql.ErrNoRows {
			return nil, fmt.Errorf("Unable to get events after '%s': %w", afterEventID, err)
		}
	}
	return events, nil

}

func (es *PgRepository) GetEvents(ctx context.Context, afterEventID string, batchSize int, filter common.Filter) ([]common.Event, error) {
	safetyMargin := time.Now().Add(lag)
	args := []interface{}{afterEventID, safetyMargin}
	var query bytes.Buffer
	query.WriteString("SELECT * FROM events WHERE id > $1 AND created_at <= $2")
	if len(filter.AggregateTypes) > 0 {
		query.WriteString(" AND (")
		first := true
		for _, v := range filter.AggregateTypes {
			if !first {
				query.WriteString(" OR ")
			}
			first = false
			args = append(args, v)
			query.WriteString(fmt.Sprintf("aggregate_type = $%d", len(args)))
		}
		query.WriteString(")")
	}
	if len(filter.Labels) > 0 {
		for k, v := range filter.Labels {
			k = common.Escape(k)

			query.WriteString(" AND (")
			first := true
			for _, x := range v {
				if !first {
					query.WriteString(" OR ")
				}
				first = false
				x = common.Escape(x)
				query.WriteString(fmt.Sprintf(`labels  @> '{"%s": "%s"}'`, k, x))
			}
			query.WriteString(")")
		}
	}
	query.WriteString(" ORDER BY id ASC LIMIT ")
	query.WriteString(strconv.Itoa(batchSize))

	rows, err := es.queryEvents(ctx, query.String(), args)
	if err != nil {
		if err != sql.ErrNoRows {
			return nil, fmt.Errorf("Unable to get events after '%s' for filter %+v: %w", afterEventID, filter, err)
		}
	}
	return rows, nil
}

func (es *PgRepository) queryEvents(ctx context.Context, query string, args []interface{}) ([]common.Event, error) {
	rows, err := es.db.QueryxContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	events := []common.Event{}
	for rows.Next() {
		pg := common.PgEvent{}
		err := rows.StructScan(&pg)
		if err != nil {
			return nil, fmt.Errorf("Unable to scan to struct: %w", err)
		}
		events = append(events, common.Event{
			ID:               pg.ID,
			AggregateID:      pg.AggregateID,
			AggregateVersion: pg.AggregateVersion,
			AggregateType:    pg.AggregateType,
			Kind:             pg.Kind,
			Body:             pg.Body,
			Labels:           pg.Labels,
			CreatedAt:        pg.CreatedAt,
		})
	}
	return events, nil
}
