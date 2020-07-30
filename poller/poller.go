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

type EventHandler func(ctx context.Context, e common.Event) error

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

func New(repo Repository, options ...Option) *Poller {
	p := &Poller{
		repo:         repo,
		pollInterval: 500 * time.Millisecond,
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
	repo         Repository
	pollInterval time.Duration
	filter       common.Filter
	limit        int
	distLocker   Locker
}

type StartOption struct {
	startFrom    Start
	afterEventID string
}

func StartEnd() StartOption {
	return StartOption{
		startFrom: END,
	}
}

func StartBeginning() StartOption {
	return StartOption{
		startFrom: BEGINNING,
	}
}

func StartAt(afterEventID string) StartOption {
	return StartOption{
		startFrom:    SEQUENCE,
		afterEventID: afterEventID,
	}
}

func (p *Poller) Handle(ctx context.Context, startOption StartOption, handler EventHandler) error {
	var afterEventID string
	var err error
	switch startOption.startFrom {
	case END:
		afterEventID, err = p.repo.GetLastEventID(ctx)
		if err != nil {
			return err
		}
	case BEGINNING:
	case SEQUENCE:
		afterEventID = startOption.afterEventID
	}
	return p.handle(ctx, afterEventID, handler)
}

func (p *Poller) handle(ctx context.Context, afterEventID string, handler EventHandler) error {
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

type Sink interface {
	LastEventID(ctx context.Context) (string, error)
	Send(ctx context.Context, e common.Event) error
}

// Forward forwars the handling to a sink.
// eg: a message queue
func (p *Poller) Forward(ctx context.Context, sink Sink) error {
	id, err := sink.LastEventID(ctx)
	if err != nil {
		return err
	}
	return p.handle(ctx, id, sink.Send)
}

func (p *Poller) ReplayUntil(ctx context.Context, handler EventHandler, untilEventID string) (string, error) {
	return p.retrieve(ctx, handler, "", untilEventID)
}

func (p *Poller) ReplayFromUntil(ctx context.Context, handler EventHandler, afterEventID, untilEventID string) (string, error) {
	return p.retrieve(ctx, handler, afterEventID, untilEventID)
}

func (p *Poller) retrieve(ctx context.Context, handler EventHandler, afterEventID, untilEventID string) (string, error) {
	loop := true
	for loop {
		events, err := p.repo.GetEvents(ctx, afterEventID, p.limit, p.filter)
		if err != nil {
			return "", err
		}
		for _, evt := range events {
			err := handler(ctx, evt)
			if err != nil {
				return "", err
			}
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

// NewPgRepository creates a new instance of ESPostgreSQL
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
