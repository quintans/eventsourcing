package repo

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/lib/pq"
	"github.com/quintans/eventstore"
	"github.com/quintans/eventstore/common"
	log "github.com/sirupsen/logrus"
)

const (
	uniqueViolation = "23505"
)

// PgEvent is the event data stored in the database
type PgEvent struct {
	ID               string      `db:"id"`
	AggregateID      string      `db:"aggregate_id"`
	AggregateVersion uint32      `db:"aggregate_version"`
	AggregateType    string      `db:"aggregate_type"`
	Kind             string      `db:"kind"`
	Body             common.Json `db:"body"`
	IdempotencyKey   string      `db:"idempotency_key"`
	Labels           common.Json `db:"labels"`
	CreatedAt        time.Time   `db:"created_at"`
}

type PgEsRepository struct {
	db *sqlx.DB
}

func NewPgEsRepository(dburl string) (*PgEsRepository, error) {
	db, err := sql.Open("postgres", dburl)
	if err != nil {
		return nil, err
	}
	if err := db.Ping(); err != nil {
		return nil, err
	}
	return NewPgEsRepositoryDB(db)
}

// NewPgEsRepositoryDB creates a new instance of PgEsRepositoryDB
func NewPgEsRepositoryDB(db *sql.DB) (*PgEsRepository, error) {
	dbx := sqlx.NewDb(db, "postgres")
	r := &PgEsRepository{
		db: dbx,
	}

	return r, nil
}

func (r *PgEsRepository) SaveEvent(ctx context.Context, eRec []eventstore.EventRecord) error {
	er := r.withTx(ctx, func(c context.Context, tx *sql.Tx) error {
		var err error
		for _, e := range eRec {
			_, err = tx.ExecContext(c,
				`INSERT INTO events (id, aggregate_id, aggregate_version, aggregate_type, kind, body, idempotency_key, labels, created_at)
				VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`,
				e.ID(), e.AggregateID, e.Version, e.AggregateType, e.Name, e.Body, e.IdempotencyKey, e.Labels, e.CreatedAt)
			if err != nil {
				if pgerr, ok := err.(*pq.Error); ok {
					if pgerr.Code == uniqueViolation {
						return eventstore.ErrConcurrentModification
					}
				}
				return fmt.Errorf("Unable to insert event: %w", err)
			}
		}
		return nil
	})
	return er
}

func (r *PgEsRepository) GetSnapshot(ctx context.Context, aggregateID string) (eventstore.Snapshot, error) {
	snap := PgSnapshot{}
	if err := r.db.GetContext(ctx, &snap, "SELECT * FROM snapshots WHERE aggregate_id = $1 ORDER BY id DESC LIMIT 1", aggregateID); err != nil {
		if err == sql.ErrNoRows {
			return eventstore.Snapshot{}, nil
		}
		return eventstore.Snapshot{}, fmt.Errorf("Unable to get snapshot for aggregate '%s': %w", aggregateID, err)
	}
	return eventstore.Snapshot{}, nil
}

func (r *PgEsRepository) SaveSnapshot(ctx context.Context, aggregate eventstore.Aggregater, eventID string) {
	snap, err := buildSnapshot(aggregate, eventID)
	if err != nil {
		return
	}

	go func() {
		_, err = r.db.NamedExecContext(ctx,
			`INSERT INTO snapshots (id, aggregate_id, aggregate_version, body, created_at)
	     VALUES (:id, :aggregate_id, :aggregate_version, :body, :created_at)`, snap)

		if err != nil {
			log.WithField("snapshot", snap).
				WithError(err).
				Error("Failed to save snapshot")
		}
	}()
}

func buildSnapshot(agg eventstore.Aggregater, eventID string) (*PgSnapshot, error) {
	body, err := json.Marshal(agg)
	if err != nil {
		log.WithField("aggregate", agg).
			WithError(err).
			Error("Failed to serialize aggregate")
		return nil, err
	}

	return &PgSnapshot{
		ID:               eventID,
		AggregateID:      agg.GetID(),
		AggregateVersion: agg.GetVersion(),
		Body:             body,
		CreatedAt:        time.Now().UTC(),
	}, nil
}

func (r *PgEsRepository) GetAggregateEvents(ctx context.Context, aggregateID string, snapVersion int) ([]eventstore.Event, error) {
	query := "SELECT * FROM events e WHERE e.aggregate_id = $1"
	args := []interface{}{aggregateID}
	if snapVersion > -1 {
		query += " AND e.aggregate_version > $2"
		args = append(args, snapVersion)
	}
	pgEvents := []PgEvent{}
	if err := r.db.SelectContext(ctx, &pgEvents, query, args...); err != nil {
		if err == sql.ErrNoRows {
			return nil, fmt.Errorf("Aggregate '%s' was not found: %w", aggregateID, err)
		}
		return nil, fmt.Errorf("Unable to get events for Aggregate '%s': %w", aggregateID, err)
	}

	events := make([]eventstore.Event, len(pgEvents))
	for k, v := range pgEvents {
		events[k] = eventstore.Event{
			ID:               v.ID,
			AggregateID:      v.AggregateID,
			AggregateVersion: v.AggregateVersion,
			AggregateType:    v.AggregateType,
			Kind:             v.Kind,
			Body:             v.Body,
			IdempotencyKey:   v.IdempotencyKey,
			Labels:           v.Labels,
			CreatedAt:        v.CreatedAt,
		}
	}

	return events, nil
}

func (r *PgEsRepository) withTx(ctx context.Context, fn func(context.Context, *sql.Tx) error) (err error) {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		if r := recover(); r != nil {
			tx.Rollback()
			panic(r)
		}
		if err != nil {
			tx.Rollback()
		}
	}()
	err = fn(ctx, tx)
	if err != nil {
		return err
	}
	return tx.Commit()
}

func (r *PgEsRepository) HasIdempotencyKey(ctx context.Context, aggregateID, idempotencyKey string) (bool, error) {
	var exists int
	err := r.db.GetContext(ctx, &exists, `SELECT EXISTS(SELECT 1 FROM events WHERE idempotency_key=$1 AND aggregate_type=$2) AS "EXISTS"`, idempotencyKey, aggregateID)
	if err != nil {
		return false, fmt.Errorf("Unable to verify the existence of the idempotency key: %w", err)
	}
	return exists != 0, nil
}

func (r *PgEsRepository) Forget(ctx context.Context, request eventstore.ForgetRequest) error {
	for _, evt := range request.Events {
		sql := JoinAndEscape(evt.Fields)
		sql = fmt.Sprintf("UPDATE events SET body =  body - '{%s}'::text[] WHERE aggregate_id = $1 AND kind = $2", sql)
		_, err := r.db.ExecContext(ctx, sql, request.AggregateID, evt.Kind)
		if err != nil {
			return fmt.Errorf("Unable to forget events: %w", err)
		}
	}

	sql := JoinAndEscape(request.AggregateFields)
	sql = fmt.Sprintf("UPDATE snapshots SET body =  body - '{%s}'::text[] WHERE aggregate_id = $1", sql)
	_, err := r.db.ExecContext(ctx, sql, request.AggregateID)
	if err != nil {
		return fmt.Errorf("Unable to forget snapshots: %w", err)
	}

	return nil
}

type PgSnapshot struct {
	ID               string      `db:"id"`
	AggregateID      string      `db:"aggregate_id"`
	AggregateVersion uint32      `db:"aggregate_version"`
	Body             common.Json `db:"body"`
	CreatedAt        time.Time   `db:"created_at"`
}

func (r *PgEsRepository) GetLastEventID(ctx context.Context, trailingLag time.Duration, filter Filter) (string, error) {
	var query bytes.Buffer
	query.WriteString("SELECT * FROM events ")
	args := []interface{}{}
	if trailingLag != time.Duration(0) {
		safetyMargin := time.Now().UTC().Add(-trailingLag)
		args = append(args, safetyMargin)
		query.WriteString("created_at <= $1 ")
	}
	args = writeFilter(filter, &query, args)
	query.WriteString(" ORDER BY id DESC LIMIT 1")
	var eventID string
	if err := r.db.GetContext(ctx, &eventID, query.String(), args...); err != nil {
		if err != sql.ErrNoRows {
			return "", fmt.Errorf("Unable to get the last event ID: %w", err)
		}
	}
	return eventID, nil
}

func (r *PgEsRepository) GetEvents(ctx context.Context, afterEventID string, batchSize int, trailingLag time.Duration, filter Filter) ([]eventstore.Event, error) {
	args := []interface{}{afterEventID}
	var query bytes.Buffer
	query.WriteString("SELECT * FROM events WHERE id > $1 ")
	if trailingLag != time.Duration(0) {
		safetyMargin := time.Now().UTC().Add(-trailingLag)
		args = append(args, safetyMargin)
		query.WriteString("AND created_at <= $2 ")
	}
	args = writeFilter(filter, &query, args)
	query.WriteString(" ORDER BY id ASC")
	if batchSize > 0 {
		query.WriteString(" LIMIT ")
		query.WriteString(strconv.Itoa(batchSize))
	}

	rows, err := r.queryEvents(ctx, query.String(), args)
	if err != nil {
		if err != sql.ErrNoRows {
			return nil, fmt.Errorf("Unable to get events after '%s' for filter %+v: %w", afterEventID, filter, err)
		}
	}
	return rows, nil
}

func writeFilter(filter Filter, query *bytes.Buffer, args []interface{}) []interface{} {
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
		if len(filter.Labels) > 0 {
			first := true
			for _, v := range filter.Labels {
				k := Escape(v.Key)
				query.WriteString(" AND (")
				if !first {
					query.WriteString(" OR ")
				}
				first = false
				x := Escape(v.Value)
				query.WriteString(fmt.Sprintf(`labels  @> '{"%s": "%s"}'`, k, x))
				query.WriteString(")")
			}
		}
	}
	return args
}

func (r *PgEsRepository) queryEvents(ctx context.Context, query string, args []interface{}) ([]eventstore.Event, error) {
	rows, err := r.db.QueryxContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	events := []eventstore.Event{}
	for rows.Next() {
		pg := PgEvent{}
		err := rows.StructScan(&pg)
		if err != nil {
			return nil, fmt.Errorf("Unable to scan to struct: %w", err)
		}
		events = append(events, eventstore.Event{
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
