package repo

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/lib/pq"
	"github.com/quintans/eventstore"
	"github.com/quintans/eventstore/common"
)

const (
	pgUniqueViolation = "23505"
)

// PgEvent is the event data stored in the database
type PgEvent struct {
	ID               string    `db:"id"`
	AggregateID      string    `db:"aggregate_id"`
	AggregateVersion uint32    `db:"aggregate_version"`
	AggregateType    string    `db:"aggregate_type"`
	Kind             string    `db:"kind"`
	Body             []byte    `db:"body"`
	IdempotencyKey   string    `db:"idempotency_key"`
	Labels           []byte    `db:"labels"`
	CreatedAt        time.Time `db:"created_at"`
}

type PgSnapshot struct {
	ID               string    `db:"id,omitempty"`
	AggregateID      string    `db:"aggregate_id,omitempty"`
	AggregateVersion uint32    `db:"aggregate_version,omitempty"`
	AggregateType    string    `db:"aggregate_type,omitempty"`
	Body             []byte    `db:"body,omitempty"`
	CreatedAt        time.Time `db:"created_at,omitempty"`
}

var _ eventstore.EsRepository = (*PgEsRepository)(nil)

type PgEsRepository struct {
	db      *sqlx.DB
	factory eventstore.Factory
	codec   eventstore.Codec
}

func NewPgEsRepository(dburl string, factory eventstore.Factory) (*PgEsRepository, error) {
	db, err := sql.Open("postgres", dburl)
	if err != nil {
		return nil, err
	}
	return NewPgEsRepositoryDB(db, factory)
}

// NewPgEsRepositoryDB creates a new instance of PgEsRepository
func NewPgEsRepositoryDB(db *sql.DB, factory eventstore.Factory) (*PgEsRepository, error) {
	dbx := sqlx.NewDb(db, "postgres")
	r := &PgEsRepository{
		db:      dbx,
		factory: factory,
		codec:   eventstore.JsonCodec{},
	}

	return r, nil
}

func (r *PgEsRepository) SetCodec(codec eventstore.Codec) {
	r.codec = codec
}

func (r *PgEsRepository) SaveEvent(ctx context.Context, eRec eventstore.EventRecord) (string, uint32, error) {
	labels, err := json.Marshal(eRec.Labels)
	if err != nil {
		return "", 0, err
	}

	version := eRec.Version
	var id string
	err = r.withTx(ctx, func(c context.Context, s *sql.Tx) error {
		for _, e := range eRec.Details {
			body, err := r.codec.Encode(e.Body)
			if err != nil {
				return err
			}

			version++
			id = common.NewEventID(eRec.CreatedAt, eRec.AggregateID, version)
			_, err = r.db.ExecContext(ctx,
				`INSERT INTO events (id, aggregate_id, aggregate_version, aggregate_type, kind, body, idempotency_key, labels, created_at)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`,
				id, eRec.AggregateID, version, eRec.AggregateType, e.Kind, body, eRec.IdempotencyKey, labels, eRec.CreatedAt)

			if err != nil {
				if isPgDup(err) {
					return eventstore.ErrConcurrentModification
				}
				return fmt.Errorf("Unable to insert event: %w", err)
			}
		}

		return nil
	})
	if err != nil {
		return "", 0, err
	}

	return id, version, nil
}

func isPgDup(err error) bool {
	if pgerr, ok := err.(*pq.Error); ok {
		if pgerr.Code == pgUniqueViolation {
			return true
		}
	}
	return false
}

func (r *PgEsRepository) GetSnapshot(ctx context.Context, aggregateID string, aggregate eventstore.Aggregater) error {
	snap := PgSnapshot{}
	if err := r.db.GetContext(ctx, &snap, "SELECT * FROM snapshots WHERE aggregate_id = $1 ORDER BY id DESC LIMIT 1", aggregateID); err != nil {
		if err == sql.ErrNoRows {
			return nil
		}
		return fmt.Errorf("Unable to get snapshot for aggregate '%s': %w", aggregateID, err)
	}
	return r.codec.Decode(snap.Body, aggregate)
}

func (r *PgEsRepository) SaveSnapshot(ctx context.Context, aggregate eventstore.Aggregater, eventID string) error {
	body, err := r.codec.Encode(aggregate)
	if err != nil {
		return fmt.Errorf("Failed to create serialize snapshot: %w", err)
	}

	snap := &PgSnapshot{
		ID:               eventID,
		AggregateID:      aggregate.GetID(),
		AggregateVersion: aggregate.GetVersion(),
		AggregateType:    aggregate.GetType(),
		Body:             body,
		CreatedAt:        time.Now().UTC(),
	}

	_, err = r.db.NamedExecContext(ctx,
		`INSERT INTO snapshots (id, aggregate_id, aggregate_version, body, created_at)
	     VALUES (:id, :aggregate_id, :aggregate_version, :body, :created_at)`, snap)

	return err
}

func (r *PgEsRepository) GetAggregateEvents(ctx context.Context, aggregateID string, snapVersion int) ([]eventstore.Event, error) {
	var query bytes.Buffer
	query.WriteString("SELECT * FROM events e WHERE e.aggregate_id = $1")
	args := []interface{}{aggregateID}
	if snapVersion > -1 {
		query.WriteString(" AND e.aggregate_version > $2")
		args = append(args, snapVersion)
	}
	query.WriteString(" ORDER BY aggregate_version ASC")

	events, err := r.queryEvents(ctx, query.String(), "", args...)
	if err != nil {
		return nil, fmt.Errorf("Unable to get events for Aggregate '%s': %w", aggregateID, err)
	}
	if len(events) == 0 {
		return nil, fmt.Errorf("Aggregate '%s' was not found: %w", aggregateID, err)
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

func (r *PgEsRepository) Forget(ctx context.Context, request eventstore.ForgetRequest, forget func(interface{}) interface{}) error {
	// FIXME use a transaction.
	// FIXME in the end should also add a new event to the event store.

	// Forget events
	events, err := r.queryEvents(ctx, "SELECT * FROM events WHERE aggregate_id = $1 AND kind = $2", "", request.AggregateID, request.EventKind)
	if err != nil {
		return fmt.Errorf("Unable to get events for Aggregate '%s' and event kind '%s': %w", request.AggregateID, request.EventKind, err)
	}

	for _, evt := range events {
		e, err := r.factory.New(evt.Kind)
		if err != nil {
			return err
		}
		e = forget(e)
		body, err := r.codec.Encode(e)
		if err != nil {
			return err
		}
		_, err = r.db.ExecContext(ctx, "UPDATE events SET body = $1 WHERE ID = $2", body, evt.ID)
		if err != nil {
			return fmt.Errorf("Unable to forget event ID %s: %w", evt.ID, err)
		}
	}

	// forget snapshots
	snaps := []PgSnapshot{}
	if err := r.db.SelectContext(ctx, &snaps, "SELECT * FROM snapshots WHERE aggregate_id = $1", request.AggregateID); err != nil {
		if err == sql.ErrNoRows {
			return nil
		}
		return fmt.Errorf("Unable to get snapshot for aggregate '%s': %w", request.AggregateID, err)
	}

	for _, snap := range snaps {
		e, err := r.factory.New(snap.AggregateType)
		if err != nil {
			return err
		}
		e = forget(e)
		body, err := r.codec.Encode(e)
		if err != nil {
			return err
		}
		_, err = r.db.ExecContext(ctx, "UPDATE snapshot SET body = $1 WHERE ID = $2", body, snap.ID)
		if err != nil {
			return fmt.Errorf("Unable to forget snapshot ID %s: %w", snap.ID, err)
		}
	}

	return nil
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

	rows, err := r.queryEvents(ctx, query.String(), afterEventID, args...)
	if err != nil {
		return nil, fmt.Errorf("Unable to get events after '%s' for filter %+v: %w", afterEventID, filter, err)
	}
	return rows, nil
}

func writeFilter(filter Filter, query *bytes.Buffer, args []interface{}) []interface{} {
	if len(filter.AggregateTypes) > 0 {
		query.WriteString(" AND (")
		for k, v := range filter.AggregateTypes {
			if k > 0 {
				query.WriteString(" OR ")
			}
			args = append(args, v)
			query.WriteString(fmt.Sprintf("aggregate_type = $%d", len(args)))
		}
		query.WriteString(")")
	}
	if len(filter.Labels) > 0 {
		for idx, v := range filter.Labels {
			k := escape(v.Key)
			query.WriteString(" AND (")
			if idx > 0 {
				query.WriteString(" OR ")
			}
			x := escape(v.Value)
			query.WriteString(fmt.Sprintf(`labels  @> '{"%s": "%s"}'`, k, x))
			query.WriteString(")")
		}
	}
	return args
}

func escape(s string) string {
	return strings.ReplaceAll(s, "'", "''")
}

func (r *PgEsRepository) queryEvents(ctx context.Context, query string, afterEventID string, args ...interface{}) ([]eventstore.Event, error) {
	rows, err := r.db.QueryxContext(ctx, query, args...)
	if err != nil {
		if err == sql.ErrNoRows {
			return []eventstore.Event{}, nil
		}
		return nil, err
	}
	events := []eventstore.Event{}
	for rows.Next() {
		pg := PgEvent{}
		err := rows.StructScan(&pg)
		if err != nil {
			return nil, fmt.Errorf("Unable to scan to struct: %w", err)
		}
		labels := map[string]interface{}{}
		err = json.Unmarshal(pg.Labels, &labels)
		if err != nil {
			return nil, fmt.Errorf("Unable to unmarshal labels to map: %w", err)
		}

		decode := func() (eventstore.Eventer, error) {
			e, err := r.factory.New(pg.Kind)
			if err != nil {
				return nil, err
			}
			err = r.codec.Decode(pg.Body, &e)
			if err != nil {
				return nil, fmt.Errorf("Unable to decode event %s: %w", pg.Kind, err)
			}
			return e.(eventstore.Eventer), nil
		}

		events = append(events, eventstore.Event{
			ID:               pg.ID,
			AggregateID:      pg.AggregateID,
			AggregateVersion: pg.AggregateVersion,
			AggregateType:    pg.AggregateType,
			Kind:             pg.Kind,
			Body:             pg.Body,
			Labels:           labels,
			CreatedAt:        pg.CreatedAt,
			Decode:           decode,
		})
	}
	return events, nil
}
