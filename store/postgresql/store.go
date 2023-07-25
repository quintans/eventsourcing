package postgresql

import (
	"bytes"
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/lib/pq"
	"github.com/quintans/faults"

	"github.com/quintans/eventsourcing"
	"github.com/quintans/eventsourcing/encoding"
	"github.com/quintans/eventsourcing/eventid"
	"github.com/quintans/eventsourcing/sink"
	"github.com/quintans/eventsourcing/store"
	"github.com/quintans/eventsourcing/util"
)

const (
	driverName        = "postgres"
	pgUniqueViolation = "23505"
)

// Event is the event data stored in the database
type Event struct {
	ID               eventid.EventID    `db:"id"`
	AggregateID      string             `db:"aggregate_id"`
	AggregateIDHash  int32              `db:"aggregate_id_hash"`
	AggregateVersion uint32             `db:"aggregate_version"`
	AggregateKind    eventsourcing.Kind `db:"aggregate_kind"`
	Kind             eventsourcing.Kind `db:"kind"`
	Body             []byte             `db:"body"`
	IdempotencyKey   NilString          `db:"idempotency_key"`
	Metadata         *encoding.JSON     `db:"metadata"`
	CreatedAt        time.Time          `db:"created_at"`
	Migration        int                `db:"migration"`
	Migrated         bool               `db:"migrated"`
	Partition        int32              `db:"sink_part"`
	Sequence         uint64             `db:"sink_seq"`
}

// NilString converts nil to empty string
type NilString string

func (ns *NilString) Scan(value interface{}) error {
	if value == nil {
		*ns = ""
		return nil
	}

	switch s := value.(type) {
	case string:
		*ns = NilString(s)
	case []byte:
		*ns = NilString(s)
	}
	return nil
}

func (ns NilString) Value() (driver.Value, error) {
	if ns == "" {
		return nil, nil
	}
	return string(ns), nil
}

type Snapshot struct {
	ID               eventid.EventID    `db:"id,omitempty"`
	AggregateID      string             `db:"aggregate_id,omitempty"`
	AggregateVersion uint32             `db:"aggregate_version,omitempty"`
	AggregateKind    eventsourcing.Kind `db:"aggregate_kind,omitempty"`
	Body             []byte             `db:"body,omitempty"`
	CreatedAt        time.Time          `db:"created_at,omitempty"`
}

var _ eventsourcing.EsRepository = (*EsRepository)(nil)

type Option func(*EsRepository)

func WithPublisher(publisher store.Publisher) Option {
	return func(r *EsRepository) {
		r.publisher = publisher
	}
}

type EsRepository struct {
	db        *sqlx.DB
	publisher store.Publisher
}

func NewStore(connString string) (*EsRepository, error) {
	db, err := sql.Open(driverName, connString)
	if err != nil {
		return nil, faults.Wrap(err)
	}

	dbx := sqlx.NewDb(db, driverName)
	r := &EsRepository{
		db: dbx,
	}

	return r, nil
}

func (r *EsRepository) SaveEvent(ctx context.Context, eRec *eventsourcing.EventRecord) (eventid.EventID, uint32, error) {
	idempotencyKey := eRec.IdempotencyKey

	version := eRec.Version
	var id eventid.EventID
	err := r.withTx(ctx, func(c context.Context, tx *sql.Tx) error {
		entropy := eventid.NewEntropy()
		for _, e := range eRec.Details {
			version++
			hash := util.Hash(eRec.AggregateID)
			var err error
			id, err = entropy.NewID(eRec.CreatedAt)
			if err != nil {
				return faults.Wrap(err)
			}
			err = r.saveEvent(c, tx, &Event{
				ID:               id,
				AggregateID:      eRec.AggregateID,
				AggregateIDHash:  util.Int32ring(hash),
				AggregateVersion: version,
				AggregateKind:    eRec.AggregateKind,
				Kind:             e.Kind,
				Body:             e.Body,
				IdempotencyKey:   NilString(idempotencyKey),
				Metadata:         encoding.JSONOfMap(eRec.Metadata),
				CreatedAt:        eRec.CreatedAt,
			})
			if err != nil {
				return faults.Wrap(err)
			}
			// for a batch of events, the idempotency key is only applied on the first record
			idempotencyKey = ""
		}

		return nil
	})
	if err != nil {
		return eventid.Zero, 0, err
	}

	return id, version, nil
}

func (r *EsRepository) saveEvent(ctx context.Context, tx *sql.Tx, event *Event) error {
	_, err := tx.ExecContext(ctx,
		`INSERT INTO events (id, aggregate_id, aggregate_version, aggregate_kind, kind, body, idempotency_key, metadata, created_at, aggregate_id_hash, migrated)
	VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)`,
		event.ID.String(), event.AggregateID, event.AggregateVersion, event.AggregateKind, event.Kind, event.Body, event.IdempotencyKey, event.Metadata, event.CreatedAt, event.AggregateIDHash, event.Migrated)
	if err != nil {
		if isDup(err) {
			return faults.Wrap(eventsourcing.ErrConcurrentModification)
		}
		return faults.Errorf("unable to insert event: %w", err)
	}

	return r.publish(ctx, event)
}

func (r *EsRepository) publish(ctx context.Context, event *Event) error {
	if r.publisher == nil {
		return nil
	}

	e := toEventsourcingEvent(event)
	return r.publisher.Publish(ctx, e)
}

func isDup(err error) bool {
	pgerr, ok := err.(*pq.Error)
	return ok && pgerr.Code == pgUniqueViolation
}

func (r *EsRepository) GetSnapshot(ctx context.Context, aggregateID string) (eventsourcing.Snapshot, error) {
	snap := Snapshot{}
	if err := r.db.GetContext(ctx, &snap, "SELECT * FROM snapshots WHERE aggregate_id = $1 ORDER BY id DESC LIMIT 1", aggregateID); err != nil {
		if err == sql.ErrNoRows {
			return eventsourcing.Snapshot{}, nil
		}
		return eventsourcing.Snapshot{}, faults.Errorf("Unable to get snapshot for aggregate '%s': %w", aggregateID, err)
	}

	return eventsourcing.Snapshot{
		ID:               snap.ID,
		AggregateID:      aggregateID,
		AggregateVersion: snap.AggregateVersion,
		AggregateKind:    snap.AggregateKind,
		Body:             snap.Body,
		CreatedAt:        snap.CreatedAt,
	}, nil
}

func (r *EsRepository) SaveSnapshot(ctx context.Context, snapshot *eventsourcing.Snapshot) error {
	return saveSnapshot(ctx, r.db, &Snapshot{
		ID:               snapshot.ID,
		AggregateID:      snapshot.AggregateID,
		AggregateVersion: snapshot.AggregateVersion,
		AggregateKind:    snapshot.AggregateKind,
		Body:             snapshot.Body,
		CreatedAt:        snapshot.CreatedAt.UTC(),
	})
}

type sqlExecuter interface {
	ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error)
}

func saveSnapshot(ctx context.Context, x sqlExecuter, s *Snapshot) error {
	// TODO instead of adding we could replace UPDATE/INSERT
	_, err := x.ExecContext(ctx,
		`INSERT INTO snapshots (id, aggregate_id, aggregate_version, aggregate_kind, body, created_at)
	     VALUES ($1, $2, $3, $4, $5, $6)`,
		s.ID, s.AggregateID, s.AggregateVersion, s.AggregateKind, s.Body, s.CreatedAt)

	return faults.Wrap(err)
}

func (r *EsRepository) GetAggregateEvents(ctx context.Context, aggregateID string, snapVersion int) ([]*eventsourcing.Event, error) {
	var query bytes.Buffer
	query.WriteString("SELECT * FROM events e WHERE e.aggregate_id = $1 AND migration = 0")
	args := []interface{}{aggregateID}
	if snapVersion > -1 {
		query.WriteString(" AND e.aggregate_version > $2")
		args = append(args, snapVersion)
	}
	query.WriteString(" ORDER BY aggregate_version ASC")

	events, err := r.queryEvents(ctx, query.String(), args...)
	if err != nil {
		return nil, faults.Errorf("Unable to get events for Aggregate '%s': %w", aggregateID, err)
	}

	return events, nil
}

type txKey struct{}

func TxFromContext(ctx context.Context) *sql.Tx {
	tx, _ := ctx.Value(txKey{}).(*sql.Tx) // with _ it will not panic if nil
	return tx
}

func (r *EsRepository) withTx(ctx context.Context, fn func(context.Context, *sql.Tx) error) error {
	tx := TxFromContext(ctx)
	if tx != nil {
		return fn(ctx, tx)
	}

	return r.wrapWithTx(ctx, fn)
}

func (r *EsRepository) wrapWithTx(ctx context.Context, fn func(context.Context, *sql.Tx) error) error {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return faults.Wrap(err)
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

	ctx = context.WithValue(ctx, txKey{}, tx)
	err = fn(ctx, tx)
	if err != nil {
		return err
	}
	return tx.Commit()
}

func (r *EsRepository) HasIdempotencyKey(ctx context.Context, idempotencyKey string) (bool, error) {
	var exists bool
	err := r.db.GetContext(ctx, &exists, `SELECT EXISTS(SELECT 1 FROM events WHERE idempotency_key=$1 AND migration = 0) AS "EXISTS"`, idempotencyKey)
	if err != nil {
		return false, faults.Errorf("Unable to verify the existence of the idempotency key: %w", err)
	}
	return exists, nil
}

func (r *EsRepository) Forget(ctx context.Context, request eventsourcing.ForgetRequest, forget func(kind eventsourcing.Kind, body []byte, snapshot bool) ([]byte, error)) error {
	// When Forget() is called, the aggregate is no longer used, therefore if it fails, it can be called again.

	// Forget events
	events, err := r.queryEvents(ctx, "SELECT * FROM events WHERE aggregate_id = $1 AND kind = $2", request.AggregateID, request.EventKind)
	if err != nil {
		return faults.Errorf("Unable to get events for Aggregate '%s' and event kind '%s': %w", request.AggregateID, request.EventKind, err)
	}

	for _, evt := range events {
		body, err := forget(evt.Kind, evt.Body, false)
		if err != nil {
			return err
		}
		_, err = r.db.ExecContext(ctx, "UPDATE events SET body = $1 WHERE ID = $2", body, evt.ID.String())
		if err != nil {
			return faults.Errorf("Unable to forget event ID %s: %w", evt.ID, err)
		}
	}

	// forget snapshots
	snaps := []Snapshot{}
	if err := r.db.SelectContext(ctx, &snaps, "SELECT * FROM snapshots WHERE aggregate_id = $1", request.AggregateID); err != nil {
		if err == sql.ErrNoRows {
			return nil
		}
		return faults.Errorf("Unable to get snapshot for aggregate '%s': %w", request.AggregateID, err)
	}

	for _, snap := range snaps {
		body, err := forget(snap.AggregateKind, snap.Body, true)
		if err != nil {
			return err
		}
		_, err = r.db.ExecContext(ctx, "UPDATE snapshots SET body = $1 WHERE ID = $2", body, snap.ID)
		if err != nil {
			return faults.Errorf("Unable to forget snapshot ID %s: %w", snap.ID, err)
		}
	}

	return nil
}

func (r *EsRepository) GetMaxSeq(ctx context.Context, filter store.Filter) (uint64, error) {
	var query bytes.Buffer
	query.WriteString("SELECT COALESCE(MAX(sink_seq), 0) FROM events WHERE migration = 0")
	args := []interface{}{}
	args = buildFilter(filter, &query, args)
	var seq uint64
	if err := r.db.GetContext(ctx, &seq, query.String(), args...); err != nil {
		if err != sql.ErrNoRows {
			return 0, faults.Errorf("unable to get the last event ID: %w", err)
		}
	}

	return seq, nil
}

func (r *EsRepository) GetEvents(ctx context.Context, afterSeq uint64, batchSize int, filter store.Filter) ([]*eventsourcing.Event, error) {
	var query bytes.Buffer
	query.WriteString("SELECT * FROM events WHERE sink_seq > $1 AND migration = 0")
	args := []interface{}{afterSeq}
	args = buildFilter(filter, &query, args)
	query.WriteString(" ORDER BY sink_seq ASC")
	if batchSize > 0 {
		query.WriteString(" LIMIT ")
		query.WriteString(strconv.Itoa(batchSize))
	}

	rows, err := r.queryEvents(ctx, query.String(), args...)
	if err != nil {
		return nil, faults.Errorf("unable to get events after '%d' for filter %+v: %w", afterSeq, filter, err)
	}
	if len(rows) == 0 {
		return nil, nil
	}

	return rows, nil
}

func (r *EsRepository) GetPendingEvents(ctx context.Context, batchSize int, filter store.Filter) ([]*eventsourcing.Event, error) {
	var query bytes.Buffer
	query.WriteString("SELECT * FROM events WHERE sink_seq = 0 AND migration = 0")
	args := []interface{}{}
	args = buildFilter(filter, &query, args)
	query.WriteString(" ORDER BY id ASC")
	if batchSize > 0 {
		query.WriteString(" LIMIT ")
		query.WriteString(strconv.Itoa(batchSize))
	}

	rows, err := r.queryEvents(ctx, query.String(), args...)
	if err != nil {
		return nil, faults.Errorf("unable to get pending events after for filter %+v: %w", filter, err)
	}
	if len(rows) == 0 {
		return nil, nil
	}

	return rows, nil
}

func (r *EsRepository) SetSinkData(ctx context.Context, eID eventid.EventID, data sink.Data) error {
	_, err := r.db.ExecContext(ctx, "UPDATE events SET sink_part = $1, sink_seq = $2 WHERE ID = $3", data.Partition(), data.Sequence(), eID.String())
	return faults.Wrapf(err, "setting publish partition %d and sequence %d for event id '%s'", data.Partition(), data.Sequence(), eID)
}

func buildFilter(filter store.Filter, query *bytes.Buffer, args []interface{}) []interface{} {
	if len(filter.AggregateKinds) > 0 {
		query.WriteString(" AND (")
		for k, v := range filter.AggregateKinds {
			if k > 0 {
				query.WriteString(" OR ")
			}
			args = append(args, v)
			query.WriteString(fmt.Sprintf("aggregate_kind = $%d", len(args)))
		}
		query.WriteString(")")
	}

	if filter.Partitions > 1 {
		size := len(args)
		if filter.PartitionLow == filter.PartitionHi {
			args = append(args, filter.Partitions, filter.PartitionLow-1)
			query.WriteString(fmt.Sprintf(" AND MOD(aggregate_id_hash, $%d) = $%d", size+1, size+2))
		} else {
			args = append(args, filter.Partitions, filter.PartitionLow-1, filter.PartitionHi-1)
			query.WriteString(fmt.Sprintf(" AND MOD(aggregate_id_hash, $%d) BETWEEN $%d AND $%d", size+1, size+2, size+4))
		}
	}

	if len(filter.Metadata) > 0 {
		for k, values := range filter.Metadata {
			k = escape(k)
			query.WriteString(" AND (")
			for idx, v := range values {
				if idx > 0 {
					query.WriteString(" OR ")
				}
				v = escape(v)
				query.WriteString(fmt.Sprintf(`metadata  @> '{"%s": "%s"}'`, k, v))
				query.WriteString(")")
			}
		}
	}
	return args
}

func escape(s string) string {
	return strings.ReplaceAll(s, "'", "''")
}

func (r *EsRepository) queryEvents(ctx context.Context, query string, args ...interface{}) ([]*eventsourcing.Event, error) {
	rows, err := r.db.QueryxContext(ctx, query, args...)
	if err != nil {
		if err == sql.ErrNoRows {
			return []*eventsourcing.Event{}, nil
		}
		return nil, faults.Errorf("Unable to query events: %w", err)
	}
	events := []*eventsourcing.Event{}
	for rows.Next() {
		pgEvent := &Event{}
		err := rows.StructScan(pgEvent)
		if err != nil {
			return nil, faults.Errorf("Unable to scan to struct: %w", err)
		}

		events = append(events, toEventsourcingEvent(pgEvent))
	}
	return events, nil
}

func toEventsourcingEvent(e *Event) *eventsourcing.Event {
	return &eventsourcing.Event{
		ID:               e.ID,
		AggregateID:      e.AggregateID,
		AggregateIDHash:  uint32(e.AggregateIDHash),
		AggregateVersion: e.AggregateVersion,
		AggregateKind:    e.AggregateKind,
		Kind:             e.Kind,
		Body:             e.Body,
		Metadata:         e.Metadata,
		CreatedAt:        e.CreatedAt,
		Migrated:         e.Migrated,
		Sequence:         e.Sequence,
	}
}
