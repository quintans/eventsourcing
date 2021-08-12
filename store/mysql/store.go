package mysql

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	"github.com/quintans/faults"

	"github.com/quintans/eventsourcing"
	"github.com/quintans/eventsourcing/common"
	"github.com/quintans/eventsourcing/encoding"
	"github.com/quintans/eventsourcing/eventid"
	"github.com/quintans/eventsourcing/store"
)

const (
	driverName      = "mysql"
	uniqueViolation = 1062
)

// Event is the event data stored in the database
type Event struct {
	ID               string                      `db:"id"`
	AggregateID      string                      `db:"aggregate_id"`
	AggregateIDHash  int32                       `db:"aggregate_id_hash"`
	AggregateVersion uint32                      `db:"aggregate_version"`
	AggregateType    eventsourcing.AggregateType `db:"aggregate_type"`
	Kind             eventsourcing.EventKind     `db:"kind"`
	Body             []byte                      `db:"body"`
	IdempotencyKey   NilString                   `db:"idempotency_key"`
	Metadata         []byte                      `db:"metadata"`
	CreatedAt        time.Time                   `db:"created_at"`
}

// NilString converts nil to empty string
type NilString string

// Scan implements the Scanner interface.
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

type Snapshot struct {
	ID               string                      `db:"id,omitempty"`
	AggregateID      string                      `db:"aggregate_id,omitempty"`
	AggregateVersion uint32                      `db:"aggregate_version,omitempty"`
	AggregateType    eventsourcing.AggregateType `db:"aggregate_type,omitempty"`
	Body             []byte                      `db:"body,omitempty"`
	CreatedAt        time.Time                   `db:"created_at,omitempty"`
}

var _ eventsourcing.EsRepository = (*EsRepository)(nil)

type StoreOption func(*EsRepository)

type Projector func(context.Context, *sql.Tx, eventsourcing.Event) error

func ProjectorOption(fn Projector) StoreOption {
	return func(r *EsRepository) {
		r.projector = fn
	}
}

type EsRepository struct {
	db        *sqlx.DB
	projector Projector
}

func NewStore(connString string, options ...StoreOption) (*EsRepository, error) {
	db, err := sql.Open(driverName, connString)
	if err != nil {
		return nil, faults.Wrap(err)
	}

	dbx := sqlx.NewDb(db, driverName)
	r := &EsRepository{
		db: dbx,
	}

	for _, o := range options {
		o(r)
	}

	return r, nil
}

func (r *EsRepository) SaveEvent(ctx context.Context, eRec eventsourcing.EventRecord) (eventid.EventID, uint32, error) {
	var idempotencyKey *string
	if eRec.IdempotencyKey != eventsourcing.EmptyIdempotencyKey {
		idempotencyKey = &eRec.IdempotencyKey
	}

	metadata := encoding.JsonOfMap(eRec.Metadata)
	version := eRec.Version
	var id eventid.EventID
	err := r.withTx(ctx, func(c context.Context, tx *sql.Tx) error {
		entropy := eventid.EntropyFactory(eRec.CreatedAt)
		for _, e := range eRec.Details {
			var err error
			id, err = eventid.New(eRec.CreatedAt, entropy)
			if err != nil {
				return faults.Wrap(err)
			}
			version++
			hash := common.Hash(eRec.AggregateID)
			_, err = tx.ExecContext(ctx,
				`INSERT INTO events (id, aggregate_id, aggregate_version, aggregate_type, kind, body, idempotency_key, metadata, created_at, aggregate_id_hash)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
				id.String(), eRec.AggregateID, version, eRec.AggregateType, e.Kind, e.Body, idempotencyKey, metadata, eRec.CreatedAt, int32ring(hash))
			// for a batch of events, the idempotency key is only applied on the first record
			idempotencyKey = nil

			if err != nil {
				if isDup(err) {
					return eventsourcing.ErrConcurrentModification
				}
				return faults.Errorf("Unable to insert event: %w", err)
			}

			if r.projector != nil {
				err := r.projector(c, tx, eventsourcing.Event{
					ID:               id,
					AggregateID:      eRec.AggregateID,
					AggregateIDHash:  hash,
					AggregateVersion: version,
					AggregateType:    eRec.AggregateType,
					Kind:             e.Kind,
					Body:             e.Body,
					Metadata:         metadata,
					CreatedAt:        eRec.CreatedAt,
				})
				if err != nil {
					return err
				}
			}
		}

		return nil
	})
	if err != nil {
		return eventid.Zero, 0, err
	}

	return id, version, nil
}

func int32ring(x uint32) int32 {
	h := int32(x)
	// we want a positive value so that partitioning (mod) results in a positive value.
	// if h overflows, becoming negative, setting sign bit to zero will make the overflow start from zero
	if h < 0 {
		// setting sign bit to zero
		h &= 0x7fffffff
	}
	return h
}

func isDup(err error) bool {
	me, ok := err.(*mysql.MySQLError)
	return ok && me.Number == uniqueViolation
}

func (r *EsRepository) GetSnapshot(ctx context.Context, aggregateID string) (eventsourcing.Snapshot, error) {
	snap := Snapshot{}
	if err := r.db.GetContext(ctx, &snap, "SELECT * FROM snapshots WHERE aggregate_id = ? ORDER BY id DESC LIMIT 1", aggregateID); err != nil {
		if err == sql.ErrNoRows {
			return eventsourcing.Snapshot{}, nil
		}
		return eventsourcing.Snapshot{}, faults.Errorf("Unable to get snapshot for aggregate '%s': %w", aggregateID, err)
	}
	eventID, err := eventid.Parse(snap.ID)
	if err != nil {
		return eventsourcing.Snapshot{}, faults.Wrap(err)
	}
	return eventsourcing.Snapshot{
		ID:               eventID,
		AggregateID:      aggregateID,
		AggregateVersion: snap.AggregateVersion,
		AggregateType:    snap.AggregateType,
		Body:             snap.Body,
		CreatedAt:        snap.CreatedAt,
	}, nil
}

func (r *EsRepository) SaveSnapshot(ctx context.Context, snapshot eventsourcing.Snapshot) error {
	s := Snapshot{
		ID:               snapshot.ID.String(),
		AggregateID:      snapshot.AggregateID,
		AggregateVersion: snapshot.AggregateVersion,
		AggregateType:    snapshot.AggregateType,
		Body:             snapshot.Body,
		CreatedAt:        snapshot.CreatedAt,
	}
	_, err := r.db.NamedExecContext(ctx,
		`INSERT INTO snapshots (id, aggregate_id, aggregate_version, aggregate_type, body, created_at)
	     VALUES (:id, :aggregate_id, :aggregate_version, :aggregate_type, :body, :created_at)`, s)

	return faults.Wrap(err)
}

func (r *EsRepository) GetAggregateEvents(ctx context.Context, aggregateID string, snapVersion int) ([]eventsourcing.Event, error) {
	var query bytes.Buffer
	query.WriteString("SELECT * FROM events e WHERE e.aggregate_id = ?")
	args := []interface{}{aggregateID}
	if snapVersion > -1 {
		query.WriteString(" AND e.aggregate_version > ?")
		args = append(args, snapVersion)
	}
	query.WriteString(" ORDER BY aggregate_version ASC")

	events, err := r.queryEvents(ctx, query.String(), args...)
	if err != nil {
		return nil, faults.Errorf("Unable to get events for Aggregate '%s': %w", aggregateID, err)
	}

	return events, nil
}

func (r *EsRepository) withTx(ctx context.Context, fn func(context.Context, *sql.Tx) error) (err error) {
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
	err = fn(ctx, tx)
	if err != nil {
		return err
	}
	return tx.Commit()
}

func (r *EsRepository) HasIdempotencyKey(ctx context.Context, idempotencyKey string) (bool, error) {
	var exists bool
	err := r.db.GetContext(ctx, &exists, `SELECT EXISTS(SELECT 1 FROM events WHERE idempotency_key=?) AS "EXISTS"`, idempotencyKey)
	if err != nil {
		return false, faults.Errorf("Unable to verify the existence of the idempotency key: %w", err)
	}
	return exists, nil
}

func (r *EsRepository) Forget(ctx context.Context, req eventsourcing.ForgetRequest, forget func(kind string, body []byte, snapshot bool) ([]byte, error)) error {
	// When Forget() is called, the aggregate is no longer used, therefore if it fails, it can be called again.

	// Forget events
	events, err := r.queryEvents(ctx, "SELECT * FROM events WHERE aggregate_id = ? AND kind = ?", req.AggregateID, req.EventKind)
	if err != nil {
		return faults.Errorf("Unable to get events for Aggregate '%s' and event kind '%s': %w", req.AggregateID, req.EventKind, err)
	}

	for _, evt := range events {
		body, err := forget(evt.Kind.String(), evt.Body, false)
		if err != nil {
			return err
		}
		_, err = r.db.ExecContext(ctx, "UPDATE events SET body = ? WHERE ID = ?", body, evt.ID.String())
		if err != nil {
			return faults.Errorf("Unable to forget event ID %s: %w", evt.ID, err)
		}
	}

	// forget snapshots
	snaps := []Snapshot{}
	if err := r.db.SelectContext(ctx, &snaps, "SELECT * FROM snapshots WHERE aggregate_id = ?", req.AggregateID); err != nil {
		if err == sql.ErrNoRows {
			return nil
		}
		return faults.Errorf("Unable to get snapshot for aggregate '%s': %w", req.AggregateID, err)
	}

	for _, snap := range snaps {
		body, err := forget(snap.AggregateType.String(), snap.Body, true)
		if err != nil {
			return err
		}
		_, err = r.db.ExecContext(ctx, "UPDATE snapshots SET body = ? WHERE ID = ?", body, snap.ID)
		if err != nil {
			return faults.Errorf("Unable to forget snapshot ID %s: %w", snap.ID, err)
		}
	}

	return nil
}

func (r *EsRepository) GetLastEventID(ctx context.Context, trailingLag time.Duration, filter store.Filter) (eventid.EventID, error) {
	var query bytes.Buffer
	query.WriteString("SELECT * FROM events ")
	args := []interface{}{}
	if trailingLag != time.Duration(0) {
		safetyMargin := time.Now().UTC().Add(-trailingLag)
		args = append(args, safetyMargin)
		query.WriteString("created_at <= ? ")
	}
	args = buildFilter(filter, &query, args)
	query.WriteString(" ORDER BY id DESC LIMIT 1")
	var eventID string
	if err := r.db.GetContext(ctx, &eventID, query.String(), args...); err != nil {
		if err != sql.ErrNoRows {
			return eventid.Zero, faults.Errorf("unable to get the last event ID: %w", err)
		}
	}
	eID, err := eventid.Parse(eventID)
	if err != nil {
		return eventid.Zero, faults.Errorf("unable to parse event ID '%s': %w", eventID, err)
	}
	return eID, nil
}

func (r *EsRepository) GetEvents(ctx context.Context, afterEventID eventid.EventID, batchSize int, trailingLag time.Duration, filter store.Filter) ([]eventsourcing.Event, error) {
	var records []eventsourcing.Event
	for len(records) < batchSize {
		var query bytes.Buffer
		query.WriteString("SELECT * FROM events WHERE id > ? ")
		args := []interface{}{afterEventID.String()}
		if trailingLag != time.Duration(0) {
			safetyMargin := time.Now().UTC().Add(-trailingLag)
			args = append(args, safetyMargin)
			query.WriteString("AND created_at <= ? ")
		}
		args = buildFilter(filter, &query, args)
		query.WriteString(" ORDER BY id ASC")
		if batchSize > 0 {
			query.WriteString(" LIMIT ")
			query.WriteString(strconv.Itoa(batchSize))
		}

		rows, err := r.queryEvents(ctx, query.String(), args...)
		if err != nil {
			return nil, faults.Errorf("Unable to get events after '%s' for filter %+v: %w", afterEventID, filter, err)
		}
		if len(rows) == 0 {
			return records, nil
		}

		afterEventID = rows[len(rows)-1].ID
		records = rows
	}
	return records, nil
}

func buildFilter(filter store.Filter, query *bytes.Buffer, args []interface{}) []interface{} {
	if len(filter.AggregateTypes) > 0 {
		query.WriteString(" AND (")
		for k, v := range filter.AggregateTypes {
			if k > 0 {
				query.WriteString(" OR ")
			}
			args = append(args, v)
			query.WriteString("aggregate_type = ?")
		}
		query.WriteString(")")
	}

	if filter.Partitions > 1 {
		if filter.PartitionLow == filter.PartitionHi {
			args = append(args, filter.Partitions, filter.PartitionLow-1)
			query.WriteString(" AND MOD(aggregate_id_hash, ?) = ?")
		} else {
			args = append(args, filter.Partitions, filter.PartitionLow-1, filter.PartitionHi-1)
			query.WriteString(" AND MOD(aggregate_id_hash, ?) BETWEEN ? AND ?")
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
				query.WriteString(fmt.Sprintf(`JSON_EXTRACT(metadata, '$.%s') = '%s'`, k, v))
				query.WriteString(")")
			}
		}
	}
	return args
}

func escape(s string) string {
	return strings.ReplaceAll(s, "'", "''")
}

func (r *EsRepository) queryEvents(ctx context.Context, query string, args ...interface{}) ([]eventsourcing.Event, error) {
	rows, err := r.db.QueryxContext(ctx, query, args...)
	if err != nil {
		if err == sql.ErrNoRows {
			return []eventsourcing.Event{}, nil
		}
		return nil, faults.Errorf("unable to query events with %s: %w", query, err)
	}
	events := []eventsourcing.Event{}
	for rows.Next() {
		event := Event{}
		err := rows.StructScan(&event)
		if err != nil {
			return nil, faults.Errorf("unable to scan to struct: %w", err)
		}

		id, err := eventid.Parse(event.ID)
		if err != nil {
			return nil, faults.Errorf("unable to parse event ID '%s': %w", event.ID, err)
		}
		events = append(events, eventsourcing.Event{
			ID:               id,
			AggregateID:      event.AggregateID,
			AggregateIDHash:  uint32(event.AggregateIDHash),
			AggregateVersion: event.AggregateVersion,
			AggregateType:    event.AggregateType,
			Kind:             event.Kind,
			Body:             event.Body,
			Metadata:         encoding.JsonOfBytes(event.Metadata),
			CreatedAt:        event.CreatedAt,
		})
	}
	return events, nil
}

func (r *EsRepository) MigrateInPlaceCopyReplace(
	ctx context.Context,
	revision int,
	snapshotThreshold uint32,
	aggregateFactory func() (eventsourcing.Aggregater, error), // called only if snapshot threshold is reached
	rehydrateFunc func(eventsourcing.Aggregater, eventsourcing.Event) error, // called only if snapshot threshold is reached
	encoder eventsourcing.Encoder,
	handler eventsourcing.MigrationHandler,
	aggregateType eventsourcing.AggregateType,
	eventTypeCriteria ...eventsourcing.EventKind,
) error {
	return faults.New("not implemented")
}
