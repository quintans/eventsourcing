package postgresql

import (
	"bytes"
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/lib/pq"
	"github.com/oklog/ulid/v2"
	"github.com/quintans/faults"

	"github.com/quintans/eventsourcing"
	"github.com/quintans/eventsourcing/common"
	"github.com/quintans/eventsourcing/encoding"
	"github.com/quintans/eventsourcing/eventid"
	"github.com/quintans/eventsourcing/store"
)

const (
	driverName        = "postgres"
	pgUniqueViolation = "23505"
)

// Event is the event data stored in the database
type Event struct {
	ID               eventid.EventID             `db:"id"`
	AggregateID      string                      `db:"aggregate_id"`
	AggregateIDHash  int32                       `db:"aggregate_id_hash"`
	AggregateVersion uint32                      `db:"aggregate_version"`
	AggregateType    eventsourcing.AggregateType `db:"aggregate_type"`
	Kind             eventsourcing.EventKind     `db:"kind"`
	Body             []byte                      `db:"body"`
	IdempotencyKey   NilString                   `db:"idempotency_key"`
	Metadata         *encoding.Json              `db:"metadata"`
	CreatedAt        time.Time                   `db:"created_at"`
	Migrated         int                         `db:"migrated"`
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
	ID               eventid.EventID             `db:"id,omitempty"`
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
	idempotencyKey := eRec.IdempotencyKey

	version := eRec.Version
	var id eventid.EventID
	err := r.withTx(ctx, func(c context.Context, tx *sql.Tx) error {
		entropy := eventid.EntropyFactory(eRec.CreatedAt)
		for _, e := range eRec.Details {
			version++
			hash := common.Hash(eRec.AggregateID)
			var err error
			id, err = r.saveEvent(c, tx, entropy, Event{
				AggregateID:      eRec.AggregateID,
				AggregateIDHash:  int32ring(hash),
				AggregateVersion: version,
				AggregateType:    eRec.AggregateType,
				Kind:             e.Kind,
				Body:             e.Body,
				IdempotencyKey:   NilString(idempotencyKey),
				Metadata:         encoding.JsonOfMap(eRec.Metadata),
				CreatedAt:        eRec.CreatedAt,
			})
			if err != nil {
				return err
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

func (r *EsRepository) saveEvent(ctx context.Context, tx *sql.Tx, entropy *ulid.MonotonicEntropy, event Event) (eventid.EventID, error) {
	id, err := eventid.New(event.CreatedAt, entropy)
	if err != nil {
		return eventid.Zero, faults.Wrap(err)
	}
	event.ID = id
	_, err = tx.ExecContext(ctx,
		`INSERT INTO events (id, aggregate_id, aggregate_version, aggregate_type, kind, body, idempotency_key, metadata, created_at, aggregate_id_hash)
	VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)`,
		event.ID.String(), event.AggregateID, event.AggregateVersion, event.AggregateType, event.Kind, event.Body, event.IdempotencyKey, event.Metadata, event.CreatedAt, event.AggregateIDHash)

	if err != nil {
		if isDup(err) {
			return eventid.Zero, eventsourcing.ErrConcurrentModification
		}
		return eventid.Zero, faults.Errorf("unable to insert event: %w", err)
	}

	if r.projector != nil {
		e := toEventsourcingEvent(event)
		err := r.projector(ctx, tx, e)
		if err != nil {
			return eventid.Zero, err
		}
	}

	return id, nil
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
		AggregateType:    snap.AggregateType,
		Body:             snap.Body,
		CreatedAt:        snap.CreatedAt,
	}, nil
}

func (r *EsRepository) SaveSnapshot(ctx context.Context, snapshot eventsourcing.Snapshot) error {
	return saveSnapshot(ctx, r.db, Snapshot{
		ID:               snapshot.ID,
		AggregateID:      snapshot.AggregateID,
		AggregateVersion: snapshot.AggregateVersion,
		AggregateType:    snapshot.AggregateType,
		Body:             snapshot.Body,
		CreatedAt:        snapshot.CreatedAt.UTC(),
	})
}

type sqlExecuter interface {
	ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error)
}

func saveSnapshot(ctx context.Context, x sqlExecuter, s Snapshot) error {
	_, err := x.ExecContext(ctx,
		`INSERT INTO snapshots (id, aggregate_id, aggregate_version, aggregate_type, body, created_at)
	     VALUES ($1, $2, $3, $4, $5, $6)`,
		s.ID, s.AggregateID, s.AggregateVersion, s.AggregateType, s.Body, s.CreatedAt)

	return faults.Wrap(err)
}

func (r *EsRepository) GetAggregateEvents(ctx context.Context, aggregateID string, snapVersion int) ([]eventsourcing.Event, error) {
	var query bytes.Buffer
	query.WriteString("SELECT * FROM events e WHERE e.aggregate_id = $1 AND migrated = 0")
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
	err := r.db.GetContext(ctx, &exists, `SELECT EXISTS(SELECT 1 FROM events WHERE idempotency_key=$1 AND migrated = 0) AS "EXISTS"`, idempotencyKey)
	if err != nil {
		return false, faults.Errorf("Unable to verify the existence of the idempotency key: %w", err)
	}
	return exists, nil
}

func (r *EsRepository) Forget(ctx context.Context, request eventsourcing.ForgetRequest, forget func(kind string, body []byte, snapshot bool) ([]byte, error)) error {
	// When Forget() is called, the aggregate is no longer used, therefore if it fails, it can be called again.

	// Forget events
	events, err := r.queryEvents(ctx, "SELECT * FROM events WHERE aggregate_id = $1 AND kind = $2", request.AggregateID, request.EventKind)
	if err != nil {
		return faults.Errorf("Unable to get events for Aggregate '%s' and event kind '%s': %w", request.AggregateID, request.EventKind, err)
	}

	for _, evt := range events {
		body, err := forget(evt.Kind.String(), evt.Body, false)
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
		body, err := forget(snap.AggregateType.String(), snap.Body, true)
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

func (r *EsRepository) GetLastEventID(ctx context.Context, trailingLag time.Duration, filter store.Filter) (eventid.EventID, error) {
	var query bytes.Buffer
	query.WriteString("SELECT * FROM events ")
	args := []interface{}{}
	if trailingLag != time.Duration(0) {
		safetyMargin := time.Now().UTC().Add(-trailingLag)
		args = append(args, safetyMargin)
		query.WriteString("created_at <= $1 ")
	}
	args = buildFilter(filter, &query, args)
	query.WriteString(" ORDER BY id DESC LIMIT 1")
	var eventID eventid.EventID
	if err := r.db.GetContext(ctx, &eventID, query.String(), args...); err != nil {
		if err != sql.ErrNoRows {
			return eventid.Zero, faults.Errorf("unable to get the last event ID: %w", err)
		}
	}

	return eventID, nil
}

func (r *EsRepository) GetEvents(ctx context.Context, afterEventID eventid.EventID, batchSize int, trailingLag time.Duration, filter store.Filter) ([]eventsourcing.Event, error) {
	var records []eventsourcing.Event
	for len(records) < batchSize {
		var query bytes.Buffer
		query.WriteString("SELECT * FROM events WHERE id > $1 ")
		args := []interface{}{afterEventID.String()}
		if trailingLag != time.Duration(0) {
			safetyMargin := time.Now().UTC().Add(-trailingLag)
			args = append(args, safetyMargin)
			query.WriteString("AND created_at <= $2 ")
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
			query.WriteString(fmt.Sprintf("aggregate_type = $%d", len(args)))
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

func (r *EsRepository) queryEvents(ctx context.Context, query string, args ...interface{}) ([]eventsourcing.Event, error) {
	rows, err := r.db.QueryxContext(ctx, query, args...)
	if err != nil {
		if err == sql.ErrNoRows {
			return []eventsourcing.Event{}, nil
		}
		return nil, faults.Errorf("Unable to query events: %w", err)
	}
	events := []eventsourcing.Event{}
	for rows.Next() {
		pgEvent := Event{}
		err := rows.StructScan(&pgEvent)
		if err != nil {
			return nil, faults.Errorf("Unable to scan to struct: %w", err)
		}

		events = append(events, toEventsourcingEvent(pgEvent))
	}
	return events, nil
}

func toEventsourcingEvent(pgEvent Event) eventsourcing.Event {
	return eventsourcing.Event{
		ID:               pgEvent.ID,
		AggregateID:      pgEvent.AggregateID,
		AggregateIDHash:  uint32(pgEvent.AggregateIDHash),
		AggregateVersion: pgEvent.AggregateVersion,
		AggregateType:    pgEvent.AggregateType,
		Kind:             pgEvent.Kind,
		Body:             pgEvent.Body,
		Metadata:         pgEvent.Metadata,
		CreatedAt:        pgEvent.CreatedAt,
	}
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
	if revision < 1 {
		return faults.New("revision must be greater than zero")
	}
	if snapshotThreshold > 0 && (aggregateFactory == nil || rehydrateFunc == nil || encoder == nil) {
		return faults.New("if snapshot threshold is greather than zero then aggregate factory, rehydrate function and encoder must be defined.")
	}

	// loops until it exhausts all streams with the event that we want to migrate
	for {
		events, err := r.eventsForMigration(ctx, aggregateType, eventTypeCriteria)
		if err != nil {
			return err
		}
		// no more streams
		if len(events) == 0 {
			return nil
		}

		migration, err := handler(events)
		if err != nil {
			return err
		}

		last := events[len(events)-1]
		err = r.saveMigration(ctx, last, migration, snapshotThreshold, aggregateFactory, rehydrateFunc, encoder, revision)
		if !errors.Is(err, eventsourcing.ErrConcurrentModification) {
			return err
		}
	}
}

func (r *EsRepository) eventsForMigration(ctx context.Context, aggregateType eventsourcing.AggregateType, eventTypeCriteria []eventsourcing.EventKind) ([]*eventsourcing.Event, error) {
	if aggregateType == "" {
		return nil, faults.New("aggregate type needs to be specified")
	}
	if len(eventTypeCriteria) == 0 {
		return nil, faults.New("event type criteria needs to be specified")
	}

	args := []interface{}{aggregateType}
	var subquery bytes.Buffer
	// get the id of the aggregate
	subquery.WriteString("SELECT aggregate_id FROM events WHERE aggregate_type = $1 AND migrated = 0 AND (")
	for k, v := range eventTypeCriteria {
		if k > 0 {
			subquery.WriteString(" OR ")
		}
		args = append(args, v)
		subquery.WriteString(fmt.Sprintf("kind = $%d", len(args)))
	}
	subquery.WriteString(") ORDER BY id ASC LIMIT 1")

	// get all events for the aggregate id returned by the subquery
	events := []*Event{}
	query := fmt.Sprintf("SELECT * FROM events WHERE aggregate_id = (%s) AND migrated = 0 ORDER BY aggregate_version ASC", subquery.String())
	err := r.db.SelectContext(ctx, &events, query, args...)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, faults.Errorf("unable to query events: %w\n%s", err, query)
	}

	evts := make([]*eventsourcing.Event, len(events))
	for k, v := range events {
		e := toEventsourcingEvent(*v)
		evts[k] = &e
	}
	return evts, nil
}

func (r *EsRepository) saveMigration(
	ctx context.Context,
	last *eventsourcing.Event,
	migration []*eventsourcing.EventMigration,
	snapshotThreshold uint32,
	aggregateFactory func() (eventsourcing.Aggregater, error),
	rehydrateFunc func(eventsourcing.Aggregater, eventsourcing.Event) error,
	encoder eventsourcing.Encoder,
	revision int,
) error {
	version := last.AggregateVersion
	entropy := eventid.EntropyFactory(time.Now().UTC())

	return r.withTx(ctx, func(c context.Context, tx *sql.Tx) error {
		// invalidate event
		version++
		_, err := r.saveEvent(c, tx, entropy, Event{
			AggregateID:      last.AggregateID,
			AggregateIDHash:  int32ring(last.AggregateIDHash),
			AggregateVersion: version,
			AggregateType:    last.AggregateType,
			Kind:             eventsourcing.InvalidatedKind,
			CreatedAt:        time.Now().UTC(),
		})
		if err != nil {
			return err
		}

		// invalidate all active events
		_, err = tx.ExecContext(c, "UPDATE events SET migrated = $1 WHERE aggregate_id = $2 AND migrated = 0", revision, last.AggregateID)
		if err != nil {
			return faults.Errorf("failed to invalidate events: %w", err)
		}

		// delete snapshots
		_, err = tx.ExecContext(c, "DELETE FROM snapshots WHERE aggregate_id = $1", last.AggregateID)
		if err != nil {
			return faults.Errorf("failed to delete stale snapshots: %w", err)
		}

		var aggregate eventsourcing.Aggregater
		if snapshotThreshold > 0 && len(migration) >= int(snapshotThreshold) {
			aggregate, err = aggregateFactory()
			if err != nil {
				return faults.Wrap(err)
			}
		}

		// insert new events
		var lastID eventid.EventID
		for _, mig := range migration {
			version++
			event := Event{
				AggregateID:      last.AggregateID,
				AggregateIDHash:  int32ring(last.AggregateIDHash),
				AggregateVersion: version,
				AggregateType:    last.AggregateType,
				Kind:             mig.Kind,
				Body:             mig.Body,
				IdempotencyKey:   NilString(mig.IdempotencyKey),
				Metadata:         mig.Metadata,
				CreatedAt:        time.Now().UTC(),
			}
			lastID, err = r.saveEvent(c, tx, entropy, event)
			if err != nil {
				return err
			}
			if aggregate != nil {
				event.ID = lastID
				err = rehydrateFunc(aggregate, toEventsourcingEvent(event))
				if err != nil {
					return err
				}
			}
		}

		if aggregate != nil {
			body, err := encoder.Encode(aggregate)
			if err != nil {
				return faults.Errorf("failed to encode aggregate on migration: %w", err)
			}

			err = saveSnapshot(c, tx, Snapshot{
				ID:               lastID,
				AggregateID:      aggregate.GetID(),
				AggregateVersion: aggregate.GetVersion(),
				AggregateType:    eventsourcing.AggregateType(aggregate.GetType()),
				Body:             body,
				CreatedAt:        time.Now().UTC(),
			})
			if err != nil {
				return err
			}
		}

		return nil
	})
}
