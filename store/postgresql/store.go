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
	"github.com/oklog/ulid/v2"
	"github.com/quintans/faults"

	"github.com/quintans/eventsourcing"
	"github.com/quintans/eventsourcing/encoding"
	"github.com/quintans/eventsourcing/eventid"
	"github.com/quintans/eventsourcing/projection"
	"github.com/quintans/eventsourcing/store"
	"github.com/quintans/eventsourcing/util/ids"
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

type Option[K eventsourcing.ID, PK eventsourcing.IDPt[K]] func(*EsRepository[K, PK])

func WithTxHandler[K eventsourcing.ID, PK eventsourcing.IDPt[K]](txHandler store.InTxHandler[K]) Option[K, PK] {
	return func(r *EsRepository[K, PK]) {
		r.txHandlers = append(r.txHandlers, txHandler)
	}
}

type Repository struct {
	db *sqlx.DB
}

func (r Repository) Connection() *sqlx.DB {
	return r.db
}

func (r Repository) TxRunner() func(ctx context.Context, fn func(context.Context) error) error {
	return func(ctx context.Context, fn func(context.Context) error) error {
		return r.WithTx(ctx, func(c context.Context, _ *sql.Tx) error {
			return fn(c)
		})
	}
}

func (r *Repository) WithTx(ctx context.Context, fn func(context.Context, *sql.Tx) error) error {
	tx := TxFromContext(ctx)
	if tx != nil {
		return fn(ctx, tx)
	}

	return r.wrapWithTx(ctx, fn)
}

func (r *Repository) wrapWithTx(ctx context.Context, fn func(context.Context, *sql.Tx) error) error {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return faults.Wrap(err)
	}
	defer tx.Rollback()

	ctx = context.WithValue(ctx, txKey{}, tx)
	err = fn(ctx, tx)
	if err != nil {
		return err
	}
	return tx.Commit()
}

var (
	_ eventsourcing.EsRepository[ulid.ULID]  = (*EsRepository[ulid.ULID, *ulid.ULID])(nil)
	_ projection.EventsRepository[ulid.ULID] = (*EsRepository[ulid.ULID, *ulid.ULID])(nil)
)

type EsRepository[K eventsourcing.ID, PK eventsourcing.IDPt[K]] struct {
	Repository
	txHandlers []store.InTxHandler[K]
}

func NewStoreWithURL[K eventsourcing.ID, PK eventsourcing.IDPt[K]](connString string, options ...Option[K, PK]) (*EsRepository[K, PK], error) {
	db, err := sql.Open(driverName, connString)
	if err != nil {
		return nil, faults.Wrap(err)
	}

	return NewStore[K, PK](db, options...), nil
}

func NewStore[K eventsourcing.ID, PK eventsourcing.IDPt[K]](db *sql.DB, options ...Option[K, PK]) *EsRepository[K, PK] {
	dbx := sqlx.NewDb(db, driverName)
	r := &EsRepository[K, PK]{
		Repository: Repository{
			db: dbx,
		},
	}

	for _, opt := range options {
		opt(r)
	}

	return r
}

func (r *EsRepository[K, PK]) SaveEvent(ctx context.Context, eRec *eventsourcing.EventRecord[K]) (eventid.EventID, uint32, error) {
	idempotencyKey := eRec.IdempotencyKey

	version := eRec.Version
	var id eventid.EventID
	err := r.WithTx(ctx, func(c context.Context, tx *sql.Tx) error {
		for _, e := range eRec.Details {
			version++
			id = e.ID
			aggIDStr := eRec.AggregateID.String()
			err := r.saveEvent(c, tx, &Event{
				ID:               id,
				AggregateID:      aggIDStr,
				AggregateIDHash:  ids.HashInt(aggIDStr),
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

func (r *EsRepository[K, PK]) saveEvent(ctx context.Context, tx *sql.Tx, event *Event) error {
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

	return r.applyTxHandlers(ctx, event)
}

func (r *EsRepository[K, PK]) applyTxHandlers(ctx context.Context, event *Event) error {
	if len(r.txHandlers) == 0 {
		return nil
	}

	e, err := toEventSourcingEvent[K, PK](event)
	if err != nil {
		return err
	}
	for _, handler := range r.txHandlers {
		err := handler(ctx, e)
		if err != nil {
			return faults.Wrap(err)
		}
	}

	return nil
}

func isDup(err error) bool {
	pgerr, ok := err.(*pq.Error)
	return ok && pgerr.Code == pgUniqueViolation
}

func (r *EsRepository[K, PK]) GetSnapshot(ctx context.Context, aggregateID K) (eventsourcing.Snapshot[K], error) {
	snap := Snapshot{}
	if err := r.db.GetContext(ctx, &snap, "SELECT * FROM snapshots WHERE aggregate_id = $1 ORDER BY id DESC LIMIT 1", aggregateID.String()); err != nil {
		if err == sql.ErrNoRows {
			return eventsourcing.Snapshot[K]{}, nil
		}
		return eventsourcing.Snapshot[K]{}, faults.Errorf("Unable to get snapshot for aggregate '%s': %w", aggregateID, err)
	}

	return eventsourcing.Snapshot[K]{
		ID:               snap.ID,
		AggregateID:      aggregateID,
		AggregateVersion: snap.AggregateVersion,
		AggregateKind:    snap.AggregateKind,
		Body:             snap.Body,
		CreatedAt:        snap.CreatedAt,
	}, nil
}

func (r *EsRepository[K, PK]) SaveSnapshot(ctx context.Context, snapshot *eventsourcing.Snapshot[K]) error {
	return saveSnapshot(ctx, r.db, &Snapshot{
		ID:               snapshot.ID,
		AggregateID:      snapshot.AggregateID.String(),
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

func (r *EsRepository[K, PK]) GetAggregateEvents(ctx context.Context, aggregateID K, snapVersion int) ([]*eventsourcing.Event[K], error) {
	var query bytes.Buffer
	query.WriteString("SELECT * FROM events e WHERE e.aggregate_id = $1 AND migration = 0")
	args := []interface{}{aggregateID.String()}
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

func (r *EsRepository[K, PK]) HasIdempotencyKey(ctx context.Context, idempotencyKey string) (bool, error) {
	var exists bool
	err := r.db.GetContext(ctx, &exists, `SELECT EXISTS(SELECT 1 FROM events WHERE idempotency_key=$1 AND migration = 0) AS "EXISTS"`, idempotencyKey)
	if err != nil {
		return false, faults.Errorf("Unable to verify the existence of the idempotency key: %w", err)
	}
	return exists, nil
}

func (r *EsRepository[K, PK]) Forget(ctx context.Context, request eventsourcing.ForgetRequest[K], forget func(kind eventsourcing.Kind, body []byte, snapshot bool) ([]byte, error)) error {
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

func (r *EsRepository[K, PK]) GetEvents(ctx context.Context, after, until eventid.EventID, batchSize int, filter store.Filter) ([]*eventsourcing.Event[K], error) {
	var query bytes.Buffer
	query.WriteString("SELECT * FROM events WHERE id > $1 AND id <= $2 AND migration = 0")
	args := []interface{}{after, until}
	args = buildFilter(&query, " AND ", filter, args)
	query.WriteString(" ORDER BY id ASC")
	if batchSize > 0 {
		query.WriteString(" LIMIT ")
		query.WriteString(strconv.Itoa(batchSize))
	}

	rows, err := r.queryEvents(ctx, query.String(), args...)
	if err != nil {
		return nil, faults.Errorf("getting events between ('%d', '%s'] for filter %+v: %w", after, until, filter, err)
	}
	if len(rows) == 0 {
		return nil, nil
	}

	return rows, nil
}

func buildFilter(qry *bytes.Buffer, prefix string, filter store.Filter, args []interface{}) []interface{} {
	var conditions []string
	if len(filter.AggregateKinds) > 0 {
		var query strings.Builder
		query.WriteString("(")
		for k, v := range filter.AggregateKinds {
			if k > 0 {
				query.WriteString(" OR ")
			}
			args = append(args, v)
			query.WriteString(fmt.Sprintf("aggregate_kind = $%d", len(args)))
		}
		query.WriteString(")")
		conditions = append(conditions, query.String())
	}

	if filter.Splits > 1 && filter.Split > 1 {
		size := len(args)
		args = append(args, filter.Splits, filter.Split)
		conditions = append(conditions, fmt.Sprintf("MOD(aggregate_id_hash, $%d) = $%d", size+1, size+2))
	}

	if len(filter.Metadata) > 0 {
		for k, values := range filter.Metadata {
			k = escape(k)
			var query strings.Builder
			query.WriteString("(")
			for idx, v := range values {
				if idx > 0 {
					query.WriteString(" OR ")
				}
				v = escape(v)
				query.WriteString(fmt.Sprintf(`metadata  @> '{"%s": "%s"}'`, k, v))
				query.WriteString(")")
			}
			conditions = append(conditions, query.String())
		}
	}

	if len(conditions) > 0 {
		qry.WriteString(prefix)
		qry.WriteString(strings.Join(conditions, " AND "))
	}
	return args
}

func escape(s string) string {
	return strings.ReplaceAll(s, "'", "''")
}

func (r *EsRepository[K, PK]) queryEvents(ctx context.Context, query string, args ...interface{}) ([]*eventsourcing.Event[K], error) {
	return queryEvents[K, PK](ctx, r.db, query, args...)
}

func queryEvents[K eventsourcing.ID, PK eventsourcing.IDPt[K]](ctx context.Context, db *sqlx.DB, query string, args ...interface{}) ([]*eventsourcing.Event[K], error) {
	rows, err := db.QueryxContext(ctx, query, args...)
	if err != nil {
		if err == sql.ErrNoRows {
			return []*eventsourcing.Event[K]{}, nil
		}
		return nil, faults.Errorf("Unable to query events: %w", err)
	}
	events := []*eventsourcing.Event[K]{}
	for rows.Next() {
		pgEvent := &Event{}
		err := rows.StructScan(pgEvent)
		if err != nil {
			return nil, faults.Errorf("Unable to scan to struct: %w", err)
		}

		event, err := toEventSourcingEvent[K, PK](pgEvent)
		if err != nil {
			return nil, err
		}

		events = append(events, event)
	}
	return events, nil
}

func toEventSourcingEvent[K eventsourcing.ID, PK eventsourcing.IDPt[K]](e *Event) (*eventsourcing.Event[K], error) {
	id := PK(new(K))
	err := id.UnmarshalText([]byte(e.AggregateID))
	if err != nil {
		return nil, faults.Errorf("unmarshaling id '%s': %w", e.AggregateID, err)
	}
	return &eventsourcing.Event[K]{
		ID:               e.ID,
		AggregateID:      *id,
		AggregateIDHash:  uint32(e.AggregateIDHash),
		AggregateVersion: e.AggregateVersion,
		AggregateKind:    e.AggregateKind,
		Kind:             e.Kind,
		Body:             e.Body,
		Metadata:         e.Metadata,
		CreatedAt:        e.CreatedAt,
		Migrated:         e.Migrated,
	}, nil
}

func (r *EsRepository[K, PK]) GetEventsByIDs(ctx context.Context, ids []string) ([]*eventsourcing.Event[K], error) {
	qry, args, err := sqlx.In("SELECT * FROM events WHERE id IN (?) ORDER BY id ASC", ids) // the query must use the '?' bind var
	if err != nil {
		return nil, faults.Errorf("getting pending events (IDs=%v): %w", ids, err)
	}
	qry = r.db.Rebind(qry) // sqlx.In returns queries with the `?` bindvar, we can rebind it for our backend

	return queryEvents[K, PK](ctx, r.db, qry, args...)
}
