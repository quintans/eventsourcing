package mysql

import (
	"bytes"
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	"github.com/oklog/ulid/v2"
	"github.com/quintans/faults"

	"github.com/quintans/eventsourcing"
	"github.com/quintans/eventsourcing/encoding"
	"github.com/quintans/eventsourcing/eventid"
	"github.com/quintans/eventsourcing/projection"
	"github.com/quintans/eventsourcing/store"
	"github.com/quintans/eventsourcing/util"
)

const (
	driverName      = "mysql"
	uniqueViolation = 1062
)

// Event is the event data is stored in the database
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

var (
	_ eventsourcing.EsRepository[*ulid.ULID]  = (*EsRepository[*ulid.ULID])(nil)
	_ projection.EventsRepository[*ulid.ULID] = (*EsRepository[*ulid.ULID])(nil)
)

type Option[K eventsourcing.ID] func(*EsRepository[K])

func WithTxHandler[K eventsourcing.ID](txHandler store.InTxHandler[K]) Option[K] {
	return func(r *EsRepository[K]) {
		r.txHandlers = append(r.txHandlers, txHandler)
	}
}

type Repository struct {
	db *sqlx.DB
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

type EsRepository[K eventsourcing.ID] struct {
	Repository
	txHandlers []store.InTxHandler[K]
}

func NewStoreWithURL[K eventsourcing.ID](connString string, options ...Option[K]) (*EsRepository[K], error) {
	db, err := sql.Open(driverName, connString)
	if err != nil {
		return nil, faults.Wrap(err)
	}

	return NewStore[K](db, options...), nil
}

func NewStore[K eventsourcing.ID](db *sql.DB, options ...Option[K]) *EsRepository[K] {
	dbx := sqlx.NewDb(db, driverName)
	r := &EsRepository[K]{
		Repository: Repository{
			db: dbx,
		},
	}

	for _, opt := range options {
		opt(r)
	}

	return r
}

func (r *EsRepository[K]) Connection() *sql.DB {
	return r.db.DB
}

func (r *EsRepository[K]) SaveEvent(ctx context.Context, eRec *eventsourcing.EventRecord[K]) (eventid.EventID, uint32, error) {
	idempotencyKey := eRec.IdempotencyKey

	version := eRec.Version
	var id eventid.EventID
	err := r.WithTx(ctx, func(c context.Context, tx *sql.Tx) error {
		for _, e := range eRec.Details {
			version++
			aggIDStr := eRec.AggregateID.String()
			var err error
			id = e.ID
			err = r.saveEvent(c, tx, &Event{
				ID:               id,
				AggregateID:      aggIDStr,
				AggregateIDHash:  util.HashInt(aggIDStr),
				AggregateVersion: version,
				AggregateKind:    eRec.AggregateKind,
				Kind:             e.Kind,
				Body:             e.Body,
				IdempotencyKey:   NilString(idempotencyKey),
				Metadata:         encoding.JSONOfMap(eRec.Metadata),
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

func (r *EsRepository[K]) saveEvent(ctx context.Context, tx *sql.Tx, event *Event) error {
	_, err := tx.ExecContext(ctx,
		`INSERT INTO events (id, aggregate_id, aggregate_version, aggregate_kind, kind, body, idempotency_key, metadata, created_at, aggregate_id_hash, migrated)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		event.ID.String(), event.AggregateID, event.AggregateVersion, event.AggregateKind, event.Kind, event.Body, event.IdempotencyKey, event.Metadata, event.CreatedAt, event.AggregateIDHash, event.Migrated)
	if err != nil {
		if isDup(err) {
			return faults.Wrap(eventsourcing.ErrConcurrentModification)
		}
		return faults.Errorf("unable to insert event: %w", err)
	}

	return r.applyTxHandlers(ctx, event)
}

func (r *EsRepository[K]) applyTxHandlers(ctx context.Context, event *Event) error {
	if r.txHandlers == nil {
		return nil
	}

	e, err := toEventsourcingEvent[K](event)
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
	me, ok := err.(*mysql.MySQLError)
	return ok && me.Number == uniqueViolation
}

func (r *EsRepository[K]) GetSnapshot(ctx context.Context, aggregateID K) (eventsourcing.Snapshot[K], error) {
	snap := Snapshot{}
	if err := r.db.GetContext(ctx, &snap, "SELECT * FROM snapshots WHERE aggregate_id = ? ORDER BY id DESC LIMIT 1", aggregateID.String()); err != nil {
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

func (r *EsRepository[K]) SaveSnapshot(ctx context.Context, snapshot *eventsourcing.Snapshot[K]) error {
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
	     VALUES (?, ?, ?, ?, ?, ?)`,
		s.ID, s.AggregateID, s.AggregateVersion, s.AggregateKind, s.Body, s.CreatedAt)

	return faults.Wrap(err)
}

func (r *EsRepository[K]) GetAggregateEvents(ctx context.Context, aggregateID K, snapVersion int) ([]*eventsourcing.Event[K], error) {
	var query bytes.Buffer
	query.WriteString("SELECT * FROM events e WHERE e.aggregate_id = ? AND migration = 0")
	args := []interface{}{aggregateID.String()}
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

type txKey struct{}

func TxFromContext(ctx context.Context) *sql.Tx {
	tx, _ := ctx.Value(txKey{}).(*sql.Tx) // with _ it will not panic if nil
	return tx
}

func (r *EsRepository[K]) HasIdempotencyKey(ctx context.Context, idempotencyKey string) (bool, error) {
	var exists bool
	err := r.db.GetContext(ctx, &exists, `SELECT EXISTS(SELECT 1 FROM events WHERE idempotency_key=? AND migration = 0) AS "EXISTS"`, idempotencyKey)
	if err != nil {
		return false, faults.Errorf("Unable to verify the existence of the idempotency key: %w", err)
	}
	return exists, nil
}

func (r *EsRepository[K]) Forget(ctx context.Context, req eventsourcing.ForgetRequest[K], forget func(kind eventsourcing.Kind, body []byte, snapshot bool) ([]byte, error)) error {
	// When Forget() is called, the aggregate is no longer used, therefore if it fails, it can be called again.

	// Forget events
	events, err := r.queryEvents(ctx, "SELECT * FROM events WHERE aggregate_id = ? AND kind = ?", req.AggregateID, req.EventKind)
	if err != nil {
		return faults.Errorf("Unable to get events for Aggregate '%s' and event kind '%s': %w", req.AggregateID, req.EventKind, err)
	}

	for _, evt := range events {
		body, err := forget(evt.Kind, evt.Body, false)
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
		body, err := forget(snap.AggregateKind, snap.Body, true)
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

func (r *EsRepository[K]) GetEvents(ctx context.Context, after, until eventid.EventID, batchSize int, filter store.Filter) ([]*eventsourcing.Event[K], error) {
	var query bytes.Buffer
	query.WriteString("SELECT * FROM events WHERE id > ? AND id <= ? AND migration = 0")
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
			query.WriteString("aggregate_kind = ?")
		}
		query.WriteString(")")
		conditions = append(conditions, query.String())
	}

	if filter.Splits > 1 && filter.Split > 1 {
		args = append(args, filter.Splits, filter.Split)
		conditions = append(conditions, "MOD(aggregate_id_hash, ?) = ?")
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
				query.WriteString(fmt.Sprintf(`JSON_EXTRACT(metadata, '$.%s') = '%s'`, k, v))
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

func (r *EsRepository[K]) queryEvents(ctx context.Context, query string, args ...interface{}) ([]*eventsourcing.Event[K], error) {
	return queryEvents[K](ctx, r.db, query, args...)
}

func queryEvents[K eventsourcing.ID](ctx context.Context, db *sqlx.DB, query string, args ...interface{}) ([]*eventsourcing.Event[K], error) {
	rows, err := db.QueryxContext(ctx, query, args...)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, faults.Errorf("unable to query events with %s: %w", query, err)
	}
	events := []*eventsourcing.Event[K]{}
	for rows.Next() {
		event := &Event{}
		err := rows.StructScan(event)
		if err != nil {
			return nil, faults.Errorf("unable to scan to struct: %w", err)
		}
		evt, err := toEventsourcingEvent[K](event)
		if err != nil {
			return nil, err
		}
		events = append(events, evt)
	}
	return events, nil
}

func toEventsourcingEvent[K eventsourcing.ID](e *Event) (*eventsourcing.Event[K], error) {
	var id K
	err := id.UnmarshalText([]byte(e.AggregateID))
	if err != nil {
		return nil, faults.Errorf("unmarshaling id '%s': %w", e.AggregateID, err)
	}
	return &eventsourcing.Event[K]{
		ID:               e.ID,
		AggregateID:      id,
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

func (r *EsRepository[K]) GetEventsByIDs(ctx context.Context, ids []string) ([]*eventsourcing.Event[K], error) {
	qry, args, err := sqlx.In("SELECT * FROM events WHERE id IN (?) ORDER BY id ASC", ids) // the query must use the '?' bind var
	if err != nil {
		return nil, faults.Errorf("getting pending events (IDs=%+v): %w", ids, err)
	}
	qry = r.db.Rebind(qry) // sqlx.In returns queries with the `?` bindvar, we can rebind it for our backend

	return queryEvents[K](ctx, r.db, qry, args...)
}
