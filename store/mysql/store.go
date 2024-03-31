package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	"github.com/oklog/ulid/v2"
	"github.com/quintans/faults"

	"github.com/quintans/eventsourcing"
	"github.com/quintans/eventsourcing/eventid"
	"github.com/quintans/eventsourcing/projection"
	"github.com/quintans/eventsourcing/store"
	"github.com/quintans/eventsourcing/util"
)

const (
	driverName      = "mysql"
	uniqueViolation = 1062
)

const (
	defSnapTable = "snapshots"
	coreSnapCols = "id, aggregate_id, aggregate_version, aggregate_kind, body, created_at"
	coreSnapVars = "?, ?, ?, ?, ?, ?"

	defEventsTable = "events"
	coreEventCols  = "id, aggregate_id, aggregate_id_hash, aggregate_version, aggregate_kind, kind, body, idempotency_key, created_at, migration, migrated"
	coreEventVars  = "?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?"
)

// Event is the event data is stored in the database
type Event struct {
	ID               eventid.EventID
	AggregateID      string
	AggregateIDHash  int32
	AggregateVersion uint32
	AggregateKind    eventsourcing.Kind
	Kind             eventsourcing.Kind
	Body             []byte
	IdempotencyKey   store.NilString
	CreatedAt        time.Time
	Migration        int
	Migrated         bool
	Metadata         eventsourcing.Metadata
}

type Snapshot struct {
	ID               eventid.EventID
	AggregateID      string
	AggregateVersion uint32
	AggregateKind    eventsourcing.Kind
	Body             []byte
	CreatedAt        time.Time
	Metadata         eventsourcing.Metadata
}

var (
	_ eventsourcing.EsRepository[ulid.ULID]  = (*EsRepository[ulid.ULID, *ulid.ULID])(nil)
	_ projection.EventsRepository[ulid.ULID] = (*EsRepository[ulid.ULID, *ulid.ULID])(nil)
)

type Option[K eventsourcing.ID, PK eventsourcing.IDPt[K]] func(*EsRepository[K, PK])

func WithTxHandler[K eventsourcing.ID, PK eventsourcing.IDPt[K]](txHandler store.InTxHandler[K]) Option[K, PK] {
	return func(r *EsRepository[K, PK]) {
		r.txHandlers = append(r.txHandlers, txHandler)
	}
}

// WithMetadata defines the metadata to be save on every event. Data keys will be converted to lower case
func WithMetadata[K eventsourcing.ID, PK eventsourcing.IDPt[K]](metadata eventsourcing.Metadata) Option[K, PK] {
	return func(r *EsRepository[K, PK]) {
		m := eventsourcing.Metadata{}
		for k, v := range metadata {
			m[strings.ToLower(k)] = v
		}
		r.metadata = m
	}
}

// WithMetadataHook defines the hook that will return the metadata.
// This metadata will override any metadata defined at the repository level
func WithMetadataHook[K eventsourcing.ID, PK eventsourcing.IDPt[K]](fn store.MetadataHook[K]) Option[K, PK] {
	return func(r *EsRepository[K, PK]) {
		r.metadataHook = fn
	}
}

func WithEventsTable[K eventsourcing.ID, PK eventsourcing.IDPt[K]](table string) Option[K, PK] {
	return func(r *EsRepository[K, PK]) {
		r.eventsTable = table
	}
}

func WithSnapshotsTable[K eventsourcing.ID, PK eventsourcing.IDPt[K]](table string) Option[K, PK] {
	return func(r *EsRepository[K, PK]) {
		r.snapshotsTable = table
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

type EsRepository[K eventsourcing.ID, PK eventsourcing.IDPt[K]] struct {
	Repository
	eventsTable    string
	snapshotsTable string
	txHandlers     []store.InTxHandler[K]
	metadata       eventsourcing.Metadata
	metadataHook   store.MetadataHook[K]
}

func NewStoreWithURL[K eventsourcing.ID, PK eventsourcing.IDPt[K]](connString string, options ...Option[K, PK]) (*EsRepository[K, PK], error) {
	db, err := sql.Open(driverName, connString)
	if err != nil {
		return nil, faults.Wrap(err)
	}

	return NewStore[K](db, options...), nil
}

func NewStore[K eventsourcing.ID, PK eventsourcing.IDPt[K]](db *sql.DB, options ...Option[K, PK]) *EsRepository[K, PK] {
	dbx := sqlx.NewDb(db, driverName)
	r := &EsRepository[K, PK]{
		Repository: Repository{
			db: dbx,
		},
		eventsTable:    defEventsTable,
		snapshotsTable: defSnapTable,
	}

	for _, opt := range options {
		opt(r)
	}

	return r
}

func (r *EsRepository[K, PK]) Connection() *sql.DB {
	return r.db.DB
}

func (r *EsRepository[K, PK]) SaveEvent(ctx context.Context, eRec *eventsourcing.EventRecord[K]) (eventid.EventID, uint32, error) {
	idempotencyKey := eRec.IdempotencyKey

	version := eRec.Version
	var id eventid.EventID
	err := r.WithTx(ctx, func(c context.Context, tx *sql.Tx) error {
		for _, e := range eRec.Details {
			version++
			aggIDStr := eRec.AggregateID.String()
			var err error
			id = e.ID
			metadata := r.metadataMerge(ctx, r.metadata, store.OnPersist)
			err = r.saveEvent(c, tx, &Event{
				ID:               id,
				AggregateID:      aggIDStr,
				AggregateIDHash:  util.HashToInt(aggIDStr),
				AggregateVersion: version,
				AggregateKind:    eRec.AggregateKind,
				Kind:             e.Kind,
				Body:             e.Body,
				IdempotencyKey:   store.NilString(idempotencyKey),
				CreatedAt:        eRec.CreatedAt,
				Metadata:         metadata,
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

func (r *EsRepository[K, PK]) metadataMerge(ctx context.Context, metadata eventsourcing.Metadata, kind store.MetadataHookKind) eventsourcing.Metadata {
	if r.metadataHook == nil {
		return metadata
	}
	meta := r.metadataHook(store.NewMetadataHookContext(ctx, kind))
	return util.MapMerge(metadata, meta)
}

func (r *EsRepository[K, PK]) saveEvent(ctx context.Context, tx *sql.Tx, event *Event) error {
	columns := []string{coreEventCols}
	values := []any{
		event.ID.String(),
		event.AggregateID,
		event.AggregateIDHash,
		event.AggregateVersion,
		event.AggregateKind,
		event.Kind,
		event.Body,
		event.IdempotencyKey,
		event.CreatedAt,
		event.Migration,
		event.Migrated,
	}

	vars := []string{coreEventVars}
	for k, v := range event.Metadata {
		columns = append(columns, store.MetaColumnPrefix+k)
		vars = append(vars, "?")
		values = append(values, v)
	}

	query := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", r.eventsTable, strings.Join(columns, ", "), strings.Join(vars, ", "))

	_, err := tx.ExecContext(ctx, query, values...)
	if err != nil {
		if isDup(err) {
			return faults.Wrap(eventsourcing.ErrConcurrentModification)
		}
		return faults.Errorf("unable to insert event, query: %s, args: %+v: %w", query, values, err)
	}

	return r.applyTxHandlers(ctx, event)
}

func (r *EsRepository[K, PK]) applyTxHandlers(ctx context.Context, event *Event) error {
	if r.txHandlers == nil {
		return nil
	}

	e, err := toEventSourcingEvent[K, PK](event)
	if err != nil {
		return err
	}
	for _, handler := range r.txHandlers {
		err := handler(store.NewInTxHandlerContext(ctx, e))
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

func (r *EsRepository[K, PK]) GetSnapshot(ctx context.Context, aggregateID K) (eventsourcing.Snapshot[K], error) {
	query := strings.Builder{}
	query.WriteString(fmt.Sprintf("SELECT * FROM %s WHERE aggregate_id = ?", r.snapshotsTable))
	args := []any{aggregateID.String()}

	metadata := r.metadataMerge(ctx, r.metadata, store.OnRetrieve)
	for k, v := range metadata {
		args = append(args, v)
		query.WriteString(fmt.Sprintf(" AND %s%s = ?", store.MetaColumnPrefix, k))
	}
	query.WriteString(" ORDER BY id DESC LIMIT 1")
	snaps, err := r.getSnapshots(ctx, metadata, query.String(), args...)
	if err != nil {
		if err == sql.ErrNoRows {
			return eventsourcing.Snapshot[K]{}, nil
		}
		return eventsourcing.Snapshot[K]{}, faults.Errorf("getting snapshot for aggregate '%s': %w", aggregateID, err)
	}
	if len(snaps) == 0 {
		return eventsourcing.Snapshot[K]{}, nil
	}

	return snaps[0], nil
}

func (r *EsRepository[K, PK]) getSnapshots(ctx context.Context, metadata eventsourcing.Metadata, query string, args ...any) ([]eventsourcing.Snapshot[K], error) {
	columns := []string{coreSnapCols}
	for k := range metadata {
		columns = append(columns, store.MetaColumnPrefix+k)
	}
	query = strings.Replace(query, "SELECT *", fmt.Sprintf("SELECT %s", strings.Join(columns, ", ")), 1)

	rows, err := r.db.QueryxContext(ctx, query, args...)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, faults.Errorf("querying snapshots with %s, args=%+v: %w", query, args, err)
	}
	snaps := []eventsourcing.Snapshot[K]{}
	for rows.Next() {
		snap := eventsourcing.Snapshot[K]{}
		dest := []any{
			&snap.ID,
			&snap.AggregateID,
			&snap.AggregateVersion,
			&snap.AggregateKind,
			&snap.Body,
			&snap.CreatedAt,
		}
		meta := []*store.Metadata{}
		for k := range metadata {
			m := &store.Metadata{Key: k}
			meta = append(meta, m)
			dest = append(dest, &m.Value)
		}

		err := rows.Scan(dest...)
		if err != nil {
			return nil, faults.Wrap(err)
		}

		m := eventsourcing.Metadata{}
		for _, v := range meta {
			m[v.Key] = v.Key
		}

		snap.Metadata = m

		snaps = append(snaps, snap)
	}

	return snaps, nil
}

func (r *EsRepository[K, PK]) SaveSnapshot(ctx context.Context, snapshot *eventsourcing.Snapshot[K]) error {
	metadata := r.metadataMerge(ctx, r.metadata, store.OnPersist)
	return r.saveSnapshot(ctx, r.db, &Snapshot{
		ID:               snapshot.ID,
		AggregateID:      snapshot.AggregateID.String(),
		AggregateVersion: snapshot.AggregateVersion,
		AggregateKind:    snapshot.AggregateKind,
		Body:             snapshot.Body,
		CreatedAt:        snapshot.CreatedAt.UTC(),
		Metadata:         metadata,
	})
}

type sqlExecuter interface {
	ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error)
}

func (r *EsRepository[K, PK]) saveSnapshot(ctx context.Context, x sqlExecuter, s *Snapshot) error {
	values := []any{
		s.ID,
		s.AggregateID,
		s.AggregateVersion,
		s.AggregateKind,
		s.Body,
		s.CreatedAt,
	}
	columns := []string{coreSnapCols}
	vars := []string{coreSnapVars}
	for k, v := range s.Metadata {
		columns = append(columns, store.MetaColumnPrefix+k)
		vars = append(vars, "?")
		values = append(values, v)
	}

	query := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", r.snapshotsTable, strings.Join(columns, ", "), strings.Join(vars, ", "))

	// TODO instead of adding we could replace UPDATE/INSERT
	_, err := x.ExecContext(ctx, query, values...)
	return faults.Wrapf(err, "saving snapshot, query:%s, args: %+v", query, values)
}

func (r *EsRepository[K, PK]) GetAggregateEvents(ctx context.Context, aggregateID K, snapVersion int) ([]*eventsourcing.Event[K], error) {
	var query strings.Builder
	query.WriteString(fmt.Sprintf("SELECT * FROM %s WHERE aggregate_id = ? AND migration = 0", r.eventsTable))
	args := []interface{}{aggregateID.String()}
	if snapVersion > -1 {
		query.WriteString(" AND aggregate_version > ?")
		args = append(args, snapVersion)
	}

	metadata := r.metadataMerge(ctx, r.metadata, store.OnRetrieve)
	for k, v := range metadata {
		args = append(args, v)
		query.WriteString(fmt.Sprintf(" AND %s%s = ?", store.MetaColumnPrefix, k))
	}
	query.WriteString(" ORDER BY aggregate_version ASC")

	events, err := r.queryEvents(ctx, metadata, query.String(), args...)
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
	qry := fmt.Sprintf(`SELECT EXISTS(SELECT 1 FROM %s WHERE idempotency_key=? AND migration = 0) AS "EXISTS"`, r.eventsTable)
	err := r.db.GetContext(ctx, &exists, qry, idempotencyKey)
	if err != nil {
		return false, faults.Errorf("Unable to verify the existence of the idempotency key: %w", err)
	}
	return exists, nil
}

func (r *EsRepository[K, PK]) Forget(ctx context.Context, req eventsourcing.ForgetRequest[K], forget func(kind eventsourcing.Kind, body []byte) ([]byte, error)) error {
	// When Forget() is called, the aggregate is no longer used, therefore if it fails, it can be called again.

	query := strings.Builder{}
	query.WriteString(fmt.Sprintf("SELECT * FROM %s WHERE aggregate_id = ? AND kind = ?", r.eventsTable))
	args := []any{req.AggregateID.String(), req.EventKind}

	// Forget events
	events, err := r.queryEvents(ctx, nil, query.String(), args...)
	if err != nil {
		return faults.Errorf("getting events for Aggregate '%s' and event kind '%s': %w", req.AggregateID, req.EventKind, err)
	}

	qry := fmt.Sprintf("UPDATE %s SET body = ? WHERE ID = ?", r.eventsTable)
	for _, evt := range events {
		body, err := forget(evt.Kind, evt.Body)
		if err != nil {
			return err
		}
		_, err = r.db.ExecContext(ctx, qry, body, evt.ID.String())
		if err != nil {
			return faults.Errorf("forgetting event ID %s: %w", evt.ID, err)
		}
	}

	// forget snapshots
	qry = fmt.Sprintf("SELECT * FROM %s WHERE aggregate_id = ?", r.snapshotsTable)
	snaps, err := r.getSnapshots(ctx, nil, qry, req.AggregateID.String())
	if err != nil {
		return faults.Errorf("getting snapshot for aggregate '%s': %w", req.AggregateID, err)
	}

	qry = fmt.Sprintf("UPDATE %s SET body = ? WHERE ID = ?", r.snapshotsTable)
	for _, snap := range snaps {
		body, err := forget(snap.AggregateKind, snap.Body)
		if err != nil {
			return err
		}
		_, err = r.db.ExecContext(ctx, qry, body, snap.ID.String())
		if err != nil {
			return faults.Errorf("forgetting snapshot ID %s: %w", snap.ID, err)
		}
	}

	return nil
}

func (r *EsRepository[K, PK]) GetEvents(ctx context.Context, after, until eventid.EventID, batchSize int, filter store.Filter) ([]*eventsourcing.Event[K], error) {
	var query strings.Builder
	query.WriteString(fmt.Sprintf("SELECT * FROM %s WHERE id > ? AND id <= ? AND migration = 0", r.eventsTable))
	args := []interface{}{after.String(), until.String()}
	metadata := r.metadataMerge(ctx, r.metadata, store.OnRetrieve)
	args = buildFilter(&query, " AND ", metadata, filter, args)
	query.WriteString(" ORDER BY id ASC")
	if batchSize > 0 {
		query.WriteString(" LIMIT ")
		query.WriteString(strconv.Itoa(batchSize))
	}

	rows, err := r.queryEvents(ctx, metadata, query.String(), args...)
	if err != nil {
		return nil, faults.Errorf("getting events between ('%d', '%s'] for filter %+v: %w", after, until, filter, err)
	}
	if len(rows) == 0 {
		return nil, nil
	}

	return rows, nil
}

func buildFilter(qry *strings.Builder, prefix string, metadata eventsourcing.Metadata, filter store.Filter, args []interface{}) []interface{} {
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

	if filter.Splits > 1 && len(filter.SplitIDs) != int(filter.Splits) {
		args = append(args, filter.Splits)
		s := strings.Builder{}
		for k, v := range filter.SplitIDs {
			if k > 0 {
				s.WriteString(", ")
			}
			s.WriteString("?")
			args = append(args, v)
		}
		conditions = append(conditions, fmt.Sprintf("MOD(aggregate_id_hash, ?) IN (%s)", s.String()))
	}

	for k, v := range metadata {
		args = append(args, v)
		qry.WriteString(fmt.Sprintf(" AND %s%s = ?", store.MetaColumnPrefix, k))
	}

	if len(filter.Metadata) > 0 {
		for _, kv := range filter.Metadata {
			// ignore if already set by the metadata
			if metadata != nil {
				_, ok := metadata[kv.Key]
				if ok {
					continue
				}
			}

			var query strings.Builder
			query.WriteString("(")
			for idx, v := range kv.Values {
				if idx > 0 {
					query.WriteString(" OR ")
				}
				query.WriteString(fmt.Sprintf("%s%s = ?", store.MetaColumnPrefix, kv.Key))
				args = append(args, v)
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

func (r *EsRepository[K, PK]) queryEvents(ctx context.Context, metadata eventsourcing.Metadata, query string, args ...any) ([]*eventsourcing.Event[K], error) {
	columns := []string{coreEventCols}
	base := []store.Metadata{}
	for k := range metadata {
		columns = append(columns, store.MetaColumnPrefix+k)
		base = append(base, store.Metadata{Key: k})
	}
	query = strings.Replace(query, "SELECT *", fmt.Sprintf("SELECT %s", strings.Join(columns, ", ")), 1)

	rows, err := r.db.QueryxContext(ctx, query, args...)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, faults.Errorf("querying events, query=%s, args=%+v: %w", query, args, err)
	}
	events := []*eventsourcing.Event[K]{}
	for rows.Next() {
		event := Event{}
		dest := []any{
			&event.ID,
			&event.AggregateID,
			&event.AggregateIDHash,
			&event.AggregateVersion,
			&event.AggregateKind,
			&event.Kind,
			&event.Body,
			&event.IdempotencyKey,
			&event.CreatedAt,
			&event.Migration,
			&event.Migrated,
		}

		meta := []*store.Metadata{}
		for k := range metadata {
			m := &store.Metadata{Key: k}
			meta = append(meta, m)
			dest = append(dest, &m.Value)
		}

		err := rows.Scan(dest...)
		if err != nil {
			return nil, faults.Errorf("unable to scan to struct: %w", err)
		}

		m := eventsourcing.Metadata{}
		for _, v := range meta {
			m[v.Key] = v.Value
		}

		event.Metadata = m
		evt, err := toEventSourcingEvent[K, PK](&event)
		if err != nil {
			return nil, err
		}

		events = append(events, evt)
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

func (r *EsRepository[K, PK]) GetEventsByRawIDs(ctx context.Context, ids []string) ([]*eventsourcing.Event[K], error) {
	qry, args, err := sqlx.In(fmt.Sprintf("SELECT * FROM %s WHERE id IN (?) ORDER BY id ASC", r.eventsTable), ids) // the query must use the '?' bind var
	if err != nil {
		return nil, faults.Errorf("getting pending events (IDs=%+v): %w", ids, err)
	}
	qry = r.db.Rebind(qry) // sqlx.In returns queries with the `?` bindvar, we can rebind it for our backend

	return r.queryEvents(ctx, nil, qry, args...)
}
