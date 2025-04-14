package postgresql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/avast/retry-go/v3"
	"github.com/jmoiron/sqlx"
	"github.com/lib/pq"
	"github.com/oklog/ulid/v2"
	"github.com/quintans/faults"

	"github.com/quintans/eventsourcing"
	"github.com/quintans/eventsourcing/eventid"
	"github.com/quintans/eventsourcing/projection"
	"github.com/quintans/eventsourcing/store"
	"github.com/quintans/eventsourcing/util"
)

const (
	driverName        = "postgres"
	pgUniqueViolation = "23505"
)

const (
	coreSnapCols     = "id, aggregate_id, aggregate_version, aggregate_kind, body, created_at"
	coreSnapVars     = "$1, $2, $3, $4, $5, $6"
	coreSnapVarCount = 6

	defEventsTable    = "events"
	coreEventCols     = "id, aggregate_id, aggregate_id_hash, aggregate_version, aggregate_kind, kind, body, created_at, migration, migrated"
	coreEventVars     = "$1, $2, $3, $4, $5, $6, $7, $8, $9, $10"
	coreEventVarCount = 10
)

// Event is the event data stored in the database
type Event struct {
	ID               eventid.EventID
	AggregateID      string
	AggregateIDHash  int32
	AggregateVersion uint32
	AggregateKind    eventsourcing.Kind
	Kind             eventsourcing.Kind
	Body             []byte
	CreatedAt        time.Time
	Migration        int
	Migrated         bool
	Discriminator    eventsourcing.Discriminator
}

type Snapshot struct {
	ID               eventid.EventID
	AggregateID      string
	AggregateVersion uint32
	AggregateKind    eventsourcing.Kind
	Body             []byte
	CreatedAt        time.Time
	Discriminator    eventsourcing.Discriminator
}

type Option[K eventsourcing.ID, PK eventsourcing.IDPt[K]] func(*EsRepository[K, PK])

func WithTxHandler[K eventsourcing.ID, PK eventsourcing.IDPt[K]](txHandler store.InTxHandler[K]) Option[K, PK] {
	return func(r *EsRepository[K, PK]) {
		r.txHandlers = append(r.txHandlers, txHandler)
	}
}

func WithDiscriminatorKeys[K eventsourcing.ID, PK eventsourcing.IDPt[K]](keys ...string) Option[K, PK] {
	return func(r *EsRepository[K, PK]) {
		for _, v := range keys {
			r.allowedKeys[v] = struct{}{}
		}
	}
}

// WithDiscriminator defines the discriminator to be save on every event. Data keys will be converted to lower case.
// Only keys that are defined, with WithDiscriminatorKeys, will be used.
func WithDiscriminator[K eventsourcing.ID, PK eventsourcing.IDPt[K]](disc eventsourcing.Discriminator) Option[K, PK] {
	return func(r *EsRepository[K, PK]) {
		m := eventsourcing.Discriminator{}
		for k, v := range disc {
			m[strings.ToLower(k)] = v
		}
		r.discriminator = m
	}
}

// WithDiscriminatorHook defines the hook that will return the discriminator.
// This discriminator will override any discriminator defined at the repository level
func WithDiscriminatorHook[K eventsourcing.ID, PK eventsourcing.IDPt[K]](fn store.DiscriminatorHook[K]) Option[K, PK] {
	return func(r *EsRepository[K, PK]) {
		r.discriminatorHook = fn
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

func WithNoPublication[K eventsourcing.ID, PK eventsourcing.IDPt[K]]() Option[K, PK] {
	return func(r *EsRepository[K, PK]) {
		r.publish = false
	}
}

var (
	_ eventsourcing.EsRepository[ulid.ULID]  = (*EsRepository[ulid.ULID, *ulid.ULID])(nil)
	_ projection.EventsRepository[ulid.ULID] = (*EsRepository[ulid.ULID, *ulid.ULID])(nil)
)

type EsRepository[K eventsourcing.ID, PK eventsourcing.IDPt[K]] struct {
	store.Repository
	eventsTable       string
	snapshotsTable    string
	publish           bool
	txHandlers        []store.InTxHandler[K]
	discriminator     eventsourcing.Discriminator
	discriminatorHook store.DiscriminatorHook[K]
	allowedKeys       map[string]struct{}
}

func TryPing(ctx context.Context, db *sql.DB) error {
	err := retry.Do(
		func() error {
			err := db.Ping()
			if err != nil && errors.Is(err, ctx.Err()) {
				return retry.Unrecoverable(err)
			}
			return faults.Wrapf(err, "pinging")
		},
		retry.Attempts(3),
		retry.Delay(time.Second),
	)
	return faults.Wrapf(err, "database did not respond to ping")
}

func NewStoreWithURL[K eventsourcing.ID, PK eventsourcing.IDPt[K]](connString string, options ...Option[K, PK]) (*EsRepository[K, PK], error) {
	db, err := sql.Open(driverName, connString)
	if err != nil {
		return nil, faults.Wrap(err)
	}

	err = TryPing(context.Background(), db)
	if err != nil {
		return nil, faults.Wrap(err)
	}

	return NewStore[K, PK](db, options...)
}

func NewStore[K eventsourcing.ID, PK eventsourcing.IDPt[K]](db *sql.DB, options ...Option[K, PK]) (*EsRepository[K, PK], error) {
	dbx := sqlx.NewDb(db, driverName)
	r := &EsRepository[K, PK]{
		Repository:  store.NewRepository(dbx),
		eventsTable: defEventsTable,
		publish:     true,
		allowedKeys: make(map[string]struct{}),
	}

	for _, opt := range options {
		opt(r)
	}

	if len(r.allowedKeys) == 0 {
		r.discriminatorHook = nil
	}

	err := r.createSchema(dbx)
	if err != nil {
		return nil, err
	}

	return r, nil
}

func (r *EsRepository[K, PK]) SaveEvent(ctx context.Context, eRec *eventsourcing.EventRecord[K]) (eventid.EventID, uint32, error) {
	version := eRec.Version
	var id eventid.EventID
	err := r.WithTx(ctx, func(c context.Context, tx store.Session) error {
		for _, e := range eRec.Details {
			version++
			id = e.ID
			aggIDStr := eRec.AggregateID.String()
			disc := r.discriminatorMerge(ctx, store.OnPersist)
			err := r.saveEvent(c, tx, &Event{
				ID:               id,
				AggregateID:      aggIDStr,
				AggregateIDHash:  util.HashToInt(aggIDStr),
				AggregateVersion: version,
				AggregateKind:    eRec.AggregateKind,
				Kind:             e.Kind,
				Body:             e.Body,
				CreatedAt:        eRec.CreatedAt,
				Discriminator:    disc,
			})
			if err != nil {
				return faults.Wrap(err)
			}
		}

		return nil
	})
	if err != nil {
		return eventid.Zero, 0, err
	}

	return id, version, nil
}

func (r *EsRepository[K, PK]) discriminatorMerge(ctx context.Context, kind store.DiscriminatorHookKind) eventsourcing.Discriminator {
	return store.DiscriminatorMerge(
		ctx,
		r.allowedKeys,
		r.discriminator,
		r.discriminatorHook,
		kind,
	)
}

func (r *EsRepository[K, PK]) saveEvent(ctx context.Context, tx store.Session, event *Event) error {
	columns := []string{coreEventCols}
	values := []any{
		event.ID.String(),
		event.AggregateID,
		event.AggregateIDHash,
		event.AggregateVersion,
		event.AggregateKind,
		event.Kind,
		event.Body,
		event.CreatedAt,
		event.Migration,
		event.Migrated,
	}
	vars := []string{coreEventVars}
	count := coreEventVarCount
	for k, v := range event.Discriminator {
		columns = append(columns, store.DiscriminatorColumnPrefix+k)
		count++
		vars = append(vars, "$"+strconv.Itoa(count))
		values = append(values, v)
	}

	query := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", r.eventsTable, strings.Join(columns, ", "), strings.Join(vars, ", "))

	_, err := tx.ExecContext(ctx, query, values...)
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
		err := handler(store.NewInTxHandlerContext[K](ctx, e))
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
	if r.snapshotsTable == "" {
		return eventsourcing.Snapshot[K]{}, nil
	}

	query := strings.Builder{}
	query.WriteString(fmt.Sprintf("SELECT * FROM %s WHERE aggregate_id = $1", r.snapshotsTable))
	args := []any{aggregateID.String()}

	disc := r.discriminatorMerge(ctx, store.OnRetrieve)
	for k, v := range disc {
		args = append(args, v)
		query.WriteString(fmt.Sprintf(" AND %s%s = $%d", store.DiscriminatorColumnPrefix, k, len(args)))
	}
	query.WriteString(" ORDER BY id DESC LIMIT 1")

	snaps, err := r.getSnapshots(ctx, disc, query.String(), args...)
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

func (r *EsRepository[K, PK]) getSnapshots(ctx context.Context, discriminator eventsourcing.Discriminator, query string, args ...any) ([]eventsourcing.Snapshot[K], error) {
	columns := []string{coreSnapCols}
	for k := range discriminator {
		columns = append(columns, store.DiscriminatorColumnPrefix+k)
	}
	query = strings.Replace(query, "SELECT *", fmt.Sprintf("SELECT %s", strings.Join(columns, ", ")), 1)

	rows, err := r.Session(ctx).QueryxContext(ctx, query, args...)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, faults.Errorf("querying snapshots, query=%s, args=%+v: %w", query, args, err)
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
		disc := []*store.Discriminator{}
		for k := range discriminator {
			m := &store.Discriminator{Key: k}
			disc = append(disc, m)
			dest = append(dest, &m.Value)
		}

		err := rows.Scan(dest...)
		if err != nil {
			return nil, faults.Wrap(err)
		}

		m := eventsourcing.Discriminator{}
		for _, v := range disc {
			m[v.Key] = v.Key
		}

		snap.Discriminator = m

		snaps = append(snaps, snap)
	}

	return snaps, nil
}

func (r *EsRepository[K, PK]) IsSnapshotEnabled() bool {
	return r.snapshotsTable != ""
}

func (r *EsRepository[K, PK]) SaveSnapshot(ctx context.Context, snapshot *eventsourcing.Snapshot[K]) error {
	disc := r.discriminatorMerge(ctx, store.OnPersist)
	return r.saveSnapshot(ctx, r.Session(ctx), &Snapshot{
		ID:               snapshot.ID,
		AggregateID:      snapshot.AggregateID.String(),
		AggregateVersion: snapshot.AggregateVersion,
		AggregateKind:    snapshot.AggregateKind,
		Body:             snapshot.Body,
		CreatedAt:        snapshot.CreatedAt.UTC(),
		Discriminator:    disc,
	})
}

func (r *EsRepository[K, PK]) saveSnapshot(ctx context.Context, x store.Session, s *Snapshot) error {
	if r.snapshotsTable == "" {
		return faults.New("snapshot table is undefined")
	}

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
	count := coreSnapVarCount
	for k, v := range s.Discriminator {
		columns = append(columns, store.DiscriminatorColumnPrefix+k)
		count++
		vars = append(vars, "$"+strconv.Itoa(count))
		values = append(values, v)
	}

	query := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", r.snapshotsTable, strings.Join(columns, ", "), strings.Join(vars, ", "))

	// TODO instead of adding we could replace UPDATE/INSERT
	_, err := x.ExecContext(ctx, query, values...)

	return faults.Wrapf(err, "saving snapshot, query:%s, args: %+v", query, values)
}

func (r *EsRepository[K, PK]) GetAggregateEvents(ctx context.Context, aggregateID K, snapVersion int) ([]*eventsourcing.Event[K], error) {
	var query strings.Builder
	query.WriteString(fmt.Sprintf("SELECT * FROM %s e WHERE e.aggregate_id = $1 AND migration = 0", r.eventsTable))
	args := []any{aggregateID.String()}
	if snapVersion > -1 {
		query.WriteString(" AND e.aggregate_version > $2")
		args = append(args, snapVersion)
	}
	disc := r.discriminatorMerge(ctx, store.OnRetrieve)
	for k, v := range disc {
		args = append(args, v)
		query.WriteString(fmt.Sprintf(" AND %s%s = $%d", store.DiscriminatorColumnPrefix, k, len(args)))
	}
	query.WriteString(" ORDER BY aggregate_version ASC")

	events, err := r.queryEvents(ctx, disc, query.String(), args...)
	if err != nil {
		return nil, faults.Errorf("Unable to get events for Aggregate '%s': %w", aggregateID, err)
	}

	return events, nil
}

func (r *EsRepository[K, PK]) Forget(ctx context.Context, request eventsourcing.ForgetRequest[K], forget func(kind eventsourcing.Kind, body []byte) ([]byte, error)) error {
	// When Forget() is called, the aggregate is no longer used, therefore if it fails, it can be called again.

	query := strings.Builder{}
	query.WriteString(fmt.Sprintf("SELECT * FROM %s WHERE aggregate_id = $1 AND kind = $2", r.eventsTable))
	args := []any{request.AggregateID.String(), request.EventKind}
	// Forget events
	events, err := r.queryEvents(ctx, nil, query.String(), args...)
	if err != nil {
		return faults.Errorf("Unable to get events for Aggregate '%s' and event kind '%s': %w", request.AggregateID, request.EventKind, err)
	}

	qry := fmt.Sprintf("UPDATE %s SET body = $1 WHERE ID = $2", r.eventsTable)
	for _, evt := range events {
		body, err := forget(evt.Kind, evt.Body)
		if err != nil {
			return err
		}

		_, err = r.Session(ctx).ExecContext(ctx, qry, body, evt.ID.String())
		if err != nil {
			return faults.Errorf("Unable to forget event ID %s: %w", evt.ID, err)
		}
	}

	// forget snapshots
	if r.snapshotsTable == "" {
		return nil
	}

	qry = fmt.Sprintf("SELECT * FROM %s WHERE aggregate_id = $1", r.snapshotsTable)
	snaps, err := r.getSnapshots(ctx, nil, qry, request.AggregateID.String())
	if err != nil {
		return faults.Errorf("getting snapshot for aggregate '%s': %w", request.AggregateID, err)
	}

	qry = fmt.Sprintf("UPDATE %s SET body = $1 WHERE ID = $2", r.snapshotsTable)
	for _, snap := range snaps {
		body, err := forget(snap.AggregateKind, snap.Body)
		if err != nil {
			return err
		}
		_, err = r.Session(ctx).ExecContext(ctx, qry, body, snap.ID.String())
		if err != nil {
			return faults.Errorf("Unable to forget snapshot ID %s: %w", snap.ID, err)
		}
	}

	return nil
}

func (r *EsRepository[K, PK]) GetEvents(ctx context.Context, after, until eventid.EventID, batchSize int, filter store.Filter) ([]*eventsourcing.Event[K], error) {
	var query strings.Builder
	query.WriteString(fmt.Sprintf("SELECT * FROM %s WHERE id > $1 AND id <= $2 AND migration = 0", r.eventsTable))
	args := []any{after.String(), until.String()}
	disc := r.discriminatorMerge(ctx, store.OnRetrieve)
	args = buildFilter(&query, " AND ", disc, filter, args)
	query.WriteString(" ORDER BY id ASC")
	if batchSize > 0 {
		query.WriteString(" LIMIT ")
		query.WriteString(strconv.Itoa(batchSize))
	}

	rows, err := r.queryEvents(ctx, disc, query.String(), args...)
	if err != nil {
		return nil, faults.Errorf("getting events between ('%d', '%s'] for filter %+v: %w", after, until, filter, err)
	}
	if len(rows) == 0 {
		return nil, nil
	}

	return rows, nil
}

func buildFilter(qry *strings.Builder, prefix string, disc eventsourcing.Discriminator, filter store.Filter, args []any) []any {
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

	if filter.Splits > 1 && len(filter.SplitIDs) != int(filter.Splits) {
		args = append(args, filter.Splits)
		pos := len(args)
		s := strings.Builder{}
		for k, v := range filter.SplitIDs {
			if k > 0 {
				s.WriteString(", ")
			}
			args = append(args, v)
			s.WriteString("$" + strconv.Itoa(len(args)))
		}
		conditions = append(conditions, fmt.Sprintf("MOD(aggregate_id_hash, $%d) IN (%s)", pos, s.String()))
	}

	for k, v := range disc {
		args = append(args, v)
		qry.WriteString(fmt.Sprintf(" AND %s%s = $%d", store.DiscriminatorColumnPrefix, k, len(args)))
	}

	if len(filter.Discriminator) > 0 {
		for _, kv := range filter.Discriminator {
			// ignore if already set by the discriminator
			if disc != nil {
				_, ok := disc[kv.Key]
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
				args = append(args, v)
				query.WriteString(fmt.Sprintf("%s%s = $%d", store.DiscriminatorColumnPrefix, kv.Key, len(args)))
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

func (r *EsRepository[K, PK]) queryEvents(ctx context.Context, discriminator eventsourcing.Discriminator, query string, args ...any) ([]*eventsourcing.Event[K], error) {
	columns := []string{coreEventCols}
	for k := range discriminator {
		columns = append(columns, store.DiscriminatorColumnPrefix+k)
	}
	query = strings.Replace(query, "SELECT *", fmt.Sprintf("SELECT %s", strings.Join(columns, ", ")), 1)

	rows, err := r.Session(ctx).QueryxContext(ctx, query, args...)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, faults.Errorf("querying events with %s, args=%+v: %w", query, args, err)
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
			&event.CreatedAt,
			&event.Migration,
			&event.Migrated,
		}

		disc := []*store.Discriminator{}
		for k := range discriminator {
			m := &store.Discriminator{Key: k}
			disc = append(disc, m)
			dest = append(dest, &m.Value)
		}

		err := rows.Scan(dest...)
		if err != nil {
			return nil, faults.Errorf("unable to scan to struct: %w", err)
		}

		m := eventsourcing.Discriminator{}
		for _, v := range disc {
			m[v.Key] = v.Value
		}

		event.Discriminator = m
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
		Discriminator:    e.Discriminator,
		CreatedAt:        e.CreatedAt,
		Migrated:         e.Migrated,
	}, nil
}

func (r *EsRepository[K, PK]) GetEventsByRawIDs(ctx context.Context, ids []string) ([]*eventsourcing.Event[K], error) {
	qry := fmt.Sprintf("SELECT * FROM %s WHERE id IN (?) ORDER BY id ASC", r.eventsTable)
	qry, args, err := sqlx.In(qry, ids) // the query must use the '?' bind var
	if err != nil {
		return nil, faults.Errorf("getting pending events (IDs=%v): %w", ids, err)
	}
	qry = r.Session(ctx).Rebind(qry) // sqlx.In returns queries with the `?` bindvar, we can rebind it for our backend

	return r.queryEvents(ctx, nil, qry, args...)
}

func (r *EsRepository[K, PK]) createSchema(dbx *sqlx.DB) error {
	sqls := []string{}
	var count int

	err := dbx.Get(&count, "SELECT count(*) FROM information_schema.tables WHERE table_name=$1", r.eventsTable)
	if err != nil {
		return faults.Wrap(err)
	}

	if count == 0 {
		sqls = append(sqls,
			fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s(
			id VARCHAR (50) PRIMARY KEY,
			aggregate_id VARCHAR (50) NOT NULL,
			aggregate_id_hash INTEGER NOT NULL,
			aggregate_version INTEGER NOT NULL,
			aggregate_kind VARCHAR (50) NOT NULL,
			kind VARCHAR (50) NOT NULL,
			body bytea,
			created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			migration INTEGER NOT NULL DEFAULT 0,
			migrated BOOLEAN NOT NULL DEFAULT false
		);`, r.eventsTable),
			fmt.Sprintf(`CREATE INDEX %[1]s_agg_id_migrated_idx ON %[1]s (aggregate_id, migration);`, r.eventsTable),
			fmt.Sprintf(`CREATE INDEX %[1]s_id_migrated_idx ON %[1]s (id, migration);`, r.eventsTable),
			fmt.Sprintf(`CREATE INDEX %[1]s_type_migrated_idx ON %[1]s (aggregate_kind, migration);`, r.eventsTable),
			fmt.Sprintf(`CREATE UNIQUE INDEX %[1]s_agg_id_ver_uk ON %[1]s (aggregate_id, aggregate_version);`, r.eventsTable),
		)

		if r.publish {
			sqls = append(sqls,
				fmt.Sprintf(`CREATE PUBLICATION %[1]s_pub FOR TABLE %[1]s WITH (publish = 'insert');`, r.eventsTable),
			)
		}
	}

	// check to see if there are new discriminator columns
	for k := range r.allowedKeys {
		count = 0
		err := dbx.Get(&count, "SELECT count(*) FROM information_schema.columns WHERE table_name=$1 AND column_name=$2", r.eventsTable, k)
		if err != nil {
			return faults.Wrap(err)
		}

		if count == 0 {
			sqls = append(sqls,
				fmt.Sprintf(`ALTER TABLE %s ADD COLUMN %s%s VARCHAR (255) NULL`, r.eventsTable, store.DiscriminatorColumnPrefix, k),
				fmt.Sprintf(`CREATE INDEX %s_%s%s_idx ON %s (%s%s)`, r.eventsTable, store.DiscriminatorColumnPrefix, k, r.eventsTable, store.DiscriminatorColumnPrefix, k),
			)
		}
	}

	if r.snapshotsTable != "" {
		count = 0
		err := dbx.Get(&count, "SELECT count(*) FROM information_schema.tables WHERE table_name=$1", r.snapshotsTable)
		if err != nil {
			return faults.Wrap(err)
		}

		if count == 0 {
			sqls = append(sqls,
				fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s(
					id VARCHAR (50) PRIMARY KEY,
					aggregate_id VARCHAR (50) NOT NULL,
					aggregate_version INTEGER NOT NULL,
					aggregate_kind VARCHAR (50) NOT NULL,
					body bytea NOT NULL,
					created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
					FOREIGN KEY (id) REFERENCES %s (id)
		);`, r.snapshotsTable, r.eventsTable),
				fmt.Sprintf(`CREATE INDEX %[1]s_agg_id_idx ON %[1]s (aggregate_id);`, r.snapshotsTable),
			)
		}

		// check to see if there are new discriminator columns
		for k := range r.allowedKeys {
			sqls = append(sqls,
				fmt.Sprintf(`ALTER TABLE %s ADD COLUMN IF NOT EXISTS %s%s VARCHAR (255) NULL`, r.snapshotsTable, store.DiscriminatorColumnPrefix, k),
			)
		}
	}

	return r.WithTx(context.Background(), func(ctx context.Context, s store.Session) error {
		for _, sql := range sqls {
			_, err := s.ExecContext(ctx, sql)
			if err != nil {
				return faults.Errorf("failed to execute %s: %w", sql, err)
			}
		}

		return nil
	})
}
