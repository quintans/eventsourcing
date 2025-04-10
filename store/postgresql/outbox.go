package postgresql

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"strings"

	"github.com/jmoiron/sqlx"
	"github.com/oklog/ulid/v2"
	"github.com/quintans/eventsourcing"
	"github.com/quintans/eventsourcing/eventid"
	"github.com/quintans/eventsourcing/sink/poller"
	"github.com/quintans/eventsourcing/store"
	"github.com/quintans/faults"
)

var _ poller.Repository[*ulid.ULID] = (*OutboxRepository[*ulid.ULID])(nil)

type EventsRepository[K eventsourcing.ID] interface {
	GetEventsByRawIDs(context.Context, []string) ([]*eventsourcing.Event[K], error)
}

type OutboxRepository[K eventsourcing.ID] struct {
	store.Repository
	tableName  string
	eventsRepo EventsRepository[K]
}

func NewOutboxStore[K eventsourcing.ID](db *sql.DB, tableName string, eventsRepo EventsRepository[K]) (*OutboxRepository[K], error) {
	dbx := sqlx.NewDb(db, driverName)
	r := &OutboxRepository[K]{
		Repository: store.NewRepository(dbx),
		tableName:  tableName,
		eventsRepo: eventsRepo,
	}

	err := r.createTableIfNotExists(dbx)
	if err != nil {
		return nil, err
	}

	return r, nil
}

func (r *OutboxRepository[K]) PendingEvents(ctx context.Context, batchSize int) ([]*eventsourcing.Event[K], error) {
	var query strings.Builder
	query.WriteString(fmt.Sprintf("SELECT id FROM %s ORDER BY id ASC", r.tableName))
	if batchSize > 0 {
		query.WriteString(" LIMIT ")
		query.WriteString(strconv.Itoa(batchSize))
	}

	var ids []string
	qry := query.String()
	err := r.Session(ctx).SelectContext(ctx, &ids, qry)
	if err != nil {
		return nil, faults.Errorf("getting pending events (SQL=%s): %w", qry, err)
	}

	if len(ids) == 0 {
		return nil, nil
	}

	rows, err := r.eventsRepo.GetEventsByRawIDs(ctx, ids)
	if err != nil {
		return nil, faults.Errorf("getting pending events: %w", err)
	}
	if len(rows) == 0 {
		return nil, nil
	}

	return rows, nil
}

func (r *OutboxRepository[K]) AfterSink(ctx context.Context, eID eventid.EventID) error {
	_, err := r.Session(ctx).ExecContext(ctx, fmt.Sprintf("DELETE FROM %s WHERE id = $1", r.tableName), eID.String())
	return faults.Wrapf(err, "deleting from '%s' where id='%s'", r.tableName, eID)
}

func (r *OutboxRepository[K]) createTableIfNotExists(dbx *sqlx.DB) error {
	var count int
	err := dbx.Get(&count, "SELECT count(*) FROM information_schema.tables WHERE table_name=$1", r.tableName)
	if err != nil {
		return faults.Wrap(err)
	}

	if count > 0 {
		return nil
	}

	sqls := []string{
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s(
			id VARCHAR (50) PRIMARY KEY
		);`, r.tableName),
	}

	for _, s := range sqls {
		_, err := dbx.Exec(s)
		if err != nil {
			return faults.Errorf("failed to execute %s: %w", s, err)
		}
	}

	return nil
}

func OutboxInsertHandler[K eventsourcing.ID](tableName string) store.InTxHandler[K] {
	return func(c *store.InTxHandlerContext[K]) error {
		ctx := c.Context()
		tx := store.TxFromContext(ctx)
		if tx == nil {
			return faults.Errorf("no transaction in context")
		}

		event := c.Event()
		query := fmt.Sprintf("INSERT INTO %s (id) VALUES ($1)", tableName)

		_, err := tx.ExecContext(ctx, query, event.ID.String())
		return faults.Wrapf(err, "inserting into the outbox, query=%s, args=%+v", query, event.ID)
	}
}
