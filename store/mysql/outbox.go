package mysql

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

const (
	coreOutboxCols = "id, aggregate_id, aggregate_kind, kind, aggregate_id_hash"
	coreOutboxVars = "?, ?, ?, ?, ?"
)

var _ poller.Repository[*ulid.ULID] = (*OutboxRepository[*ulid.ULID])(nil)

type EventsRepository[K eventsourcing.ID] interface {
	GetEventsByRawIDs(context.Context, []string) ([]*eventsourcing.Event[K], error)
}

type OutboxRepository[K eventsourcing.ID] struct {
	Repository
	tableName  string
	eventsRepo EventsRepository[K]
}

func NewOutboxStore[K eventsourcing.ID](db *sql.DB, tableName string, eventsRepo EventsRepository[K]) *OutboxRepository[K] {
	dbx := sqlx.NewDb(db, driverName)
	r := &OutboxRepository[K]{
		Repository: Repository{
			db: dbx,
		},
		tableName:  tableName,
		eventsRepo: eventsRepo,
	}

	return r
}

func (r *OutboxRepository[K]) PendingEvents(ctx context.Context, batchSize int, filter store.Filter) ([]*eventsourcing.Event[K], error) {
	var query strings.Builder
	query.WriteString(fmt.Sprintf("SELECT id FROM %s", r.tableName))
	args := buildFilter(&query, " WHERE ", nil, filter, []interface{}{})
	query.WriteString(" ORDER BY id ASC")
	if batchSize > 0 {
		query.WriteString(" LIMIT ")
		query.WriteString(strconv.Itoa(batchSize))
	}

	var ids []string
	err := r.db.Select(&ids, query.String(), args...)
	if err != nil {
		return nil, faults.Errorf("getting pending events: query=%s, args=%v:  %w", query.String(), args, err)
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
	_, err := r.db.ExecContext(ctx, fmt.Sprintf("DELETE FROM %s WHERE id = ?", r.tableName), eID.String())
	return faults.Wrapf(err, "deleting from '%s' where id='%s'", r.tableName, eID)
}

func OutboxInsertHandler[K eventsourcing.ID](tableName string) store.InTxHandler[K] {
	return func(c *store.InTxHandlerContext[K]) error {
		ctx := c.Context()
		tx := TxFromContext(ctx)
		if tx == nil {
			return faults.Errorf("no transaction in context")
		}

		event := c.Event()
		values := []any{
			event.ID.String(),
			event.AggregateID.String(),
			event.AggregateKind,
			event.Kind,
			event.AggregateIDHash,
		}
		columns := []string{coreOutboxCols}
		vars := []string{coreOutboxVars}
		for k, v := range event.Metadata {
			columns = append(columns, store.MetaColumnPrefix+k)
			vars = append(vars, "?")
			values = append(values, v)
		}

		query := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", tableName, strings.Join(columns, ", "), strings.Join(vars, ", "))

		_, err := tx.ExecContext(ctx, query, values...)
		return faults.Wrapf(err, "inserting into the outbox, query=%s, args=%+v", query, values)
	}
}
