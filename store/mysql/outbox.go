package mysql

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"strconv"

	"github.com/jmoiron/sqlx"
	"github.com/quintans/eventsourcing"
	"github.com/quintans/eventsourcing/eventid"
	"github.com/quintans/eventsourcing/sink/poller"
	"github.com/quintans/eventsourcing/store"
	"github.com/quintans/faults"
)

var _ poller.Repository = (*OutboxRepository)(nil)

type EventsRepository interface {
	GetEventsByIDs(context.Context, []string) ([]*eventsourcing.Event, error)
}

type OutboxRepository struct {
	Repository
	tableName  string
	eventsRepo EventsRepository
}

func NewOutboxStore(db *sql.DB, tableName string, eventsRepo EventsRepository) *OutboxRepository {
	dbx := sqlx.NewDb(db, driverName)
	r := &OutboxRepository{
		Repository: Repository{
			db: dbx,
		},
		tableName:  tableName,
		eventsRepo: eventsRepo,
	}

	return r
}

func (r *OutboxRepository) PendingEvents(ctx context.Context, batchSize int, filter store.Filter) ([]*eventsourcing.Event, error) {
	var query bytes.Buffer
	query.WriteString(fmt.Sprintf("SELECT id FROM %s", r.tableName))
	args := buildFilter(&query, " WHERE ", filter, []interface{}{})
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

	rows, err := r.eventsRepo.GetEventsByIDs(ctx, ids)
	if err != nil {
		return nil, faults.Errorf("getting pending events: %w", err)
	}
	if len(rows) == 0 {
		return nil, nil
	}

	return rows, nil
}

func (r *OutboxRepository) AfterSink(ctx context.Context, eID eventid.EventID) error {
	_, err := r.db.ExecContext(ctx, fmt.Sprintf("DELETE FROM %s WHERE id = ?", r.tableName), eID.String())
	return faults.Wrapf(err, "deleting from '%s' where id='%s'", r.tableName, eID)
}

func OutboxInsertHandler(tableName string) store.InTxHandler {
	return func(ctx context.Context, event *eventsourcing.Event) error {
		tx := TxFromContext(ctx)
		if tx == nil {
			return faults.Errorf("no transaction in context")
		}
		_, err := tx.ExecContext(ctx,
			fmt.Sprintf(`INSERT INTO %s (id, aggregate_id, aggregate_kind, kind, metadata, aggregate_id_hash)
		VALUES (?, ?, ?, ?, ?, ?)`, tableName),
			event.ID.String(), event.AggregateID, event.AggregateKind, event.Kind, event.Metadata, event.AggregateIDHash)
		return faults.Wrap(err)
	}
}