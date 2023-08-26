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
	"github.com/quintans/faults"
)

type OutboxRepository struct {
	Repository
	tableName string
}

func NewOutboxStore(db *sql.DB, tableName string) *OutboxRepository {
	dbx := sqlx.NewDb(db, driverName)
	r := &OutboxRepository{
		Repository: Repository{
			db: dbx,
		},
		tableName: tableName,
	}

	return r
}

func (r *OutboxRepository) GetPendingEvents(ctx context.Context, batchSize int) ([]*eventsourcing.Event, error) {
	var query bytes.Buffer
	query.WriteString(fmt.Sprintf("SELECT * FROM %s WHERE done = false", r.tableName))
	query.WriteString(" ORDER BY id ASC")
	if batchSize > 0 {
		query.WriteString(" LIMIT ")
		query.WriteString(strconv.Itoa(batchSize))
	}

	var ids []string
	err := r.db.Select(&ids, query.String())
	if err != nil {
		return nil, faults.Errorf("getting pending events: %w", err)
	}

	rows, err := r.queryEvents(ctx, ids)
	if err != nil {
		return nil, faults.Errorf("getting pending events: %w", err)
	}
	if len(rows) == 0 {
		return nil, nil
	}

	return rows, nil
}

func (r *OutboxRepository) queryEvents(ctx context.Context, ids []string) ([]*eventsourcing.Event, error) {
	qry, args, err := sqlx.In("SELECT * FROM events WHERE id IN (?) ORDER BY id ASC", ids) // the query must use the '?' bind var
	if err != nil {
		return nil, faults.Errorf("getting pending events (IDs=%+v): %w", ids, err)
	}
	qry = r.db.Rebind(qry) // sqlx.In returns queries with the `?` bindvar, we can rebind it for our backend

	return queryEvents(ctx, r.db, qry, args...)
}

func (r *OutboxRepository) SetSinkData(ctx context.Context, eID eventid.EventID) error {
	_, err := r.db.ExecContext(ctx, fmt.Sprintf("UPDATE %s SET done = true WHERE ID = ?", r.tableName), eID.String())
	return faults.Wrapf(err, "setting done=true for event id '%s'", eID)
}

func OutboxInsertHandler(tableName string) func(context.Context, ...*eventsourcing.Event) error {
	return func(ctx context.Context, events ...*eventsourcing.Event) error {
		tx := TxFromContext(ctx)
		if tx == nil {
			return faults.Errorf("no transaction in context")
		}
		for _, event := range events {
			_, err := tx.ExecContext(ctx, fmt.Sprintf("INSERT INTO %s (id, done) VALUES (?, false)", tableName), event.ID.String())
			if err != nil {
				return faults.Wrap(err)
			}
		}
		return nil
	}
}
