package mysql

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/jmoiron/sqlx"
	"github.com/quintans/faults"

	"github.com/quintans/eventsourcing/store"
)

var _ store.KVStore = (*ProjectionResume)(nil)

type projectionResumeRow struct {
	ID    string `db:"id,omitempty"`
	Token string `db:"token,omitempty"`
}

type ProjectionResume struct {
	Repository
	table string
}

func NewProjectionResume(db *sql.DB, table string) ProjectionResume {
	dbx := sqlx.NewDb(db, driverName)
	return ProjectionResume{
		Repository: Repository{
			db: dbx,
		},
		table: table,
	}
}

func (r ProjectionResume) Get(ctx context.Context, key string) (string, error) {
	row := projectionResumeRow{}
	if err := r.db.GetContext(ctx, &row, fmt.Sprintf("SELECT id, token FROM %s WHERE id = ?", r.table), key); err != nil {
		if err == sql.ErrNoRows {
			return "", nil
		}
		return "", faults.Errorf("getting resume token for '%s': %w", key, err)
	}

	return row.Token, nil
}

func (m ProjectionResume) Put(ctx context.Context, key string, token string) error {
	return m.WithTx(ctx, func(ctx context.Context, tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, fmt.Sprintf("UPDATE %s SET token = ? WHERE id = ?", m.table), token, key)
		return faults.Wrapf(err, "setting resume token '%s' for key '%s': %w", token, key, err)
	})
}
