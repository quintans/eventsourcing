package mysql

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/jmoiron/sqlx"
	"github.com/quintans/faults"

	"github.com/quintans/eventsourcing/projection"
)

var _ projection.ResumeStore = (*ProjectionResume)(nil)

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

func (r ProjectionResume) GetStreamResumeToken(ctx context.Context, key projection.ResumeKey) (projection.Token, error) {
	row := projectionResumeRow{}
	if err := r.db.GetContext(ctx, &row, fmt.Sprintf("SELECT id, token FROM %s WHERE id = ?", r.table), key.String()); err != nil {
		if err == sql.ErrNoRows {
			return projection.Token{}, nil
		}
		return projection.Token{}, faults.Errorf("getting resume token for '%s': %w", key, err)
	}

	return projection.ParseToken(row.Token)
}

func (m ProjectionResume) SetStreamResumeToken(ctx context.Context, key projection.ResumeKey, token projection.Token) error {
	return m.WithTx(ctx, func(ctx context.Context, tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, fmt.Sprintf("UPDATE %s SET token = ? WHERE id = ?", m.table), token.String(), key.String())
		return faults.Wrapf(err, "setting resume token '%s' for key '%s': %w", token, key, err)
	})
}
