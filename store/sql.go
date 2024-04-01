package store

import (
	"context"
	"database/sql"

	"github.com/jmoiron/sqlx"
	"github.com/quintans/faults"
)

type Tx func(ctx context.Context, fn func(context.Context) error) error

type Session interface {
	Rebind(query string) string
	GetContext(ctx context.Context, dest interface{}, query string, args ...interface{}) error
	SelectContext(ctx context.Context, dest interface{}, query string, args ...interface{}) error
	QueryxContext(ctx context.Context, query string, args ...any) (*sqlx.Rows, error)
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}

type Repository struct {
	db *sqlx.DB
}

func NewRepository(db *sqlx.DB) Repository {
	return Repository{db}
}

func TxRunner(db *sqlx.DB) Tx {
	return Repository{db}.TxRunner()
}

func (r Repository) TxRunner() Tx {
	return func(ctx context.Context, fn func(context.Context) error) error {
		return r.WithTx(ctx, func(c context.Context, _ Session) error {
			return fn(c)
		})
	}
}

func (r *Repository) WithTx(ctx context.Context, fn func(context.Context, Session) error) error {
	tx := TxFromContext(ctx)
	if tx != nil {
		return fn(ctx, tx)
	}

	return r.wrapWithTx(ctx, fn)
}

func (r *Repository) wrapWithTx(ctx context.Context, fn func(context.Context, Session) error) error {
	tx, err := r.db.BeginTxx(ctx, nil)
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

// Session will check if there is a transaction in context and use it, otherwise it will use the current pool
func (r *Repository) Session(ctx context.Context) Session {
	tx := TxFromContext(ctx)
	if tx != nil {
		return tx
	}

	return r.db
}

type txKey struct{}

func TxFromContext(ctx context.Context) Session {
	tx, _ := ctx.Value(txKey{}).(Session) // with _ it will not panic if nil
	return tx
}
