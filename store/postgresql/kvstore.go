package postgresql

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/jmoiron/sqlx"
	"github.com/quintans/faults"

	"github.com/quintans/eventsourcing/store"
)

var _ store.KVStore = (*KVStore)(nil)

type kvStoreRow struct {
	Token string `db:"value,omitempty"`
}

type KVStore struct {
	Repository
	table string
}

func NewKVStoreWithURL(connString string, table string) (KVStore, error) {
	db, err := sql.Open(driverName, connString)
	if err != nil {
		return KVStore{}, faults.Wrap(err)
	}

	return NewKVStore(db, table), nil
}

func NewKVStore(db *sql.DB, table string) KVStore {
	dbx := sqlx.NewDb(db, driverName)
	return KVStore{
		Repository: Repository{
			db: dbx,
		},
		table: table,
	}
}

func (r KVStore) Get(ctx context.Context, key string) (string, error) {
	row := kvStoreRow{}
	if err := r.db.GetContext(ctx, &row, fmt.Sprintf("SELECT value FROM %s WHERE key = $1", r.table), key); err != nil {
		if err == sql.ErrNoRows {
			return "", nil
		}
		return "", faults.Errorf("getting value for key '%s': %w", key, err)
	}

	return row.Token, nil
}

func (m KVStore) Put(ctx context.Context, key string, value string) error {
	if value == "" {
		return nil
	}
	return m.WithTx(ctx, func(ctx context.Context, tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, fmt.Sprintf("INSERT INTO %s(key, value) VALUES($1, $2) ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value", m.table), key, value)
		return faults.Wrapf(err, "setting value '%s' for key '%s': %w", value, key, err)
	})
}
