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
	store.Repository
	tableName string
}

func NewKVStoreWithURL(connString string, table string) (KVStore, error) {
	db, err := sql.Open(driverName, connString)
	if err != nil {
		return KVStore{}, faults.Wrap(err)
	}

	return NewKVStore(db, table)
}

func NewKVStore(db *sql.DB, table string) (KVStore, error) {
	dbx := sqlx.NewDb(db, driverName)

	r := KVStore{
		Repository: store.NewRepository(dbx),
		tableName:  table,
	}
	err := r.createTableIfNotExists(dbx)
	if err != nil {
		return KVStore{}, faults.Wrap(err)
	}

	return r, nil
}

func (r KVStore) Get(ctx context.Context, key string) (string, error) {
	row := kvStoreRow{}
	if err := r.Session(ctx).GetContext(ctx, &row, fmt.Sprintf("SELECT value FROM %s WHERE key = $1", r.tableName), key); err != nil {
		if err == sql.ErrNoRows {
			return "", nil
		}
		return "", faults.Errorf("getting value for key '%s': %w", key, err)
	}

	return row.Token, nil
}

func (r KVStore) Put(ctx context.Context, key string, value string) error {
	if value == "" {
		return nil
	}
	return r.WithTx(ctx, func(ctx context.Context, tx store.Session) error {
		_, err := tx.ExecContext(ctx, fmt.Sprintf("INSERT INTO %s(key, value) VALUES($1, $2) ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value", r.tableName), key, value)
		return faults.Wrapf(err, "setting value '%s' for key '%s': %w", value, key, err)
	})
}

func (r KVStore) createTableIfNotExists(dbx *sqlx.DB) error {
	sqls := []string{}
	var count int

	err := dbx.Get(&count, "SELECT count(*) FROM information_schema.tables WHERE table_name=$1", r.tableName)
	if err != nil {
		return faults.Wrap(err)
	}

	if count == 0 {
		sqls = append(sqls,
			fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s(
				key VARCHAR PRIMARY KEY,
				value VARCHAR NOT NULL
			);`, r.tableName),
		)
	}

	for _, s := range sqls {
		_, err := dbx.Exec(s)
		if err != nil {
			return faults.Errorf("failed to execute %s: %w", s, err)
		}
	}

	return nil
}
