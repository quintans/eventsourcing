package mysql

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
	if err := r.Session(ctx).GetContext(ctx, &row, fmt.Sprintf("SELECT value FROM %s WHERE id = ?", r.tableName), key); err != nil {
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
	return m.WithTx(ctx, func(ctx context.Context, tx store.Session) error {
		_, err := tx.ExecContext(ctx, fmt.Sprintf("INSERT INTO %s(id, value) VALUES(?, ?) ON DUPLICATE KEY UPDATE value = ?", m.tableName), key, value, value)
		return faults.Wrapf(err, "setting value '%s' for key '%s': %w", value, key, err)
	})
}

func (r KVStore) createTableIfNotExists(dbx *sqlx.DB) error {
	sqls := []string{}
	var count int

	// check to see if a table exists in mysql

	err := dbx.Get(&count, "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name=?", r.tableName)
	if err != nil {
		return faults.Wrap(err)
	}

	if count == 0 {
		sqls = append(sqls,
			fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s(
			id VARCHAR(50) PRIMARY KEY,
			value VARCHAR(256) NOT NULL
		)ENGINE=innodb`, r.tableName),
		)
	}

	for _, s := range sqls {
		_, err := dbx.Exec(s)
		if err != nil {
			return faults.Errorf("failed to execute (SQL=%s): %w", s, err)
		}
	}

	return nil
}
