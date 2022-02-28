package postgresql

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"strconv"

	"github.com/avast/retry-go/v3"
	"github.com/quintans/faults"

	"github.com/quintans/eventsourcing"
	"github.com/quintans/eventsourcing/eventid"
	"github.com/quintans/eventsourcing/lock"
	"github.com/quintans/eventsourcing/log"
	"github.com/quintans/eventsourcing/store"
	"github.com/quintans/eventsourcing/util"
)

const retries = 3

// ProjectionMigrater represents the structure implementation that a projection must return when asked how to rebuild itself
type ProjectionMigrater interface {
	// Name returns the name of the new projection. It is used to track if the projection was fully processed
	Name() string
	// Steps returns the order of the aggregate types to process to recreate the projection
	Steps() []ProjectionMigrationStep
	// Flush is called for each aggregate with the current state
	Flush(context.Context, store.AggregateMetadata, eventsourcing.Aggregater) error
}

type ProjectionMigrationStep struct {
	AggregateType eventsourcing.AggregateType
	// Factory creates a new aggregate instance
	Factory func() eventsourcing.Aggregater
}

type getByIDFunc func(ctx context.Context, aggregateID string) (eventsourcing.Aggregater, store.AggregateMetadata, error)

// MigrateConsistentProjection migrates a consistent projection by creating a new one
func (r *EsRepository) MigrateConsistentProjection(
	ctx context.Context,
	logger log.Logger,
	locker lock.WaitLocker,
	migrater ProjectionMigrater,
	getByID getByIDFunc,
) error {
	if err := r.createMigrationTable(ctx); err != nil {
		return err
	}

	go func() {
		err := r.migrateProjection(ctx, locker, migrater, getByID)
		if err != nil {
			logger.WithError(err).Error("Failed to catchup projection '%s'", migrater.Name())
		}
	}()

	return nil
}

func (r *EsRepository) migrateProjection(
	ctx context.Context,
	locker lock.WaitLocker,
	migrater ProjectionMigrater,
	getByID getByIDFunc,
) error {
	// check
	ok, err := r.shouldMigrate(ctx, migrater.Name())
	if err != nil {
		return err
	}
	if !ok {
		return nil
	}
	// lock
	for {
		_, err = locker.Lock(ctx)
		if errors.Is(err, lock.ErrLockAlreadyAcquired) {
			locker.WaitForUnlock(ctx)
			continue
		} else if err != nil {
			faults.Wrap(err)
		}

		defer locker.Unlock(context.Background())
		break
	}
	// recheck
	ok, err = r.shouldMigrate(ctx, migrater.Name())
	if err != nil {
		return err
	}
	if !ok {
		return nil
	}

	steps := migrater.Steps()
	for _, s := range steps {
		// retrieve events for an aggregate type, aggregate id, event id in batches
		err = r.distinctAggregates(ctx, s.AggregateType, func(c context.Context, aggregateID string) error {
			return retry.Do(
				func() error {
					return r.processAggregate(ctx, migrater, s.AggregateType, aggregateID, getByID)
				},
				retry.Attempts(retries),
				retry.RetryIf(isDup),
			)
		})
		if err != nil {
			return faults.Wrap(err)
		}
	}

	return r.doneMigration(ctx, migrater.Name())
}

func (r *EsRepository) processAggregate(
	c context.Context,
	migrater ProjectionMigrater,
	aggregateType eventsourcing.AggregateType,
	aggregateID string,
	getByID getByIDFunc,
) error {
	agg, metadata, err := getByID(c, aggregateID)
	if err != nil {
		return faults.Wrap(err)
	}

	return r.withTx(c, func(c context.Context, tx *sql.Tx) error {
		// flush the event to the handler
		err := migrater.Flush(c, metadata, agg)
		if err != nil {
			return faults.Wrap(err)
		}
		err = r.addNoOp(c, metadata)
		if err != nil {
			return faults.Wrap(err)
		}

		return nil
	})
}

func (r *EsRepository) createMigrationTable(ctx context.Context) error {
	_, err := r.db.ExecContext(ctx, "CREATE TABLE IF NOT EXISTS projection_migration (name VARCHAR (100) PRIMARY KEY)")
	if err != nil {
		return faults.Errorf("failed to create projection_migration table: %w", err)
	}

	return nil
}

func (r *EsRepository) shouldMigrate(ctx context.Context, name string) (bool, error) {
	var value int
	if err := r.db.GetContext(ctx, &value, "SELECT 1 FROM projection_migration WHERE name=$1"); err != nil {
		if err != sql.ErrNoRows {
			return false, faults.Errorf("unable to get the projection status: %w", err)
		}
	}

	return value == 1, nil
}

func (r *EsRepository) doneMigration(ctx context.Context, name string) error {
	_, err := r.db.ExecContext(ctx, `INSERT INTO projection_migration (name) VALUES ($1)`, name)
	if err != nil {
		return faults.Errorf("failed to mark projection migration '%s' as done: %w", err)
	}

	return nil
}

const distinctLimit = 100

func (r *EsRepository) distinctAggregates(
	ctx context.Context,
	aggregateType eventsourcing.AggregateType,
	handler func(c context.Context, aggregateID string) error,
) error {
	aggregateID := ""
	for {
		args := []interface{}{aggregateType}
		var query bytes.Buffer
		// get the id of the aggregate
		query.WriteString("SELECT distinct aggregate_id FROM events WHERE aggregate_type = $1")
		if aggregateID != "" {
			args = append(args, aggregateID)
			query.WriteString("aggregate_id > $" + strconv.Itoa(len(args)))
		}
		query.WriteString(" ORDER BY id ASC LIMIT " + strconv.Itoa(distinctLimit))

		aggIDs := []string{}
		err := r.db.SelectContext(ctx, &aggIDs, query.String(), args...)
		if errors.Is(err, sql.ErrNoRows) {
			return nil
		} else if err != nil {
			return faults.Errorf("unable to query aggregates IDs: %w\n%s", err, query)
		}

		for _, aggID := range aggIDs {
			err = handler(ctx, aggID)
			if err != nil {
				return faults.Wrap(err)
			}
		}

		if len(aggIDs) != distinctLimit {
			break
		}
		aggregateID = aggIDs[distinctLimit-1]
	}
	return nil
}

func (r *EsRepository) addNoOp(ctx context.Context, metadata store.AggregateMetadata) error {
	clock := util.NewClock()
	t := clock.After(metadata.UpdatedAt)
	ver := metadata.Version + 1
	aggID := metadata.ID
	hash := util.Hash(aggID)
	id, err := eventid.NewEntropy().NewID(t)
	if err != nil {
		return faults.Wrap(err)
	}
	tx := TxFromContext(ctx)
	err = r.saveEvent(ctx, tx, Event{
		ID:               id,
		AggregateID:      aggID,
		AggregateIDHash:  int32ring(hash),
		AggregateVersion: ver,
		AggregateType:    metadata.Type,
		Kind:             eventsourcing.KindNoOpEvent,
		CreatedAt:        t,
	})
	return faults.Wrap(err)
}
