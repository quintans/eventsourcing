package postgresql

import (
	"context"
	"database/sql"
	"errors"
	"log/slog"
	"strconv"
	"strings"
	"time"

	"github.com/avast/retry-go/v3"
	"github.com/quintans/faults"

	"github.com/quintans/eventsourcing"
	"github.com/quintans/eventsourcing/dist"
	"github.com/quintans/eventsourcing/eventid"
	"github.com/quintans/eventsourcing/log"
	"github.com/quintans/eventsourcing/store"
	"github.com/quintans/eventsourcing/util"
)

const retries = 3

// ProjectionMigrater represents the structure implementation that a projection must return when asked how to rebuild itself
type ProjectionMigrater[K eventsourcing.ID] interface {
	// Name returns the name of the new projection. It is used to track if the projection was fully processed
	Name() string
	// Steps returns the order of the aggregate types to process to recreate the projection
	Steps() []ProjectionMigrationStep[K]
	// Flush is called for each aggregate with the current state
	Flush(context.Context, store.AggregateMetadata[K], eventsourcing.Aggregater[K]) error
}

type ProjectionMigrationStep[K eventsourcing.ID] struct {
	AggregateKind eventsourcing.Kind
	// Factory creates a new aggregate instance
	Factory func() eventsourcing.Aggregater[K]
}

type getByIDFunc[K eventsourcing.ID] func(ctx context.Context, aggregateID string) (eventsourcing.Aggregater[K], store.AggregateMetadata[K], error)

// MigrateConsistentProjection migrates a consistent projection by creating a new one
func (r *EsRepository[K, PK]) MigrateConsistentProjection(
	ctx context.Context,
	logger *slog.Logger,
	locker dist.WaitLocker,
	migrater ProjectionMigrater[K],
	getByID getByIDFunc[K],
) error {
	if err := r.createMigrationTable(ctx); err != nil {
		return err
	}

	go func() {
		err := r.migrateProjection(ctx, logger, locker, migrater, getByID)
		if err != nil {
			logger.Error("Failed to catchup projection", "projection", migrater.Name(), log.Err(err))
		}
	}()

	return nil
}

func (r *EsRepository[K, PK]) migrateProjection(
	ctx context.Context,
	logger *slog.Logger,
	locker dist.WaitLocker,
	migrater ProjectionMigrater[K],
	getByID getByIDFunc[K],
) error {
	// check
	ok, err := r.shouldMigrate(ctx)
	if err != nil {
		return err
	}
	if !ok {
		return nil
	}
	// lock
	for {
		_, err = locker.Lock(ctx)
		if errors.Is(err, dist.ErrLockAlreadyAcquired) {
			er := locker.WaitForUnlock(ctx)
			if er != nil {
				logger.Error("waiting for unlock", log.Err(er))
			}
			continue
		} else if err != nil {
			return faults.Wrap(err)
		}

		defer locker.Unlock(context.Background())
		break
	}
	// recheck
	ok, err = r.shouldMigrate(ctx)
	if err != nil {
		return err
	}
	if !ok {
		return nil
	}

	steps := migrater.Steps()
	for _, s := range steps {
		// retrieve events for an aggregate type, aggregate id, event id in batches
		err = r.distinctAggregates(ctx, s.AggregateKind, func(c context.Context, aggregateID string) error {
			return retry.Do(
				func() error {
					return r.processAggregate(ctx, migrater, aggregateID, getByID)
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

func (r *EsRepository[K, PK]) processAggregate(
	c context.Context,
	migrater ProjectionMigrater[K],
	aggregateID string,
	getByID getByIDFunc[K],
) error {
	agg, metadata, err := getByID(c, aggregateID)
	if err != nil {
		return faults.Wrap(err)
	}

	return r.WithTx(c, func(c context.Context, tx store.Session) error {
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

func (r *EsRepository[K, PK]) createMigrationTable(ctx context.Context) error {
	_, err := r.Session(ctx).ExecContext(ctx, "CREATE TABLE IF NOT EXISTS projection_migration (name VARCHAR (100) PRIMARY KEY)")
	if err != nil {
		return faults.Errorf("failed to create projection_migration table: %w", err)
	}

	return nil
}

func (r *EsRepository[K, PK]) shouldMigrate(ctx context.Context) (bool, error) {
	var value int
	if err := r.Session(ctx).GetContext(ctx, &value, "SELECT 1 FROM projection_migration WHERE name=$1"); err != nil {
		if err != sql.ErrNoRows {
			return false, faults.Errorf("unable to get the projection status: %w", err)
		}
	}

	return value == 1, nil
}

func (r *EsRepository[K, PK]) doneMigration(ctx context.Context, name string) error {
	_, err := r.Session(ctx).ExecContext(ctx, `INSERT INTO projection_migration (name) VALUES ($1)`, name)
	if err != nil {
		return faults.Errorf("failed to mark projection migration '%s' as done: %w", name, err)
	}

	return nil
}

const distinctLimit = 100

func (r *EsRepository[K, PK]) distinctAggregates(
	ctx context.Context,
	aggregateKind eventsourcing.Kind,
	handler func(c context.Context, aggregateID string) error,
) error {
	aggregateID := ""
	for {
		args := []interface{}{aggregateKind}
		var query strings.Builder
		// get the id of the aggregate
		query.WriteString("SELECT distinct aggregate_id FROM events WHERE aggregate_kind = $1")
		if aggregateID != "" {
			args = append(args, aggregateID)
			query.WriteString("aggregate_id > $" + strconv.Itoa(len(args)))
		}
		query.WriteString(" ORDER BY id ASC LIMIT " + strconv.Itoa(distinctLimit))

		aggIDs := []string{}
		err := r.Session(ctx).SelectContext(ctx, &aggIDs, query.String(), args...)
		if errors.Is(err, sql.ErrNoRows) {
			return nil
		} else if err != nil {
			return faults.Errorf("unable to query aggregates IDs: %w\n%s", err, query.String())
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

func (r *EsRepository[K, PK]) addNoOp(ctx context.Context, metadata store.AggregateMetadata[K]) error {
	ver := metadata.Version + 1
	aggID := metadata.ID.String()
	id := eventid.NewAfterTime(metadata.UpdatedAt)
	tx := store.TxFromContext(ctx)
	err := r.saveEvent(ctx, tx, &Event{
		ID:               id,
		AggregateID:      aggID,
		AggregateIDHash:  util.HashToInt(aggID),
		AggregateVersion: ver,
		AggregateKind:    metadata.Type,
		Kind:             eventsourcing.KindNoOpEvent,
		CreatedAt:        time.Now(),
	})
	return faults.Wrap(err)
}
