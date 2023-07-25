package mysql

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/quintans/faults"

	"github.com/quintans/eventsourcing"
	"github.com/quintans/eventsourcing/eventid"
	"github.com/quintans/eventsourcing/util"
)

func (r *EsRepository) MigrateInPlaceCopyReplace(
	ctx context.Context,
	revision int,
	snapshotThreshold uint32,
	rehydrateFunc func(eventsourcing.Aggregater, *eventsourcing.Event) error, // called only if snapshot threshold is reached
	codec eventsourcing.Codec,
	handler eventsourcing.MigrationHandler,
	targetAggregateKind eventsourcing.Kind,
	aggregateKind eventsourcing.Kind,
	eventTypeCriteria ...eventsourcing.Kind,
) error {
	if revision < 1 {
		return faults.New("revision must be greater than zero")
	}
	if snapshotThreshold > 0 && (rehydrateFunc == nil || codec == nil) {
		return faults.New("if snapshot threshold is greather than zero then aggregate factory, rehydrate function and codec must be defined.")
	}

	// loops until it exhausts all streams with the event that we want to migrate
	for {
		// the event to migrate will be replaced by a new one and in this way the migrated aggregate will not be selected in the next loop
		events, err := r.eventsForMigration(ctx, aggregateKind, eventTypeCriteria)
		if err != nil {
			return err
		}
		// no more streams
		if len(events) == 0 {
			return nil
		}

		migration, err := handler(events)
		if err != nil {
			return err
		}

		last := events[len(events)-1]
		err = r.saveMigration(ctx, targetAggregateKind, last, migration, snapshotThreshold, rehydrateFunc, codec, revision)
		if !errors.Is(err, eventsourcing.ErrConcurrentModification) {
			return err
		}
	}
}

func (r *EsRepository) eventsForMigration(ctx context.Context, aggregateKind eventsourcing.Kind, eventTypeCriteria []eventsourcing.Kind) ([]*eventsourcing.Event, error) {
	if aggregateKind == "" {
		return nil, faults.New("aggregate type needs to be specified")
	}
	if len(eventTypeCriteria) == 0 {
		return nil, faults.New("event type criteria needs to be specified")
	}

	args := []interface{}{aggregateKind}
	var subquery bytes.Buffer
	// get the id of the aggregate
	subquery.WriteString("SELECT aggregate_id FROM events WHERE aggregate_kind = ? AND migration = 0 AND (")
	for k, v := range eventTypeCriteria {
		if k > 0 {
			subquery.WriteString(" OR ")
		}
		args = append(args, v)
		subquery.WriteString("kind = ?")
	}
	subquery.WriteString(") ORDER BY id ASC LIMIT 1")

	// get all events for the aggregate id returned by the subquery
	events := []*Event{}
	query := fmt.Sprintf("SELECT * FROM events WHERE aggregate_id = (%s) AND migration = 0 ORDER BY aggregate_version ASC", subquery.String())
	err := r.db.SelectContext(ctx, &events, query, args...)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, faults.Errorf("unable to query events: %w\n%s", err, query)
	}

	evts := make([]*eventsourcing.Event, len(events))
	for k, v := range events {
		evts[k] = toEventsourcingEvent(v)
	}
	return evts, nil
}

func (r *EsRepository) saveMigration(
	ctx context.Context,
	targetAggregateKind eventsourcing.Kind,
	last *eventsourcing.Event,
	migration []*eventsourcing.EventMigration,
	snapshotThreshold uint32,
	rehydrateFunc func(eventsourcing.Aggregater, *eventsourcing.Event) error,
	codec eventsourcing.Codec,
	revision int,
) error {
	version := last.AggregateVersion
	clock := util.NewClockAfter(last.CreatedAt)
	entropy := eventid.NewEntropy()

	return r.withTx(ctx, func(c context.Context, tx *sql.Tx) error {
		// invalidate event, making sure that no other event was added in the meantime
		version++
		t := clock.Now()
		id, err := entropy.NewID(t)
		if err != nil {
			return faults.Wrap(err)
		}
		err = r.saveEvent(c, tx, &Event{
			ID:               id,
			AggregateID:      last.AggregateID,
			AggregateIDHash:  util.Int32ring(last.AggregateIDHash),
			AggregateVersion: version,
			AggregateKind:    last.AggregateKind,
			Kind:             eventsourcing.InvalidatedKind,
			CreatedAt:        time.Now().UTC(),
		})
		if err != nil {
			return err
		}

		// invalidate all active events
		_, err = tx.ExecContext(c, "UPDATE events SET migration = ? WHERE aggregate_id = ? AND migration = 0", revision, last.AggregateID)
		if err != nil {
			return faults.Errorf("failed to invalidate events: %w", err)
		}

		// delete snapshots
		_, err = tx.ExecContext(c, "DELETE FROM snapshots WHERE aggregate_id = ?", last.AggregateID)
		if err != nil {
			return faults.Errorf("failed to delete stale snapshots: %w", err)
		}

		var aggregate eventsourcing.Aggregater
		if snapshotThreshold > 0 && len(migration) >= int(snapshotThreshold) {
			t, er := codec.Decode(nil, targetAggregateKind)
			if er != nil {
				return faults.Wrap(er)
			}
			aggregate = t.(eventsourcing.Aggregater)
		}

		// insert new events
		var lastID eventid.EventID
		t = clock.Now()
		for _, mig := range migration {
			version++
			lastID, err = entropy.NewID(t)
			if err != nil {
				return faults.Wrap(err)
			}
			event := &Event{
				ID:               lastID,
				AggregateID:      last.AggregateID,
				AggregateIDHash:  util.Int32ring(last.AggregateIDHash),
				AggregateVersion: version,
				AggregateKind:    last.AggregateKind,
				Kind:             mig.Kind,
				Body:             mig.Body,
				IdempotencyKey:   NilString(mig.IdempotencyKey),
				Metadata:         mig.Metadata,
				CreatedAt:        t,
				Migrated:         true,
			}
			err = r.saveEvent(c, tx, event)
			if err != nil {
				return err
			}
			if aggregate != nil {
				event.ID = lastID
				err = rehydrateFunc(aggregate, toEventsourcingEvent(event))
				if err != nil {
					return err
				}
			}
		}

		if aggregate != nil {
			body, err := codec.Encode(aggregate)
			if err != nil {
				return faults.Errorf("failed to encode aggregate on migration: %w", err)
			}

			err = saveSnapshot(c, tx, &Snapshot{
				ID:               lastID,
				AggregateID:      last.AggregateID,
				AggregateVersion: version,
				AggregateKind:    aggregate.GetKind(),
				Body:             body,
				CreatedAt:        time.Now().UTC(),
			})
			if err != nil {
				return err
			}
		}

		return nil
	})
}
