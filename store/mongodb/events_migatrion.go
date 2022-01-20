package mongodb

import (
	"context"
	"errors"
	"time"

	"github.com/quintans/faults"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/quintans/eventsourcing"
	"github.com/quintans/eventsourcing/common"
	"github.com/quintans/eventsourcing/eventid"
)

func (r *EsRepository) MigrateInPlaceCopyReplace(
	ctx context.Context,
	revision int,
	snapshotThreshold uint32,
	aggregateFactory func() (eventsourcing.Aggregater, error), // called only if snapshot threshold is reached
	rehydrateFunc func(eventsourcing.Aggregater, eventsourcing.Event) error, // called only if snapshot threshold is reached
	encoder eventsourcing.Encoder,
	handler eventsourcing.MigrationHandler,
	aggregateType eventsourcing.AggregateType,
	eventTypeCriteria ...eventsourcing.EventKind,
) error {
	if revision < 1 {
		return faults.New("revision must be greater than zero")
	}
	if snapshotThreshold > 0 && (aggregateFactory == nil || rehydrateFunc == nil || encoder == nil) {
		return faults.New("if snapshot threshold is greather than zero then aggregate factory, rehydrate function and encoder must be defined.")
	}

	// loops until it exhausts all streams with the event that we want to migrate
	for {
		events, err := r.eventsForMigration(ctx, aggregateType, eventTypeCriteria)
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
		err = r.saveMigration(ctx, last, migration, snapshotThreshold, aggregateFactory, rehydrateFunc, encoder, revision)
		if !errors.Is(err, eventsourcing.ErrConcurrentModification) {
			return err
		}
	}
}

func (r *EsRepository) eventsForMigration(ctx context.Context, aggregateType eventsourcing.AggregateType, eventTypeCriteria []eventsourcing.EventKind) ([]*eventsourcing.Event, error) {
	if aggregateType == "" {
		return nil, faults.New("aggregate type needs to be specified")
	}
	if len(eventTypeCriteria) == 0 {
		return nil, faults.New("event type criteria needs to be specified")
	}

	// find an aggregate ID to migrate
	filter := bson.D{
		{"aggregate_type", bson.D{{"$eq", aggregateType}}},
		{"migrated", bson.D{{"$eq", 0}}},
		{"kind", bson.D{{"$in", eventTypeCriteria}}},
	}

	oneOpts := options.FindOne().
		SetSort(bson.D{{"_id", 1}}).
		SetProjection(bson.D{
			{"aggregate_id", 1},
			{"_id", 0},
		})

	event := Event{}
	if err := r.eventsCollection().FindOne(ctx, filter, oneOpts).Decode(&event); err != nil {
		if err == mongo.ErrNoDocuments {
			return nil, nil
		}
		return nil, faults.Errorf("failed to find aggregate for migration: %w", err)
	}

	// get all events for the aggregate id returned by the subquery
	filter = bson.D{
		{"aggregate_id", bson.D{{"$eq", event.AggregateID}}},
		{"migrated", bson.D{{"$eq", 0}}},
	}

	opts := options.Find().
		SetSort(bson.D{{"aggregate_version", 1}})

	events := []*Event{}
	cursor, err := r.eventsCollection().Find(ctx, filter, opts)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return nil, nil
		}
		return nil, faults.Errorf("failed to find events for migration: %w", err)
	}
	if err = cursor.All(ctx, &events); err != nil {
		return nil, faults.Wrap(err)
	}

	evts := make([]*eventsourcing.Event, len(events))
	for k, v := range events {
		id, err := eventid.Parse(v.ID)
		if err != nil {
			return nil, faults.Wrap(err)
		}
		e := toEventsourcingEvent(*v, id)
		evts[k] = &e
	}
	return evts, nil
}

func (r *EsRepository) saveMigration(
	ctx context.Context,
	last *eventsourcing.Event,
	migration []*eventsourcing.EventMigration,
	snapshotThreshold uint32,
	aggregateFactory func() (eventsourcing.Aggregater, error),
	rehydrateFunc func(eventsourcing.Aggregater, eventsourcing.Event) error,
	encoder eventsourcing.Encoder,
	revision int,
) error {
	version := last.AggregateVersion
	clock := common.NewClockAfter(last.CreatedAt)
	entropy := eventid.NewEntropy()

	return r.withTx(ctx, func(mCtx mongo.SessionContext) error {
		// invalidate event, making sure that no other event was added in the meantime
		version++
		t := clock.Now()
		id, err := entropy.NewID(t)
		if err != nil {
			return faults.Wrap(err)
		}
		err = r.saveEvent(
			mCtx,
			Event{
				ID:               id.String(),
				AggregateID:      last.AggregateID,
				AggregateIDHash:  last.AggregateIDHash,
				AggregateVersion: version,
				AggregateType:    last.AggregateType,
				Kind:             eventsourcing.InvalidatedKind,
				CreatedAt:        t,
			},
			id,
		)
		if err != nil {
			return err
		}

		// invalidate all active events
		filter := bson.D{
			{"aggregate_id", last.AggregateID},
			{"migrated", 0},
		}
		update := bson.M{
			"$set": bson.M{"migrated": revision},
		}

		_, err = r.eventsCollection().UpdateMany(mCtx, filter, update)
		if err != nil {
			return faults.Errorf("failed to invalidate events: %w", err)
		}

		// delete snapshots
		_, err = r.snapshotCollection().DeleteMany(mCtx, bson.D{
			{"aggregate_id", last.AggregateID},
		})
		if err != nil {
			return faults.Errorf("failed to delete stale snapshots: %w", err)
		}

		var aggregate eventsourcing.Aggregater
		if snapshotThreshold > 0 && len(migration) >= int(snapshotThreshold) {
			aggregate, err = aggregateFactory()
			if err != nil {
				return faults.Wrap(err)
			}
		}

		// insert new events
		var lastID eventid.EventID
		t = clock.Now()
		for _, mig := range migration {
			version++
			metadata, err := mig.Metadata.AsMap()
			if err != nil {
				return faults.Wrap(err)
			}
			lastID, err = entropy.NewID(t)
			if err != nil {
				return faults.Wrap(err)
			}
			event := Event{
				ID:               lastID.String(),
				AggregateID:      last.AggregateID,
				AggregateIDHash:  last.AggregateIDHash,
				AggregateVersion: version,
				AggregateType:    last.AggregateType,
				Kind:             mig.Kind,
				Body:             mig.Body,
				IdempotencyKey:   mig.IdempotencyKey,
				Metadata:         metadata,
				CreatedAt:        t,
			}
			err = r.saveEvent(mCtx, event, id)
			if err != nil {
				return err
			}
			if aggregate != nil {
				err = rehydrateFunc(aggregate, toEventsourcingEvent(event, lastID))
				if err != nil {
					return err
				}
			}
		}

		if aggregate != nil {
			body, err := encoder.Encode(aggregate)
			if err != nil {
				return faults.Errorf("failed to encode aggregate on migration: %w", err)
			}

			err = r.saveSnapshot(mCtx, Snapshot{
				ID:               lastID.String(),
				AggregateID:      aggregate.GetID(),
				AggregateVersion: aggregate.GetVersion(),
				AggregateType:    eventsourcing.AggregateType(aggregate.GetType()),
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
