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
	"github.com/quintans/eventsourcing/eventid"
)

func (r *EsRepository[K, PK]) MigrateInPlaceCopyReplace(
	ctx context.Context,
	revision int,
	snapshotThreshold uint32,
	rehydrateFunc func(eventsourcing.Aggregater[K], *eventsourcing.Event[K]) error, // called only if snapshot threshold is reached
	codec eventsourcing.Codec[K],
	handler eventsourcing.MigrationHandler[K],
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

func (r *EsRepository[K, PK]) eventsForMigration(ctx context.Context, aggregateKind eventsourcing.Kind, eventTypeCriteria []eventsourcing.Kind) ([]*eventsourcing.Event[K], error) {
	if aggregateKind == "" {
		return nil, faults.New("aggregate type needs to be specified")
	}
	if len(eventTypeCriteria) == 0 {
		return nil, faults.New("event type criteria needs to be specified")
	}

	// find an aggregate ID to migrate
	filter := bson.D{
		{"aggregate_kind", bson.D{{"$eq", aggregateKind}}},
		{"migration", bson.D{{"$eq", 0}}},
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
		{"migration", bson.D{{"$eq", 0}}},
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

	evts := make([]*eventsourcing.Event[K], len(events))
	for k, v := range events {
		id, err := eventid.Parse(v.ID)
		if err != nil {
			return nil, faults.Wrap(err)
		}
		evt, err := toEventsourcingEvent[K, PK](v, id)
		if err != nil {
			return nil, err
		}
		evts[k] = evt
	}
	return evts, nil
}

func (r *EsRepository[K, PK]) saveMigration(
	ctx context.Context,
	targetAggregateKind eventsourcing.Kind,
	last *eventsourcing.Event[K],
	migration []*eventsourcing.EventMigration,
	snapshotThreshold uint32,
	rehydrateFunc func(eventsourcing.Aggregater[K], *eventsourcing.Event[K]) error,
	codec eventsourcing.Codec[K],
	revision int,
) error {
	version := last.AggregateVersion
	gen := eventid.NewGenerator(last.CreatedAt)

	return r.WithTx(ctx, func(ctx context.Context) error {
		// invalidate event, making sure that no other event was added in the meantime
		version++
		now := time.Now()
		id := gen.NewID()
		err := r.saveEvent(
			ctx,
			&Event{
				ID:               id.String(),
				AggregateID:      last.AggregateID.String(),
				AggregateIDHash:  int32(last.AggregateIDHash),
				AggregateVersion: version,
				AggregateKind:    last.AggregateKind,
				Kind:             eventsourcing.InvalidatedKind,
				CreatedAt:        now,
			},
			id,
		)
		if err != nil {
			return err
		}

		// invalidate all active events
		filter := bson.D{
			{"aggregate_id", last.AggregateID.String()},
			{"migration", 0},
		}
		update := bson.M{
			"$set": bson.M{"migration": revision},
		}

		_, err = r.eventsCollection().UpdateMany(ctx, filter, update)
		if err != nil {
			return faults.Errorf("failed to invalidate events: %w", err)
		}

		// delete snapshots
		_, err = r.snapshotCollection().DeleteMany(ctx, bson.D{
			{"aggregate_id", last.AggregateID.String()},
		})
		if err != nil {
			return faults.Errorf("failed to delete stale snapshots: %w", err)
		}

		var aggregate eventsourcing.Aggregater[K]
		if snapshotThreshold > 0 && len(migration) >= int(snapshotThreshold) {
			t, err := codec.Decode(nil, eventsourcing.DecoderMeta[K]{
				Kind:        targetAggregateKind,
				AggregateID: last.AggregateID,
			})
			if err != nil {
				return faults.Wrap(err)
			}
			aggregate = t.(eventsourcing.Aggregater[K])
		}

		// insert new events
		var lastID eventid.EventID
		now = time.Now()
		for _, mig := range migration {
			version++
			lastID = gen.NewID()
			event := &Event{
				ID:               lastID.String(),
				AggregateID:      last.AggregateID.String(),
				AggregateIDHash:  int32(last.AggregateIDHash),
				AggregateVersion: version,
				AggregateKind:    last.AggregateKind,
				Kind:             mig.Kind,
				Body:             mig.Body,
				IdempotencyKey:   mig.IdempotencyKey,
				Metadata:         fromMetadata(r.metadata),
				CreatedAt:        now,
				Migrated:         true,
			}
			err = r.saveEvent(ctx, event, lastID)
			if err != nil {
				return err
			}
			evt, err := toEventsourcingEvent[K, PK](event, lastID)
			if err != nil {
				return err
			}
			if aggregate != nil {
				err = rehydrateFunc(aggregate, evt)
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

			err = r.saveSnapshot(ctx, &Snapshot{
				ID:               lastID.String(),
				AggregateID:      last.AggregateID.String(),
				AggregateVersion: version,
				AggregateKind:    aggregate.GetKind(),
				Body:             body,
				CreatedAt:        time.Now().UTC(),
				Metadata:         fromMetadata(r.metadata),
			})
			if err != nil {
				return err
			}
		}

		return nil
	})
}
