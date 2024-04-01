package mongodb

import (
	"context"
	"errors"
	"slices"
	"time"

	"github.com/avast/retry-go/v3"
	"github.com/quintans/eventsourcing"
	"github.com/quintans/faults"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

type Schema struct {
	CollectionNames []string
}

func WithPostSchemaCreation[K eventsourcing.ID, PK eventsourcing.IDPt[K]](post func(Schema) []bson.D) Option[K, PK] {
	return func(r *EsRepository[K, PK]) {
		r.postSchemaCreation = post
	}
}

func (r *EsRepository[K, PK]) createSchema(ctx context.Context) error {
	err := retry.Do(
		func() error {
			err := r.client.Ping(ctx, readpref.Primary())
			if err != nil && errors.Is(err, ctx.Err()) {
				return retry.Unrecoverable(err)
			}
			return faults.Wrapf(err, "pinging")
		},
		retry.Attempts(3),
		retry.Delay(time.Second),
	)
	if err != nil {
		return faults.Wrapf(err, "database did not respond to ping")
	}

	db := r.client.Database(r.dbName)
	cNames, err := db.ListCollectionNames(ctx, bson.D{})
	if err != nil {
		return faults.Wrapf(err, "getting collections")
	}

	// Simply search in the names slice, e.g.
	for _, name := range cNames {
		if name == r.snapshotsCollectionName {
			return nil
		}
	}

	cmds := []bson.D{}
	if !slices.Contains(cNames, r.eventsCollectionName) {
		cmds = append(cmds,
			bson.D{
				{"create", r.eventsCollectionName},
			},
			bson.D{
				{"createIndexes", r.eventsCollectionName},
				{"indexes", []bson.D{
					{
						{"key", bson.D{
							{"aggregate_id", 1},
							{"migration", 1},
						}},
						{"name", "evt_agg_id_migrated_idx"},
						{"unique", false},
						{"background", true},
					},
					{
						{"key", bson.D{
							{"_id", 1},
							{"migration", 1},
						}},
						{"name", "evt_migration_idx"},
						{"unique", false},
						{"background", true},
					},
					{
						{"key", bson.D{
							{"aggregate_kind", 1},
							{"migration", 1},
						}},
						{"name", "evt_type_migrated_idx"},
						{"unique", false},
						{"background", true},
					},
					{
						{"key", bson.D{
							{"aggregate_id", 1},
							{"aggregate_version", 1},
						}},
						{"name", "unique_aggregate_version"},
						{"unique", true},
						{"background", true},
					},
				}},
			})
	}

	if !slices.Contains(cNames, r.snapshotsCollectionName) {
		cmds = append(cmds,
			bson.D{
				{"create", r.snapshotsCollectionName},
			},
			bson.D{
				{"createIndexes", r.snapshotsCollectionName},
				{"indexes", []bson.D{
					{
						{"key", bson.D{
							{"aggregate_id", 1},
						}},
						{"name", "idx_aggregate"},
						{"background", true},
					},
				}},
			})
	}

	if r.postSchemaCreation != nil {
		cmds = append(cmds, r.postSchemaCreation(Schema{CollectionNames: cNames})...)
	}

	for _, c := range cmds {
		err := db.RunCommand(ctx, c).Err()
		if err != nil {
			return faults.Wrapf(err, "running command %+v", c)
		}
	}

	return nil
}
