package mongodb

import (
	"context"
	"slices"

	"github.com/quintans/faults"
	"go.mongodb.org/mongo-driver/bson"
)

func (r *EsRepository[K, PK]) createSchema(ctx context.Context) error {
	db := r.client.Database(r.dbName)
	cNames, err := db.ListCollectionNames(ctx, bson.M{
		"name": bson.M{"$in": bson.A{r.eventsCollectionName, r.snapshotsCollectionName}},
	})
	if err != nil {
		return faults.Wrapf(err, "getting collections")
	}

	cmds := []bson.D{}
	if !slices.Contains(cNames, r.eventsCollectionName) {
		cmds = append(cmds,
			bson.D{
				{Key: "create", Value: r.eventsCollectionName},
			},
			bson.D{
				{Key: "createIndexes", Value: r.eventsCollectionName},
				{Key: "indexes", Value: []bson.D{
					{
						{Key: "key", Value: bson.D{
							{Key: "aggregate_id", Value: 1},
							{Key: "migration", Value: 1},
						}},
						{Key: "name", Value: "evt_agg_id_migrated_idx"},
						{Key: "unique", Value: false},
						{Key: "background", Value: true},
					},
					{
						{Key: "key", Value: bson.D{
							{Key: "_id", Value: 1},
							{Key: "migration", Value: 1},
						}},
						{Key: "name", Value: "evt_migration_idx"},
						{Key: "unique", Value: false},
						{Key: "background", Value: true},
					},
					{
						{Key: "key", Value: bson.D{
							{Key: "aggregate_kind", Value: 1},
							{Key: "migration", Value: 1},
						}},
						{Key: "name", Value: "evt_type_migrated_idx"},
						{Key: "unique", Value: false},
						{Key: "background", Value: true},
					},
					{
						{Key: "key", Value: bson.D{
							{Key: "aggregate_id", Value: 1},
							{Key: "aggregate_version", Value: 1},
						}},
						{Key: "name", Value: "unique_aggregate_version"},
						{Key: "unique", Value: true},
						{Key: "background", Value: true},
					},
				}},
			})
	}

	if r.snapshotsCollectionName != "" && !slices.Contains(cNames, r.snapshotsCollectionName) {
		cmds = append(cmds,
			bson.D{
				{Key: "create", Value: r.snapshotsCollectionName},
			},
			bson.D{
				{Key: "createIndexes", Value: r.snapshotsCollectionName},
				{Key: "indexes", Value: []bson.D{
					{
						{Key: "key", Value: bson.D{
							{Key: "aggregate_id", Value: 1},
						}},
						{Key: "name", Value: "idx_aggregate"},
						{Key: "background", Value: true},
					},
				}},
			})
	}

	for _, c := range cmds {
		err := db.RunCommand(ctx, c).Err()
		if err != nil {
			return faults.Wrapf(err, "running command %+v", c)
		}
	}
	return nil
}
