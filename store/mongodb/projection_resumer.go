package mongodb

import (
	"context"

	"github.com/quintans/faults"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/quintans/eventsourcing/store"
)

var _ store.KVStore = (*ProjectionResume)(nil)

type projectionResumeRow struct {
	ID    string `bson:"_id,omitempty"`
	Token string `bson:"token,omitempty"`
}

type ProjectionResume struct {
	Repository
	collection *mongo.Collection
}

func NewProjectionResume(client *mongo.Client, dbName, collection string) ProjectionResume {
	c := client.Database(dbName).Collection(collection)

	return ProjectionResume{
		Repository: Repository{
			client: client,
		},
		collection: c,
	}
}

func (m ProjectionResume) Get(ctx context.Context, key string) (string, error) {
	opts := options.FindOne()
	row := projectionResumeRow{}
	if err := m.collection.FindOne(ctx, bson.D{{"_id", key}}, opts).Decode(&row); err != nil {
		if err == mongo.ErrNoDocuments {
			return "", faults.Wrap(store.ErrResumeTokenNotFound)
		}
		return "", faults.Errorf("getting resume token for key '%s': %w", key, err)
	}

	return row.Token, nil
}

func (m ProjectionResume) Put(ctx context.Context, key string, token string) error {
	return m.WithTx(ctx, func(ctx context.Context) error {
		opts := options.Update().SetUpsert(true)
		_, err := m.collection.UpdateOne(
			ctx,
			bson.M{"_id": key},
			bson.M{
				"$set": bson.M{"token": token},
			},
			opts,
		)
		if err != nil {
			return faults.Errorf("Failed to set resume token for key '%s': %w", key, err)
		}

		return nil
	})
}
