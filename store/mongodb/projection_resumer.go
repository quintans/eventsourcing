package mongodb

import (
	"context"

	"github.com/quintans/faults"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/quintans/eventsourcing/projection"
)

var _ projection.ResumeStore = (*ProjectionResume)(nil)

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

func (m ProjectionResume) GetStreamResumeToken(ctx context.Context, key projection.ResumeKey) (projection.Token, error) {
	opts := options.FindOne()
	row := projectionResumeRow{}
	if err := m.collection.FindOne(ctx, bson.D{{"_id", key.String()}}, opts).Decode(&row); err != nil {
		if err == mongo.ErrNoDocuments {
			return projection.Token{}, faults.Wrap(projection.ErrResumeTokenNotFound)
		}
		return projection.Token{}, faults.Errorf("Failed to get resume token for key '%s': %w", key, err)
	}

	return projection.ParseToken(row.Token)
}

func (m ProjectionResume) SetStreamResumeToken(ctx context.Context, key projection.ResumeKey, token projection.Token) error {
	return m.WithTx(ctx, func(ctx context.Context) error {
		opts := options.Update().SetUpsert(true)
		_, err := m.collection.UpdateOne(
			ctx,
			bson.M{"_id": key.String()},
			bson.M{
				"$set": bson.M{"token": token.String()},
			},
			opts,
		)
		if err != nil {
			return faults.Errorf("Failed to set resume token for key '%s': %w", key, err)
		}

		return nil
	})
}
