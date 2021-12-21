package mongodb

import (
	"context"
	"time"

	"github.com/quintans/faults"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/quintans/eventsourcing/projection"
)

var _ projection.ResumeStore = (*StreamResumer)(nil)

type StreamResumerRow struct {
	ID    string `bson:"_id,omitempty"`
	Token string `bson:"token,omitempty"`
}

type StreamResumer struct {
	collection *mongo.Collection
}

func NewStreamResumer(connString string, dbName string, collection string) (StreamResumer, error) {
	ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(connString))
	if err != nil {
		return StreamResumer{}, faults.Wrap(err)
	}

	c := client.Database(dbName).Collection(collection)

	return StreamResumer{
		collection: c,
	}, nil
}

func (m StreamResumer) GetStreamResumeToken(ctx context.Context, key projection.ResumeKey) (string, error) {
	opts := options.FindOne()
	row := StreamResumerRow{}
	if err := m.collection.FindOne(ctx, bson.D{{"_id", key.String()}}, opts).Decode(&row); err != nil {
		if err == mongo.ErrNoDocuments {
			return "", nil
		}
		return "", faults.Errorf("Failed to get resume token for key '%s': %w", key, err)
	}

	return row.Token, nil
}

func (m StreamResumer) SetStreamResumeToken(ctx context.Context, key projection.ResumeKey, token string) error {
	opts := options.Update().SetUpsert(true)
	_, err := m.collection.UpdateOne(
		ctx,
		bson.M{"_id": key.String()},
		bson.M{
			"$set": bson.M{"token": token},
		},
		opts,
	)
	if err != nil {
		return faults.Errorf("Failed to set resume token for key '%s': %w", key, err)
	}

	return nil
}
