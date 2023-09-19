package mongodb

import (
	"context"
	"time"

	"github.com/quintans/faults"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/quintans/eventsourcing/store"
)

var _ store.KVStore = (*KVStore)(nil)

type kvStoreRow struct {
	Token string `bson:"value,omitempty"`
}

type KVStore struct {
	Repository
	collection *mongo.Collection
}

func NewKVStoreWithURI(connString, database, collection string) (KVStore, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	client, err := mongo.Connect(ctx, options.Client().ApplyURI(connString))
	if err != nil {
		return KVStore{}, faults.Wrap(err)
	}

	return NewKVStore(client, database, collection), nil
}

func NewKVStore(client *mongo.Client, dbName, collection string) KVStore {
	c := client.Database(dbName).Collection(collection)

	return KVStore{
		Repository: Repository{
			client: client,
		},
		collection: c,
	}
}

func (m KVStore) Get(ctx context.Context, key string) (string, error) {
	opts := options.FindOne()
	row := kvStoreRow{}
	if err := m.collection.FindOne(ctx, bson.D{{"_id", key}}, opts).Decode(&row); err != nil {
		if err == mongo.ErrNoDocuments {
			return "", faults.Wrap(store.ErrResumeTokenNotFound)
		}
		return "", faults.Errorf("getting resume token for key '%s': %w", key, err)
	}

	return row.Token, nil
}

func (m KVStore) Put(ctx context.Context, key string, value string) error {
	if value == "" {
		return nil
	}
	return m.WithTx(ctx, func(ctx context.Context) error {
		opts := options.Update().SetUpsert(true)
		_, err := m.collection.UpdateOne(
			ctx,
			bson.M{"_id": key},
			bson.M{
				"$set": bson.M{"value": value},
			},
			opts,
		)
		if err != nil {
			return faults.Errorf("Failed to set value for key '%s': %w", key, err)
		}

		return nil
	})
}
