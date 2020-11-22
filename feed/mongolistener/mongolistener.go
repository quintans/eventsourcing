package mongolistener

import (
	"context"
	"time"

	"github.com/quintans/eventstore"
	"github.com/quintans/eventstore/common"
	"github.com/quintans/eventstore/feed"
	"github.com/quintans/eventstore/repo"
	"github.com/quintans/eventstore/sink"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type MongoListener struct {
	dbName     string
	client     *mongo.Client
	partitions int
}

type Option func(*MongoListener)

func WithPartitions(partitions int) Option {
	return func(p *MongoListener) {
		if partitions > 0 {
			p.partitions = partitions
		}
	}
}

func New(connString string, dbName string, opts ...Option) (MongoListener, error) {
	ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(connString))
	if err != nil {
		return MongoListener{}, err
	}

	m := MongoListener{
		dbName:     dbName,
		client:     client,
		partitions: 0,
	}
	for _, o := range opts {
		o(&m)
	}

	return m, nil
}

type ChangeEvent struct {
	FullDocument repo.MongoEvent `bson:"fullDocument,omitempty"`
}

func (m MongoListener) Feed(ctx context.Context, sinker sink.Sinker, filters ...repo.FilterOption) error {
	_, resumeToken, err := feed.LastEventIDInSink(ctx, sinker, m.partitions)
	if err != nil {
		return err
	}

	matchPipeline := bson.D{{Key: "$match", Value: bson.D{{Key: "operationType", Value: "insert"}}}}
	pipeline := mongo.Pipeline{matchPipeline}

	eventsCollection := m.client.Database(m.dbName).Collection("events")
	var eventsStream *mongo.ChangeStream
	if len(resumeToken) != 0 {
		eventsStream, err = eventsCollection.Watch(ctx, pipeline, options.ChangeStream().SetResumeAfter(bson.Raw(resumeToken)))
	} else {
		eventsStream, err = eventsCollection.Watch(ctx, pipeline)
	}
	if err != nil {
		return err
	}
	defer eventsStream.Close(ctx)

	for eventsStream.Next(ctx) {
		var data ChangeEvent
		if err := eventsStream.Decode(&data); err != nil {
			return err
		}
		eventDoc := data.FullDocument
		for k, d := range eventDoc.Details {
			event := eventstore.Event{
				ID:               common.NewMessageID(eventDoc.ID, uint8(k)),
				ResumeToken:      []byte(eventsStream.ResumeToken()),
				AggregateID:      eventDoc.AggregateID,
				AggregateVersion: eventDoc.AggregateVersion,
				AggregateType:    eventDoc.AggregateType,
				Kind:             d.Kind,
				Body:             d.Body,
				IdempotencyKey:   eventDoc.IdempotencyKey,
				Labels:           eventDoc.Labels,
				CreatedAt:        eventDoc.CreatedAt,
			}
			err = sinker.Sink(ctx, event)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (m MongoListener) Close(ctx context.Context) error {
	return m.client.Disconnect(ctx)
}
