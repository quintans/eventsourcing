package mongodb

import (
	"context"
	"time"

	"github.com/quintans/eventstore"
	"github.com/quintans/eventstore/common"
	"github.com/quintans/eventstore/sink"
	"github.com/quintans/eventstore/store"
	"github.com/quintans/faults"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type Feed struct {
	dbName        string
	client        *mongo.Client
	partitions    uint32
	partitionsLow uint32
	partitionsHi  uint32
}

type FeedOption func(*Feed)

func WithPartitions(partitions, partitionsLow, partitionsHi uint32) FeedOption {
	return func(p *Feed) {
		if partitions <= 1 {
			return
		}
		p.partitions = partitions
		p.partitionsLow = partitionsLow
		p.partitionsHi = partitionsHi
	}
}

func NewFeed(connString string, dbName string, opts ...FeedOption) (Feed, error) {
	ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(connString))
	if err != nil {
		return Feed{}, faults.Errorf("Unable to connect to '%s': %w", connString, err)
	}

	m := Feed{
		dbName: dbName,
		client: client,
	}

	for _, o := range opts {
		o(&m)
	}
	return m, nil
}

type ChangeEvent struct {
	FullDocument Event `bson:"fullDocument,omitempty"`
}

func (m Feed) Feed(ctx context.Context, sinker sink.Sinker) error {
	_, resumeToken, err := store.LastEventIDInSink(ctx, sinker, m.partitionsLow, m.partitionsHi)
	if err != nil {
		return err
	}

	match := bson.D{
		{"operationType", "insert"},
	}
	if m.partitions > 1 {
		match = append(match, partitionFilter("fullDocument.aggregate_id_hash", m.partitions, m.partitionsLow, m.partitionsHi))
	}

	matchPipeline := bson.D{{Key: "$match", Value: match}}
	pipeline := mongo.Pipeline{matchPipeline}

	eventsCollection := m.client.Database(m.dbName).Collection("events")
	var eventsStream *mongo.ChangeStream
	if len(resumeToken) != 0 {
		eventsStream, err = eventsCollection.Watch(ctx, pipeline, options.ChangeStream().SetResumeAfter(bson.Raw(resumeToken)))
		err = faults.Wrap(err)
	} else {
		eventsStream, err = eventsCollection.Watch(ctx, pipeline)
		err = faults.Wrap(err)
	}
	if err != nil {
		return err
	}
	defer eventsStream.Close(ctx)

	for eventsStream.Next(ctx) {
		var data ChangeEvent
		if err := eventsStream.Decode(&data); err != nil {
			return faults.Wrap(err)
		}
		eventDoc := data.FullDocument

		// check if the event is to be forwarded to the sinker
		p := common.WhichPartition(eventDoc.AggregateIDHash, m.partitions)
		if p < m.partitionsLow || p > m.partitionsHi {
			continue
		}

		for k, d := range eventDoc.Details {
			event := eventstore.Event{
				ID:               common.NewMessageID(eventDoc.ID, uint8(k)),
				ResumeToken:      []byte(eventsStream.ResumeToken()),
				AggregateID:      eventDoc.AggregateID,
				AggregateIDHash:  eventDoc.AggregateIDHash,
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

func (m Feed) Close(ctx context.Context) error {
	return m.client.Disconnect(ctx)
}
