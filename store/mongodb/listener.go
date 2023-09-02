package mongodb

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"log/slog"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/quintans/faults"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/quintans/eventsourcing"
	"github.com/quintans/eventsourcing/encoding"
	"github.com/quintans/eventsourcing/eventid"
	"github.com/quintans/eventsourcing/sink"
)

type Feed struct {
	logger           *slog.Logger
	connString       string
	dbName           string
	eventsCollection string
	sinker           sink.Sinker
}

type FeedOption func(*Feed)

func WithFeedEventsCollection(eventsCollection string) FeedOption {
	return func(p *Feed) {
		p.eventsCollection = eventsCollection
	}
}

func NewFeed(logger *slog.Logger, connString, database string, sinker sink.Sinker, opts ...FeedOption) (Feed, error) {
	feed := Feed{
		logger:           logger.With("feed", "mongo"),
		dbName:           database,
		connString:       connString,
		eventsCollection: "events",
		sinker:           sinker,
	}

	for _, o := range opts {
		o(&feed)
	}

	return feed, nil
}

type ChangeEvent struct {
	FullDocument Event `bson:"fullDocument,omitempty"`
}

func (f *Feed) Run(ctx context.Context) error {
	f.logger.Info("Starting Feed", "dbName", f.dbName, "eventsCollection", f.eventsCollection)
	var lastResumeToken []byte
	err := f.sinker.ResumeTokens(ctx, func(resumeToken encoding.Base64) error {
		if bytes.Compare(resumeToken, lastResumeToken) > 0 {
			lastResumeToken = resumeToken
		}
		return nil
	})
	if err != nil {
		return err
	}

	ctx2, cancel := context.WithTimeout(ctx, 10*time.Second)
	client, err := mongo.Connect(ctx2, options.Client().ApplyURI(f.connString))
	cancel()
	if err != nil {
		return faults.Errorf("Unable to connect to '%s': %w", f.connString, err)
	}
	defer func() {
		client.Disconnect(context.Background())
	}()

	match := bson.D{
		{"operationType", "insert"},
	}
	total, partitions := f.sinker.Partitions()
	if len(partitions) > 1 {
		match = append(match, f.feedPartitionFilter(total, partitions))
	}

	matchPipeline := bson.D{{Key: "$match", Value: match}}
	pipeline := mongo.Pipeline{matchPipeline}

	eventsCollection := client.Database(f.dbName).Collection(f.eventsCollection)
	var eventsStream *mongo.ChangeStream
	if len(lastResumeToken) != 0 {
		f.logger.Info("Starting feeding", "partitions", partitions, "lastResumeToken", hex.EncodeToString(lastResumeToken))
		eventsStream, err = eventsCollection.Watch(ctx, pipeline, options.ChangeStream().SetResumeAfter(bson.Raw(lastResumeToken)))
		if err != nil {
			return faults.Wrap(err)
		}
	} else {
		f.logger.Info("Starting feeding from the beginning", "partitions", partitions)
		eventsStream, err = eventsCollection.Watch(ctx, pipeline, options.ChangeStream().SetStartAtOperationTime(&primitive.Timestamp{}))
		if err != nil {
			return faults.Wrap(err)
		}
	}
	defer eventsStream.Close(ctx)

	// TODO should be configured
	b := backoff.NewExponentialBackOff()
	b.MaxElapsedTime = 10 * time.Second

	return backoff.Retry(func() error {
		for eventsStream.Next(ctx) {
			var changeEvent ChangeEvent
			if err := eventsStream.Decode(&changeEvent); err != nil {
				return faults.Wrap(backoff.Permanent(err))
			}
			eventDoc := changeEvent.FullDocument

			lastResumeToken = []byte(eventsStream.ResumeToken())
			id, err := eventid.Parse(eventDoc.ID)
			if err != nil {
				return faults.Wrap(backoff.Permanent(err))
			}
			// Partition and Sequence don't need to be assigned because at this moment they have a zero value.
			// They will be populate with the values returned by the sink.
			event := &eventsourcing.Event{
				ID:               id,
				AggregateID:      eventDoc.AggregateID,
				AggregateIDHash:  eventDoc.AggregateIDHash,
				AggregateVersion: eventDoc.AggregateVersion,
				AggregateKind:    eventDoc.AggregateKind,
				Kind:             eventDoc.Kind,
				Body:             eventDoc.Body,
				IdempotencyKey:   eventDoc.IdempotencyKey,
				Metadata:         encoding.JSONOfMap(eventDoc.Metadata),
				CreatedAt:        eventDoc.CreatedAt,
				Migrated:         eventDoc.Migrated,
			}
			err = f.sinker.Sink(ctx, event, sink.Meta{ResumeToken: lastResumeToken})
			if err != nil {
				return backoff.Permanent(err)
			}

			b.Reset()
		}

		err := eventsStream.Err()
		if errors.Is(err, context.Canceled) {
			return backoff.Permanent(err)
		}
		return err
	}, b)
}

func (f *Feed) feedPartitionFilter(maxPartition uint32, partitions []uint32) bson.E {
	parts := make([]uint32, len(partitions))
	for k, v := range partitions {
		parts[k] = v - 1
	}

	field := "$fullDocument.aggregate_id_hash"
	// aggregate: { $expr: {"$eq": [{"$mod" : [$field, m.partitions]}],  m.partitionsLow - 1]} }
	return bson.E{
		"$expr",
		bson.D{
			{"$in", bson.A{
				bson.D{
					{"$mod", bson.A{field, maxPartition}},
				},
				parts,
			}},
		},
	}
}
