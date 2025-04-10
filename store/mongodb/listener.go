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
	"github.com/quintans/eventsourcing/store"
)

type Feed[K eventsourcing.ID, PK eventsourcing.IDPt[K]] struct {
	logger           *slog.Logger
	connString       string
	dbName           string
	eventsCollection string
	sinker           sink.Sinker[K]
	filter           *store.Filter
}

type FeedOption[K eventsourcing.ID, PK eventsourcing.IDPt[K]] func(*Feed[K, PK])

func WithFeedEventsCollection[K eventsourcing.ID, PK eventsourcing.IDPt[K]](eventsCollection string) FeedOption[K, PK] {
	return func(p *Feed[K, PK]) {
		p.eventsCollection = eventsCollection
	}
}

func WithFilter[K eventsourcing.ID, PK eventsourcing.IDPt[K]](filter *store.Filter) FeedOption[K, PK] {
	return func(p *Feed[K, PK]) {
		p.filter = filter
	}
}

func NewFeed[K eventsourcing.ID, PK eventsourcing.IDPt[K]](logger *slog.Logger, connString, database string, sinker sink.Sinker[K], opts ...FeedOption[K, PK]) (Feed[K, PK], error) {
	feed := Feed[K, PK]{
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

func (f *Feed[K, PK]) Run(ctx context.Context) error {
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
		{Key: "operationType", Value: "insert"},
	}
	var splits uint32
	splitIDs := []uint32{}
	if f.filter != nil && int(splits) != len(splitIDs) {
		match = append(match, f.feedPartitionFilter(splits, splitIDs))
	}

	matchPipeline := bson.D{{Key: "$match", Value: match}}
	pipeline := mongo.Pipeline{matchPipeline}

	eventsCollection := client.Database(f.dbName).Collection(f.eventsCollection)
	var eventsStream *mongo.ChangeStream
	if len(lastResumeToken) != 0 {
		f.logger.Info("Starting feeding",
			"splits", splits,
			"splitIDs", splitIDs,
			"lastResumeToken", hex.EncodeToString(lastResumeToken),
		)
		eventsStream, err = eventsCollection.Watch(ctx, pipeline, options.ChangeStream().SetResumeAfter(bson.Raw(lastResumeToken)))
		if err != nil {
			return faults.Wrap(err)
		}
	} else {
		f.logger.Info("Starting feeding from the beginning",
			"splits", splits,
			"splitIDs", splitIDs,
		)
		eventsStream, err = eventsCollection.Watch(ctx, pipeline, options.ChangeStream().SetStartAtOperationTime(&primitive.Timestamp{}))
		if err != nil {
			return faults.Wrap(err)
		}
	}
	defer eventsStream.Close(ctx)

	// TODO should be configured
	b := backoff.NewExponentialBackOff()
	b.MaxElapsedTime = 10 * time.Second

	// TODO is there any instance that it is no
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
			aggID := PK(new(K))
			err = aggID.UnmarshalText([]byte(eventDoc.AggregateID))
			if err != nil {
				return faults.Errorf("unmarshaling id '%s': %w", eventDoc.AggregateID, backoff.Permanent(err))
			}

			event := &eventsourcing.Event[K]{
				ID:               id,
				AggregateID:      *aggID,
				AggregateIDHash:  uint32(eventDoc.AggregateIDHash),
				AggregateVersion: eventDoc.AggregateVersion,
				AggregateKind:    eventDoc.AggregateKind,
				Kind:             eventDoc.Kind,
				Body:             eventDoc.Body,
				Discriminator:    toDiscriminator(eventDoc.Discriminator),
				CreatedAt:        eventDoc.CreatedAt,
				Migrated:         eventDoc.Migrated,
			}
			err = f.sinker.Sink(ctx, event, sink.Meta{ResumeToken: lastResumeToken})
			if err != nil {
				return faults.Errorf("sinking: %w", err)
			}

			b.Reset()
		}

		f.logger.Info("Shutting down feed", "dbName", f.dbName, "eventsCollection", f.eventsCollection)

		err := eventsStream.Err()
		if errors.Is(err, context.Canceled) {
			return backoff.Permanent(err)
		}
		return err
	}, b)
}

func (f *Feed[K, PK]) feedPartitionFilter(partitions uint32, partitionIDs []uint32) bson.E {
	parts := make([]uint32, len(partitionIDs))
	for k, v := range partitionIDs {
		parts[k] = v - 1
	}

	field := "$fullDocument.aggregate_id_hash"
	// aggregate: { $expr: {"$eq": [{"$mod" : [$field, m.partitions]}],  m.partitionsLow - 1]} }
	return bson.E{
		Key: "$expr",
		Value: bson.D{
			{Key: "$in", Value: bson.A{
				bson.D{
					{Key: "$mod", Value: bson.A{field, partitions}},
				},
				parts,
			}},
		},
	}
}
