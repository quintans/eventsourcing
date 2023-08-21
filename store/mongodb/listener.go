package mongodb

import (
	"bytes"
	"context"
	"errors"
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
	"github.com/quintans/eventsourcing/log"
	"github.com/quintans/eventsourcing/sink"
	"github.com/quintans/eventsourcing/store"
)

type Feed struct {
	logger           log.Logger
	connString       string
	dbName           string
	eventsCollection string
	partitions       uint32
	partitionsLow    uint32
	partitionsHi     uint32
	sinker           sink.Sinker
	setSeqRepo       SetSeqRepository
}

type SetSeqRepository interface {
	SetSinkData(ctx context.Context, eID eventid.EventID, data sink.Data) error
}

type FeedOption func(*Feed)

func WithPartitions(partitions, partitionsLow, partitionsHi uint32) FeedOption {
	return func(p *Feed) {
		if partitions <= 1 {
			p.partitions = 0
			p.partitionsLow = 0
			p.partitionsHi = 0
			return
		}
		p.partitions = partitions
		p.partitionsLow = partitionsLow
		p.partitionsHi = partitionsHi
	}
}

func WithFeedEventsCollection(eventsCollection string) FeedOption {
	return func(p *Feed) {
		p.eventsCollection = eventsCollection
	}
}

func NewFeed(logger log.Logger, connString, database string, sinker sink.Sinker, setSeqRepo SetSeqRepository, opts ...FeedOption) Feed {
	m := Feed{
		logger:           logger,
		dbName:           database,
		connString:       connString,
		eventsCollection: "events",
		sinker:           sinker,
		setSeqRepo:       setSeqRepo,
	}

	for _, o := range opts {
		o(&m)
	}
	return m
}

type ChangeEvent struct {
	FullDocument Event `bson:"fullDocument,omitempty"`
}

func (f *Feed) Run(ctx context.Context) error {
	f.logger.Infof("Starting Feed for '%s.%s'", f.dbName, f.eventsCollection)
	var lastResumeToken []byte
	err := store.ForEachSequenceInSinkPartitions(ctx, f.sinker, f.partitionsLow, f.partitionsHi, func(_ uint64, message *sink.Message) error {
		if bytes.Compare(message.ResumeToken, lastResumeToken) > 0 {
			lastResumeToken = message.ResumeToken
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
	if f.partitions > 1 {
		match = append(match, f.feedPartitionFilter())
	}

	matchPipeline := bson.D{{Key: "$match", Value: match}}
	pipeline := mongo.Pipeline{matchPipeline}

	eventsCollection := client.Database(f.dbName).Collection(f.eventsCollection)
	var eventsStream *mongo.ChangeStream
	if len(lastResumeToken) != 0 {
		f.logger.Infof("Starting feeding (partitions: [%d-%d]) from '%X'", f.partitionsLow, f.partitionsHi, lastResumeToken)
		eventsStream, err = eventsCollection.Watch(ctx, pipeline, options.ChangeStream().SetResumeAfter(bson.Raw(lastResumeToken)))
		if err != nil {
			return faults.Wrap(err)
		}
	} else {
		f.logger.Infof("Starting feeding (partitions: [%d-%d]) from the beginning", f.partitionsLow, f.partitionsHi)
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
			data, err := f.sinker.Sink(ctx, event, sink.Meta{ResumeToken: lastResumeToken})
			if err != nil {
				return backoff.Permanent(err)
			}

			err = f.setSeqRepo.SetSinkData(ctx, id, data)
			if err != nil {
				return faults.Wrap(backoff.Permanent(err))
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

func (f *Feed) feedPartitionFilter() bson.E {
	field := "$fullDocument.aggregate_id_hash"
	// aggregate: { $expr: {"$eq": [{"$mod" : [$field, m.partitions]}],  m.partitionsLow - 1]} }
	if f.partitionsLow == f.partitionsHi {
		return bson.E{
			"$expr",
			bson.D{
				{"$eq", bson.A{
					bson.D{
						{"$mod", bson.A{field, f.partitions}},
					},
					f.partitionsLow - 1,
				}},
			},
		}
	}

	// {$expr: {$and: [{"$gte": [ { "$mod" : [$field, m.partitions] }, m.partitionsLow - 1 ]}, {$lte: [ { $mod : [$field, m.partitions] }, partitionsHi - 1 ]}  ] }});
	return bson.E{
		"$expr",
		bson.D{
			{"$and", bson.A{
				bson.D{
					{"$gte", bson.A{
						bson.D{
							{"$mod", bson.A{field, f.partitions}},
						},
						f.partitionsLow - 1,
					}},
				},
				bson.D{
					{"$lte", bson.A{
						bson.D{
							{"$mod", bson.A{field, f.partitions}},
						},
						f.partitionsHi - 1,
					}},
				},
			}},
		},
	}
}
