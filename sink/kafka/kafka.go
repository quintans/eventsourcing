package kafka

import (
	"context"
	"errors"
	"fmt"

	"github.com/quintans/faults"

	"github.com/quintans/eventsourcing"
	"github.com/quintans/eventsourcing/encoding"
	"github.com/quintans/eventsourcing/log"
	"github.com/quintans/eventsourcing/sink"
	"github.com/quintans/eventsourcing/store"

	"github.com/IBM/sarama"
)

var _ sink.Sinker = (*Sink)(nil)

const checkPointBuffer = 1_000

type Sink struct {
	kvStore  store.KVStore
	logger   log.Logger
	producer sarama.SyncProducer
	topic    string
	codec    sink.Codec
	brokers  []string

	checkPointCh chan encoding.Base64
}

// NewSink instantiate a Kafka sink
func NewSink(logger log.Logger, kvStore store.KVStore, topic string, brokers []string) (*Sink, error) {
	// producer config
	config := sarama.NewConfig()
	config.Producer.Retry.Max = 5
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true

	prd, err := sarama.NewSyncProducer(brokers, config)
	if err != nil {
		return nil, faults.Errorf("initializing kafka sink: %w", err)
	}

	s := &Sink{
		kvStore:      kvStore,
		logger:       logger,
		topic:        topic,
		codec:        sink.JSONCodec{},
		producer:     prd,
		brokers:      brokers,
		checkPointCh: make(chan encoding.Base64, checkPointBuffer),
	}

	// saves into the resume db. It is fine if it sporadically fails. It will just pickup from there
	go func() {
		for token := range s.checkPointCh {
			ts := token.String()
			err := s.kvStore.Put(context.Background(), topic, ts)
			if err != nil {
				logger.WithError(err).WithTags(log.Tags{
					"topic":  topic,
					"resume": ts,
				}).Error("Failed to save sink resume key")
			}
		}
	}()

	return s, nil
}

func (s *Sink) SetCodec(codec sink.Codec) {
	s.codec = codec
}

func (s *Sink) Close() {
	if s.producer != nil {
		s.producer.Close()
	}
}

// ResumeToken gets the last saved resumed token
// It will return 0 if there is no last message
func (s *Sink) ResumeToken(ctx context.Context, partition uint32) (encoding.Base64, error) {
	topic, err := resumeTokenKey(s.topic, partition)
	if err != nil {
		return nil, faults.Wrap(err)
	}
	resume, err := s.kvStore.Get(ctx, topic)
	if err != nil {
		if errors.Is(err, store.ErrResumeTokenNotFound) {
			return nil, nil
		}
		return nil, faults.Wrap(err)
	}

	dec, err := encoding.ParseBase64(resume)
	if err != nil {
		return nil, faults.Wrap(err)
	}
	return dec, nil
}

func resumeTokenKey(topic string, partitionID uint32) (_ string, e error) {
	defer faults.Catch(&e, "ComposeTopic(topic=%s, partitionID=%d)", topic, partitionID)

	if topic == "" {
		return "", faults.New("topic root cannot be empty")
	}
	if partitionID < 1 {
		return "", faults.Errorf("the partitionID (%d) must be greater than  0", partitionID)
	}
	return fmt.Sprintf("%s#%d", topic, partitionID), nil
}

// Sink sends the event to the message queue
func (s *Sink) Sink(_ context.Context, e *eventsourcing.Event, m sink.Meta) error {
	body, err := s.codec.Encode(e)
	if err != nil {
		return faults.Errorf("encoding event: %w", err)
	}

	s.logger.WithTags(log.Tags{
		"topic": s.topic,
	}).Debugf("publishing '%+v'", e)

	msg := &sarama.ProducerMessage{
		Topic: s.topic,
		Key:   sarama.StringEncoder(e.AggregateID),
		Value: sarama.ByteEncoder(body),
	}
	_, _, err = s.producer.SendMessage(msg)
	if err != nil {
		return faults.Errorf("encoding event: %w", err)
	}

	s.checkPointCh <- m.ResumeToken

	return nil
}
