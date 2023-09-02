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
	"github.com/quintans/eventsourcing/util"

	"github.com/IBM/sarama"
)

var _ sink.Sinker = (*Sink)(nil)

const checkPointBuffer = 1_000

type Sink struct {
	kvStore    store.KVStore
	logger     log.Logger
	producer   sarama.SyncProducer
	topic      string
	partitions []uint32
	codec      sink.Codec
	brokers    []string

	checkPointCh chan resume
}

type resume struct {
	key   string
	value encoding.Base64
}

// NewSink instantiate a Kafka sink
func NewSink(logger log.Logger, kvStore store.KVStore, topic string, brokers []string) (*Sink, error) {
	// producer config
	config := sarama.NewConfig()
	config.Producer.Retry.Max = 5
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true

	client, err := sarama.NewClient(brokers, config)
	if err != nil {
		return nil, faults.Errorf("initializing kafka client: %w", err)
	}
	prd, err := sarama.NewSyncProducerFromClient(client)
	if err != nil {
		return nil, faults.Errorf("initializing kafka producer: %w", err)
	}

	partitions, err := client.Partitions(topic)
	if err != nil {
		return nil, faults.Errorf("getting partitions: %w", err)
	}

	s := &Sink{
		kvStore:      kvStore,
		logger:       logger.WithTags(log.Tags{"sink": "kafka"}),
		topic:        topic,
		codec:        sink.JSONCodec{},
		producer:     prd,
		partitions:   util.NormalizePartitions(partitions),
		brokers:      brokers,
		checkPointCh: make(chan resume, checkPointBuffer),
	}

	// saves into the resume db. It is fine if it sporadically fails. It will just pickup from there
	go func() {
		for cp := range s.checkPointCh {
			if cp.key == "" {
				// quit received
				return
			}

			ts := cp.value.String()
			err := s.kvStore.Put(context.Background(), cp.key, ts)
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
	s.checkPointCh <- resume{} // signal quit
}

func (s *Sink) Partitions() (total uint32, partitions []uint32) {
	return uint32(len(s.partitions)), s.partitions
}

// ResumeTokens iterates over all the last saved resumed token per partition
// It will return 0 if there is no last message
func (s *Sink) ResumeTokens(ctx context.Context, forEach func(resumeToken encoding.Base64) error) (e error) {
	defer faults.Catch(&e, "ResumeTokens(...)")

	for _, partition := range s.partitions {
		topic, err := resumeTokenKey(s.topic, partition)
		if err != nil {
			return faults.Wrap(err)
		}
		resume, err := s.kvStore.Get(ctx, topic)
		if err != nil {
			if errors.Is(err, store.ErrResumeTokenNotFound) {
				continue
			}
			return faults.Wrap(err)
		}

		dec, err := encoding.ParseBase64(resume)
		if err != nil {
			return faults.Wrap(err)
		}
		err = forEach(dec)
		if err != nil {
			return faults.Wrap(err)
		}
	}
	return nil
}

func resumeTokenKey(topic string, partitionID uint32) (_ string, e error) {
	defer faults.Catch(&e, "resumeTokenKey(topic=%s, partitionID=%d)", topic, partitionID)

	if topic == "" {
		return "", faults.New("topic root cannot be empty")
	}
	if partitionID < 1 {
		return "", faults.Errorf("the partitionID (%d) must be greater than  0", partitionID)
	}
	return fmt.Sprintf("%s#%d", topic, partitionID), nil
}

func (s *Sink) Accepts(_ uint32) bool {
	return true
}

// Sink sends the event to the message queue
func (s *Sink) Sink(_ context.Context, e *eventsourcing.Event, m sink.Meta) (er error) {
	defer faults.Catch(&er, "Sink(...)")

	body, err := s.codec.Encode(e)
	if err != nil {
		return faults.Errorf("encoding event: %w", err)
	}

	msg := &sarama.ProducerMessage{
		Topic: s.topic,
		Key:   sarama.StringEncoder(e.AggregateID),
		Value: sarama.ByteEncoder(body),
	}
	partition, _, err := s.producer.SendMessage(msg)
	if err != nil {
		return faults.Errorf("encoding event: %w", err)
	}

	topic, err := resumeTokenKey(s.topic, uint32(partition+1))
	if err != nil {
		return faults.Wrap(err)
	}
	s.checkPointCh <- resume{key: topic, value: m.ResumeToken}

	return nil
}
