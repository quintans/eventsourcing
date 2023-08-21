package kafka

import (
	"context"
	"time"

	"github.com/quintans/faults"

	"github.com/quintans/eventsourcing"
	"github.com/quintans/eventsourcing/log"
	"github.com/quintans/eventsourcing/sink"

	"github.com/IBM/sarama"
)

var _ sink.Sinker = (*Sink)(nil)

type Sink struct {
	logger   log.Logger
	producer sarama.SyncProducer
	topic    string
	codec    sink.Codec
	brokers  []string
}

// NewSink instantiate a Kafka sink
func NewSink(logger log.Logger, topic string, brokers []string) (*Sink, error) {
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

	return &Sink{
		logger:   logger,
		topic:    topic,
		codec:    sink.JSONCodec{},
		producer: prd,
		brokers:  brokers,
	}, nil
}

func (s *Sink) SetCodec(codec sink.Codec) {
	s.codec = codec
}

func (s *Sink) Close() {
	if s.producer != nil {
		s.producer.Close()
	}
}

// LastMessage gets the last message sent to the MQ
// It will return 0 if there is no last message
func (s *Sink) LastMessage(ctx context.Context, partition uint32) (uint64, *sink.Message, error) {
	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true

	client, err := sarama.NewClient(s.brokers, config)
	if err != nil {
		return 0, nil, faults.Errorf("creating kafka client: %w", err)
	}
	defer client.Close()

	nextOffset, err := client.GetOffset(s.topic, int32(partition), sarama.OffsetNewest)
	if err != nil {
		return 0, nil, faults.Errorf("getting offset: %w", err)
	}
	if nextOffset == 0 {
		return 0, nil, nil
	}

	consumer, err := sarama.NewConsumerFromClient(client)
	if err != nil {
		return 0, nil, faults.Errorf("creating new consumer to get last message: %w", err)
	}
	defer consumer.Close()

	partitionConsumer, err := consumer.ConsumePartition(s.topic, int32(partition), nextOffset-1)
	if err != nil {
		return 0, nil, faults.Errorf("creating consumer partition: %w", err)
	}
	defer partitionConsumer.Close()

	ctx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()

	var message *sarama.ConsumerMessage
	select {
	case message = <-partitionConsumer.Messages():
	case <-ctx.Done():
		// no last message
		return 0, nil, nil
	}

	event, err := s.codec.Decode(message.Value)
	if err != nil {
		return 0, nil, err
	}

	return uint64(message.Offset), event, nil
}

// Sink sends the event to the message queue
func (s *Sink) Sink(ctx context.Context, e *eventsourcing.Event, m sink.Meta) (sink.Data, error) {
	body, err := s.codec.Encode(e, m)
	if err != nil {
		return sink.Data{}, faults.Errorf("encoding event: %w", err)
	}

	s.logger.WithTags(log.Tags{
		"topic": s.topic,
	}).Debugf("publishing '%+v'", e)

	msg := &sarama.ProducerMessage{
		Topic: s.topic,
		Key:   sarama.StringEncoder(e.AggregateID),
		Value: sarama.ByteEncoder(body),
	}
	partition, offset, err := s.producer.SendMessage(msg)
	if err != nil {
		return sink.Data{}, faults.Errorf("encoding event: %w", err)
	}

	return sink.NewSinkData(uint32(partition), uint64(offset)), nil
}
