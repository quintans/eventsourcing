package test

import (
	"context"
	"fmt"
	"sync"

	"github.com/quintans/eventsourcing"
	"github.com/quintans/eventsourcing/sink"
	"github.com/quintans/eventsourcing/util"
	"github.com/quintans/faults"
)

type MockSink struct {
	mu         sync.Mutex
	partitions uint32
	events     map[uint32][]*sink.Message
	lastEvents map[uint32]*sink.Message
	sequence   uint64
	onSink     func(ctx context.Context, e *eventsourcing.Event) error
}

func NewMockSink(partitions uint32) *MockSink {
	events := map[uint32][]*sink.Message{}
	lastEvents := map[uint32]*sink.Message{}

	return &MockSink{
		events:     events,
		lastEvents: lastEvents,
		partitions: partitions,
	}
}

func (s *MockSink) OnSink(handler func(ctx context.Context, e *eventsourcing.Event) error) {
	s.onSink = handler
}

func (s *MockSink) Sink(ctx context.Context, e *eventsourcing.Event, m sink.Meta) (sink.Data, error) {
	var partition uint32
	if s.partitions <= 1 {
		partition = 1
	} else {
		partition = util.WhichPartition(e.AggregateIDHash, s.partitions)
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	events := s.events[partition]
	msg := sink.ToMessage(e, m)
	s.events[partition] = append(events, msg)
	s.lastEvents[partition] = msg
	s.sequence++

	if s.onSink != nil {
		err := s.onSink(ctx, e)
		if err != nil {
			return sink.Data{}, faults.Wrap(err)
		}
	}

	return sink.NewSinkData(partition, s.sequence), nil
}

func (s *MockSink) LastMessage(ctx context.Context, partition uint32) (uint64, *sink.Message, error) {
	if partition == 0 {
		partition = 1
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	e, ok := s.lastEvents[partition]
	if !ok {
		return 0, nil, nil
	}
	return s.sequence, e, nil
}

func (s *MockSink) Close() {}

func (s *MockSink) GetEvents() []*sink.Message {
	s.mu.Lock()
	events := []*sink.Message{}
	for _, v := range s.events {
		events = append(events, v...)
	}
	s.mu.Unlock()
	return events
}

func (s *MockSink) LastMessages() map[uint32]*sink.Message {
	s.mu.Lock()
	defer s.mu.Unlock()

	msgs := map[uint32]*sink.Message{}
	for k, v := range s.lastEvents {
		msgs[k] = v
	}

	return msgs
}

func (s *MockSink) SetLastMessages(lastEvents map[uint32]*sink.Message) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.lastEvents = lastEvents
}

func (s *MockSink) Dump() {
	for _, v := range s.GetEvents() {
		fmt.Printf("ID:%s; AggregateID: %s; Kind:%s; Body: %s; ResumeToken: %s\n", v.ID, v.AggregateID, v.Kind, string(v.Body), v.ResumeToken)
	}
}
