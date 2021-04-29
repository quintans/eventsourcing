package test

import (
	"context"
	"fmt"
	"sync"

	"github.com/quintans/eventsourcing/common"
	"github.com/quintans/eventstore"
)

type MockSink struct {
	mu         sync.Mutex
	partitions uint32
	events     map[uint32][]eventstore.Event
	lastEvents map[uint32]eventstore.Event
}

func NewMockSink(partitions uint32) *MockSink {
	events := map[uint32][]eventstore.Event{}
	lastEvents := map[uint32]eventstore.Event{}

	return &MockSink{
		events:     events,
		lastEvents: lastEvents,
		partitions: partitions,
	}
}

func (s *MockSink) Sink(ctx context.Context, e eventstore.Event) error {
	var partition uint32
	if s.partitions <= 1 {
		partition = 1
	} else {
		partition = common.WhichPartition(e.AggregateIDHash, s.partitions)
	}
	s.mu.Lock()
	events := s.events[partition]
	s.events[partition] = append(events, e)
	s.lastEvents[partition] = e
	s.mu.Unlock()

	return nil
}

func (s *MockSink) LastMessage(ctx context.Context, partition uint32) (*eventstore.Event, error) {
	if partition == 0 {
		partition = 1
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.lastEvents[partition]
	if !ok {
		return nil, nil
	}
	return &e, nil
}

func (s *MockSink) Close() {}

func (s *MockSink) GetEvents() []eventstore.Event {
	s.mu.Lock()
	events := []eventstore.Event{}
	for _, v := range s.events {
		events = append(events, v...)
	}
	s.mu.Unlock()
	return events
}

func (s *MockSink) LastMessages() map[uint32]eventstore.Event {
	s.mu.Lock()
	defer s.mu.Unlock()

	msgs := map[uint32]eventstore.Event{}
	for k, v := range s.lastEvents {
		msgs[k] = v
	}

	return msgs
}

func (s *MockSink) SetLastMessages(lastEvents map[uint32]eventstore.Event) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.lastEvents = lastEvents
}

func (s *MockSink) Dump() {
	for _, v := range s.GetEvents() {
		fmt.Printf("ID:%s; AggregateID: %s; Kind:%s; Body: %s; ResumeToken: %s\n", v.ID, v.AggregateID, v.Kind, string(v.Body), v.ResumeToken)
	}
}
