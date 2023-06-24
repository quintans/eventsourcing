package test

import (
	"context"
	"fmt"
	"sync"

	"github.com/quintans/eventsourcing"
	"github.com/quintans/eventsourcing/util"
)

type MockSink struct {
	mu         sync.Mutex
	partitions uint32
	events     map[uint32][]*eventsourcing.Event
	lastEvents map[uint32]*eventsourcing.Event
}

func NewMockSink(partitions uint32) *MockSink {
	events := map[uint32][]*eventsourcing.Event{}
	lastEvents := map[uint32]*eventsourcing.Event{}

	return &MockSink{
		events:     events,
		lastEvents: lastEvents,
		partitions: partitions,
	}
}

func (s *MockSink) Sink(ctx context.Context, e *eventsourcing.Event) error {
	var partition uint32
	if s.partitions <= 1 {
		partition = 1
	} else {
		partition = util.WhichPartition(e.AggregateIDHash, s.partitions)
	}
	s.mu.Lock()
	events := s.events[partition]
	s.events[partition] = append(events, e)
	s.lastEvents[partition] = e
	s.mu.Unlock()

	return nil
}

func (s *MockSink) LastMessage(ctx context.Context, partition uint32) (*eventsourcing.Event, error) {
	if partition == 0 {
		partition = 1
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.lastEvents[partition]
	if !ok {
		return nil, nil
	}
	return e, nil
}

func (s *MockSink) Close() {}

func (s *MockSink) GetEvents() []*eventsourcing.Event {
	s.mu.Lock()
	events := []*eventsourcing.Event{}
	for _, v := range s.events {
		events = append(events, v...)
	}
	s.mu.Unlock()
	return events
}

func (s *MockSink) LastMessages() map[uint32]*eventsourcing.Event {
	s.mu.Lock()
	defer s.mu.Unlock()

	msgs := map[uint32]*eventsourcing.Event{}
	for k, v := range s.lastEvents {
		msgs[k] = v
	}

	return msgs
}

func (s *MockSink) SetLastMessages(lastEvents map[uint32]*eventsourcing.Event) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.lastEvents = lastEvents
}

func (s *MockSink) Dump() {
	for _, v := range s.GetEvents() {
		fmt.Printf("ID:%s; AggregateID: %s; Kind:%s; Body: %s; ResumeToken: %s\n", v.ID, v.AggregateID, v.Kind, string(v.Body), v.ResumeToken)
	}
}
