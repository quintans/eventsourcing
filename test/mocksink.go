package test

import (
	"context"
	"fmt"
	"sync"

	"github.com/quintans/eventsourcing"
	"github.com/quintans/eventsourcing/encoding"
	"github.com/quintans/eventsourcing/sink"
	"github.com/quintans/eventsourcing/util"
	"github.com/quintans/faults"
)

var _ sink.Sinker = (*MockSink)(nil)

type MockSink struct {
	mu          sync.Mutex
	partitions  uint32
	events      map[uint32][]*sink.Message
	lastResumes map[uint32]encoding.Base64
	onSink      func(ctx context.Context, e *eventsourcing.Event) error
}

func NewMockSink(partitions uint32) *MockSink {
	events := map[uint32][]*sink.Message{}
	lastEvents := map[uint32]encoding.Base64{}

	return &MockSink{
		events:      events,
		lastResumes: lastEvents,
		partitions:  partitions,
	}
}

func (s *MockSink) OnSink(handler func(ctx context.Context, e *eventsourcing.Event) error) {
	s.onSink = handler
}

func (s *MockSink) Sink(ctx context.Context, e *eventsourcing.Event, m sink.Meta) error {
	partition := util.CalcPartition(e.AggregateIDHash, s.partitions)

	s.mu.Lock()
	defer s.mu.Unlock()

	events := s.events[partition]
	msg := sink.ToMessage(e)
	s.events[partition] = append(events, msg)
	s.lastResumes[partition] = m.ResumeToken

	if s.onSink != nil {
		err := s.onSink(ctx, e)
		if err != nil {
			return faults.Wrap(err)
		}
	}

	return nil
}

func (s *MockSink) ResumeToken(ctx context.Context, partition uint32) (encoding.Base64, error) {
	if partition == 0 {
		partition = 1
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	resume, ok := s.lastResumes[partition]
	if !ok {
		return nil, nil
	}
	return resume, nil
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

func (s *MockSink) LastResumes() map[uint32]encoding.Base64 {
	s.mu.Lock()
	defer s.mu.Unlock()

	msgs := map[uint32]encoding.Base64{}
	for k, v := range s.lastResumes {
		msgs[k] = v
	}

	return msgs
}

func (s *MockSink) SetLastResumes(lastEvents map[uint32]encoding.Base64) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.lastResumes = lastEvents
}

func (s *MockSink) Dump() {
	for _, v := range s.GetEvents() {
		fmt.Printf("ID:%s; AggregateID: %s; Kind:%s; Body: %s\n", v.ID, v.AggregateID, v.Kind, string(v.Body))
	}
	fmt.Printf("LastResumes: %+v", s.lastResumes)
}
