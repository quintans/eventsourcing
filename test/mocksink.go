package test

import (
	"context"
	"sync"

	"github.com/quintans/eventsourcing"
	"github.com/quintans/eventsourcing/encoding"
	"github.com/quintans/eventsourcing/sink"
	"github.com/quintans/eventsourcing/util"
	"github.com/quintans/faults"
)

var _ sink.Sinker = (*MockSinkClient)(nil)

type MockSinkData struct {
	mu          sync.Mutex
	events      map[uint32][]*sink.Message
	lastResumes map[uint32]encoding.Base64
}

func NewMockSinkData() *MockSinkData {
	return &MockSinkData{
		events:      map[uint32][]*sink.Message{},
		lastResumes: map[uint32]encoding.Base64{},
	}
}

func (s *MockSinkData) GetEvents() []*sink.Message {
	s.mu.Lock()
	events := []*sink.Message{}
	for _, v := range s.events {
		events = append(events, v...)
	}
	s.mu.Unlock()
	return events
}

func (s *MockSinkData) LastResumes() map[uint32]encoding.Base64 {
	s.mu.Lock()
	defer s.mu.Unlock()

	msgs := map[uint32]encoding.Base64{}
	for k, v := range s.lastResumes {
		msgs[k] = v
	}

	return msgs
}

func (s *MockSinkData) SetLastResumes(lastEvents map[uint32]encoding.Base64) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.lastResumes = lastEvents
}

type MockSinkClient struct {
	totalPartitions uint32
	partitions      []uint32
	onSink          func(ctx context.Context, e *eventsourcing.Event) error
	data            *MockSinkData
}

func NewMockSink(data *MockSinkData, partitions, partitionsLow, partitionHi uint32) *MockSinkClient {
	parts := []uint32{}
	for i := partitionsLow; i <= partitionHi; i++ {
		parts = append(parts, i)
	}

	return &MockSinkClient{
		data:            data,
		partitions:      parts,
		totalPartitions: partitions,
	}
}

func (s *MockSinkClient) Partitions() (uint32, []uint32) {
	return s.totalPartitions, s.partitions
}

func (s *MockSinkClient) Accepts(hash uint32) bool {
	partition := util.CalcPartition(hash, uint32(len(s.partitions)))
	return util.In(partition, s.partitions...)
}

func (s *MockSinkClient) OnSink(handler func(ctx context.Context, e *eventsourcing.Event) error) {
	s.onSink = handler
}

func (s *MockSinkClient) Sink(ctx context.Context, e *eventsourcing.Event, m sink.Meta) error {
	partition := util.CalcPartition(e.AggregateIDHash, uint32(len(s.partitions)))
	if !util.In(partition, s.partitions...) {
		return nil
	}

	s.data.mu.Lock()
	events := s.data.events[partition]
	msg := sink.ToMessage(e)
	s.data.events[partition] = append(events, msg)
	s.data.lastResumes[partition] = m.ResumeToken
	s.data.mu.Unlock()

	if s.onSink != nil {
		err := s.onSink(ctx, e)
		if err != nil {
			return faults.Wrap(err)
		}
	}

	return nil
}

func (s *MockSinkClient) ResumeTokens(ctx context.Context, forEach func(resumeToken encoding.Base64) error) error {
	s.data.mu.Lock()
	defer s.data.mu.Unlock()
	for _, partition := range s.partitions {
		resume, ok := s.data.lastResumes[partition]
		if !ok {
			continue
		}
		err := forEach(resume)
		if err != nil {
			return faults.Wrap(err)
		}
	}
	return nil
}

func (s *MockSinkClient) Close() {}
