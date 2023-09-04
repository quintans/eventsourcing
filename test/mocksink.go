package test

import (
	"context"
	"sync"

	"github.com/oklog/ulid/v2"
	"github.com/quintans/eventsourcing"
	"github.com/quintans/eventsourcing/encoding"
	"github.com/quintans/eventsourcing/sink"
	"github.com/quintans/eventsourcing/util"
	"github.com/quintans/faults"
)

var _ sink.Sinker[*ulid.ULID] = (*MockSinkClient[*ulid.ULID])(nil)

type MockSinkData[K eventsourcing.ID] struct {
	mu          sync.Mutex
	events      map[uint32][]*sink.Message[K]
	lastResumes map[uint32]encoding.Base64
}

func NewMockSinkData[K eventsourcing.ID]() *MockSinkData[K] {
	return &MockSinkData[K]{
		events:      map[uint32][]*sink.Message[K]{},
		lastResumes: map[uint32]encoding.Base64{},
	}
}

func (s *MockSinkData[K]) GetEvents() []*sink.Message[K] {
	s.mu.Lock()
	events := []*sink.Message[K]{}
	for _, v := range s.events {
		events = append(events, v...)
	}
	s.mu.Unlock()
	return events
}

func (s *MockSinkData[K]) LastResumes() map[uint32]encoding.Base64 {
	s.mu.Lock()
	defer s.mu.Unlock()

	msgs := map[uint32]encoding.Base64{}
	for k, v := range s.lastResumes {
		msgs[k] = v
	}

	return msgs
}

func (s *MockSinkData[K]) SetLastResumes(lastEvents map[uint32]encoding.Base64) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.lastResumes = lastEvents
}

type MockSinkClient[K eventsourcing.ID] struct {
	totalPartitions uint32
	partitions      []uint32
	onSink          func(ctx context.Context, e *eventsourcing.Event[K]) error
	data            *MockSinkData[K]
}

func NewMockSink[K eventsourcing.ID](data *MockSinkData[K], partitions, partitionsLow, partitionHi uint32) *MockSinkClient[K] {
	parts := []uint32{}
	for i := partitionsLow; i <= partitionHi; i++ {
		parts = append(parts, i)
	}

	return &MockSinkClient[K]{
		data:            data,
		partitions:      parts,
		totalPartitions: partitions,
	}
}

func (s *MockSinkClient[K]) Partitions() (uint32, []uint32) {
	return s.totalPartitions, s.partitions
}

func (s *MockSinkClient[K]) Accepts(hash uint32) bool {
	partition := util.CalcPartition(hash, uint32(len(s.partitions)))
	return util.In(partition, s.partitions...)
}

func (s *MockSinkClient[K]) OnSink(handler func(ctx context.Context, e *eventsourcing.Event[K]) error) {
	s.onSink = handler
}

func (s *MockSinkClient[K]) Sink(ctx context.Context, e *eventsourcing.Event[K], m sink.Meta) error {
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

func (s *MockSinkClient[K]) ResumeTokens(ctx context.Context, forEach func(resumeToken encoding.Base64) error) error {
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

func (s *MockSinkClient[K]) Close() {}
