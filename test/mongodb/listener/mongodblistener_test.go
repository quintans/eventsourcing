package listener

import (
	"context"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/quintans/eventstore"
	"github.com/quintans/eventstore/common"
	"github.com/quintans/eventstore/sink"
	"github.com/quintans/eventstore/store/mongodb"
	"github.com/quintans/eventstore/test"
	tmg "github.com/quintans/eventstore/test/mongodb"
	"github.com/quintans/faults"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
	for i := uint32(0); i < partitions; i++ {
		events[i+1] = []eventstore.Event{}
		lastEvents[i+1] = eventstore.Event{}
	}

	return &MockSink{
		events:     events,
		lastEvents: lastEvents,
		partitions: partitions,
	}
}

func (s *MockSink) Init() error {
	return nil
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

func (s *MockSink) Events() []eventstore.Event {
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

type slot struct {
	low  uint32
	high uint32
}

func TestMongoListenere(t *testing.T) {
	testcases := []struct {
		name           string
		partitionSlots []slot
	}{
		{
			name: "no_partition",
			partitionSlots: []slot{
				{
					low:  1,
					high: 1,
				},
			},
		},
		{
			name: "two_single_partitions",
			partitionSlots: []slot{
				{
					low:  1,
					high: 1,
				},
				{
					low:  2,
					high: 2,
				},
			},
		},
		{
			name: "two_double_partitions",
			partitionSlots: []slot{
				{
					low:  1,
					high: 2,
				},
				{
					low:  3,
					high: 4,
				},
			},
		},
	}

	for _, tt := range testcases {
		t.Run(tt.name, func(t *testing.T) {
			tearDown, err := tmg.Setup("../docker-compose.yaml")
			if err != nil {
				log.Fatal(err)
			}
			defer tearDown()
			repository, err := mongodb.NewStore(tmg.DBURL, tmg.DBName)
			require.NoError(t, err)
			defer repository.Close(context.Background())

			quit := make(chan os.Signal, 1)
			signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

			partitions := partitionSize(tt.partitionSlots)
			mockSink := NewMockSink(partitions)

			ctx, cancel := context.WithCancel(context.Background())
			feeding(ctx, t, partitions, tt.partitionSlots, mockSink)
			time.Sleep(100 * time.Millisecond)

			es := eventstore.NewEventStore(repository, 3, test.AggregateFactory{}, test.EventFactory{})

			id := uuid.New().String()
			acc := test.CreateAccount("Paulo", id, 100)
			acc.Deposit(10)
			acc.Deposit(20)
			err = es.Save(ctx, acc)
			require.NoError(t, err)

			time.Sleep(100 * time.Millisecond)

			events := mockSink.Events()
			require.Equal(t, 3, len(events), "event size")
			assert.Equal(t, "AccountCreated", events[0].Kind)
			assert.Equal(t, "MoneyDeposited", events[1].Kind)
			assert.Equal(t, "MoneyDeposited", events[2].Kind)

			// cancel current listeners
			cancel()

			// reconnecting
			ctx, cancel = context.WithCancel(context.Background())
			feeding(ctx, t, partitions, tt.partitionSlots, mockSink)
			time.Sleep(200 * time.Millisecond)

			events = mockSink.Events()
			assert.Equal(t, 3, len(events), "event size")

			lastMessages := mockSink.LastMessages()

			acc.Withdraw(5)
			err = es.Save(ctx, acc)
			require.NoError(t, err)

			time.Sleep(200 * time.Millisecond)

			events = mockSink.Events()
			require.Equal(t, 4, len(events), "event size")
			require.Equal(t, "MoneyWithdrawn", events[3].Kind)

			cancel()

			// listening ALL from the beginning
			mockSink = NewMockSink(partitions)

			// connecting
			ctx, cancel = context.WithCancel(context.Background())
			feeding(ctx, t, partitions, tt.partitionSlots, mockSink)
			time.Sleep(200 * time.Millisecond)

			events = mockSink.Events()
			require.Equal(t, 4, len(events), "event size")

			cancel()

			// listening messages from a specific message
			mockSink = NewMockSink(partitions)
			mockSink.SetLastMessages(lastMessages)

			// reconnecting
			ctx, cancel = context.WithCancel(context.Background())
			feeding(ctx, t, partitions, tt.partitionSlots, mockSink)
			time.Sleep(200 * time.Millisecond)

			events = mockSink.Events()
			require.Equal(t, 1, len(events), "event size")

			cancel()
		})
	}
}

func partitionSize(slots []slot) uint32 {
	var partitions uint32
	for _, v := range slots {
		if v.high > partitions {
			partitions = v.high
		}
	}
	return partitions
}

func feeding(ctx context.Context, t *testing.T, partitions uint32, slots []slot, sinker sink.Sinker) {
	for _, v := range slots {
		listener, err := mongodb.NewFeed(tmg.DBURL, tmg.DBName, mongodb.WithPartitions(partitions, v.low, v.high))
		require.NoError(t, err)
		go func() {
			err := listener.Feed(ctx, sinker)
			if err != nil {
				log.Fatalf("Error feeding on #1: %v", faults.Wrap(err))
			}
		}()
		go func() {
			<-ctx.Done()
			time.Sleep(100 * time.Millisecond)
			listener.Close(context.Background())
		}()
	}
}
