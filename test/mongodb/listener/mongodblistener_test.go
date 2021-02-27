package listener

import (
	"context"
	"errors"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/quintans/eventstore"
	"github.com/quintans/eventstore/log"
	"github.com/quintans/eventstore/sink"
	"github.com/quintans/eventstore/store/mongodb"
	"github.com/quintans/eventstore/test"
	tmg "github.com/quintans/eventstore/test/mongodb"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	logger = log.NewLogrus(logrus.StandardLogger())
)

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
			dbConfig, tearDown, err := tmg.Setup("../docker-compose.yaml")
			require.NoError(t, err)
			defer tearDown()

			repository, err := mongodb.NewStore(dbConfig.Url(), dbConfig.Database)
			require.NoError(t, err)
			defer repository.Close(context.Background())

			quit := make(chan os.Signal, 1)
			signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

			partitions := partitionSize(tt.partitionSlots)
			mockSink := test.NewMockSink(partitions)

			ctx, cancel := context.WithCancel(context.Background())
			feeding(ctx, t, dbConfig, partitions, tt.partitionSlots, mockSink)
			time.Sleep(200 * time.Millisecond)

			es := eventstore.NewEventStore(repository, 3, test.AggregateFactory{})

			id := uuid.New().String()
			acc := test.CreateAccount("Paulo", id, 100)
			acc.Deposit(10)
			acc.Deposit(20)
			err = es.Save(ctx, acc)
			require.NoError(t, err)

			time.Sleep(time.Second)

			events := mockSink.GetEvents()
			require.Equal(t, 3, len(events), "event size")
			assert.Equal(t, "AccountCreated", events[0].Kind)
			assert.Equal(t, "MoneyDeposited", events[1].Kind)
			assert.Equal(t, "MoneyDeposited", events[2].Kind)

			// cancel current listeners
			cancel()
			time.Sleep(time.Second)

			// reconnecting
			ctx, cancel = context.WithCancel(context.Background())
			feeding(ctx, t, dbConfig, partitions, tt.partitionSlots, mockSink)
			time.Sleep(200 * time.Millisecond)

			events = mockSink.GetEvents()
			assert.Equal(t, 3, len(events), "event size")

			acc.Withdraw(5)
			acc.Withdraw(10)
			err = es.Save(ctx, acc)
			require.NoError(t, err)

			time.Sleep(time.Second)

			events = mockSink.GetEvents()
			require.Equal(t, 5, len(events), "event size")
			require.Equal(t, "MoneyWithdrawn", events[3].Kind)

			cancel()
			time.Sleep(time.Second)

			// listening ALL from the beginning
			mockSink = test.NewMockSink(partitions)

			// connecting
			ctx, cancel = context.WithCancel(context.Background())
			feeding(ctx, t, dbConfig, partitions, tt.partitionSlots, mockSink)
			time.Sleep(200 * time.Millisecond)

			events = mockSink.GetEvents()
			require.Equal(t, 5, len(events), "event size")

			cancel()
			time.Sleep(time.Second)

			lastMessages := mockSink.LastMessages()
			// listening messages from a specific message
			mockSink = test.NewMockSink(partitions)
			mockSink.SetLastMessages(lastMessages)

			// reconnecting
			ctx, cancel = context.WithCancel(context.Background())
			feeding(ctx, t, dbConfig, partitions, tt.partitionSlots, mockSink)
			time.Sleep(200 * time.Millisecond)

			events = mockSink.GetEvents()
			// when reconnecting the resume token may refer to one or more events (one document - many events)
			// but it is filtered by the event id
			require.Equal(t, 0, len(events), "event size")

			cancel()
			time.Sleep(time.Second)
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

func feeding(ctx context.Context, t *testing.T, dbConfig tmg.DBConfig, partitions uint32, slots []slot, sinker sink.Sinker) {
	var wg sync.WaitGroup
	for _, v := range slots {
		wg.Add(1)
		listener, err := mongodb.NewFeed(logger, dbConfig.Url(), dbConfig.Database, mongodb.WithPartitions(partitions, v.low, v.high))
		require.NoError(t, err)
		go func() {
			wg.Done()
			err := listener.Feed(ctx, sinker)
			if err != nil && !errors.Is(err, context.Canceled) {
				t.Fatalf("Error feeding #1: %v", err)
			}
		}()
	}
	// wait for all goroutines to run
	wg.Wait()
}
