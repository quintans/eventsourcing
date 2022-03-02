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

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/quintans/eventsourcing"
	"github.com/quintans/eventsourcing/log"
	"github.com/quintans/eventsourcing/sink"
	"github.com/quintans/eventsourcing/store/mongodb"
	"github.com/quintans/eventsourcing/test"
	tmg "github.com/quintans/eventsourcing/test/mongodb"
	"github.com/quintans/eventsourcing/util"
)

var logger = log.NewLogrus(logrus.StandardLogger())

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
			errs := feeding(ctx, dbConfig, partitions, tt.partitionSlots, mockSink)

			es := eventsourcing.NewEventStore(repository, test.NewJSONCodec(), eventsourcing.WithSnapshotThreshold(3))

			id := util.MustNewULID()
			acc, _ := test.CreateAccount("Paulo", id, 100)
			acc.Deposit(10)
			acc.Deposit(20)
			err = es.Create(ctx, acc)
			require.NoError(t, err)

			time.Sleep(500 * time.Millisecond)
			events := mockSink.GetEvents()

			require.Equal(t, 3, len(events), "event size")
			assert.Equal(t, "AccountCreated", events[0].Kind.String())
			assert.Equal(t, "MoneyDeposited", events[1].Kind.String())
			assert.Equal(t, "MoneyDeposited", events[2].Kind.String())

			// cancel current listeners
			cancel()
			for i := 0; i < len(tt.partitionSlots); i++ {
				require.NoError(t, <-errs, "Error feeding #1")
			}

			// reconnecting
			ctx, cancel = context.WithCancel(context.Background())
			errs = feeding(ctx, dbConfig, partitions, tt.partitionSlots, mockSink)

			time.Sleep(time.Second)
			events = mockSink.GetEvents()

			assert.Equal(t, 3, len(events), "event size")

			err = es.Update(ctx, id.String(), func(a eventsourcing.Aggregater) (eventsourcing.Aggregater, error) {
				acc := a.(*test.Account)
				acc.Withdraw(5)
				acc.Withdraw(10)
				return acc, nil
			})
			require.NoError(t, err)

			time.Sleep(500 * time.Millisecond)
			events = mockSink.GetEvents()

			require.Equal(t, 5, len(events), "event size")
			assert.Equal(t, "MoneyWithdrawn", events[3].Kind.String())

			cancel()
			for i := 0; i < len(tt.partitionSlots); i++ {
				require.NoError(t, <-errs, "Error feeding #2")
			}

			// listening ALL from the beginning
			mockSink = test.NewMockSink(partitions)

			// connecting
			ctx, cancel = context.WithCancel(context.Background())
			errs = feeding(ctx, dbConfig, partitions, tt.partitionSlots, mockSink)

			time.Sleep(500 * time.Millisecond)
			events = mockSink.GetEvents()

			require.Equal(t, 5, len(events), "event size")

			cancel()
			for i := 0; i < len(tt.partitionSlots); i++ {
				require.NoError(t, <-errs, "Error feeding #3")
			}

			lastMessages := mockSink.LastMessages()
			// listening messages from a specific message
			mockSink = test.NewMockSink(partitions)
			mockSink.SetLastMessages(lastMessages)

			// reconnecting
			ctx, cancel = context.WithCancel(context.Background())
			errs = feeding(ctx, dbConfig, partitions, tt.partitionSlots, mockSink)

			time.Sleep(500 * time.Millisecond)
			events = mockSink.GetEvents()
			// when reconnecting the resume token may refer to one or more events (one document - many events)
			// but it is filtered by the event id
			require.Equal(t, 0, len(events), "event size")

			cancel()
			for i := 0; i < len(tt.partitionSlots); i++ {
				require.NoError(t, <-errs, "Error feeding #4")
			}
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

func feeding(ctx context.Context, dbConfig tmg.DBConfig, partitions uint32, slots []slot, sinker sink.Sinker) chan error {
	errCh := make(chan error, len(slots))
	var wg sync.WaitGroup
	for _, v := range slots {
		wg.Add(1)
		listener := mongodb.NewFeed(logger, dbConfig.Url(), dbConfig.Database, sinker, mongodb.WithPartitions(partitions, v.low, v.high))
		go func() {
			wg.Done()
			err := listener.Run(ctx)
			if err != nil && !errors.Is(err, context.Canceled) {
				errCh <- err
			} else {
				errCh <- nil
			}
		}()
	}
	// wait for all goroutines to run
	wg.Wait()
	return errCh
}
