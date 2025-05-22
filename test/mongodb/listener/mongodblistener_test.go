//go:build mongo

package listener

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/quintans/eventsourcing"
	"github.com/quintans/eventsourcing/store/mongodb"
	"github.com/quintans/eventsourcing/test"
	tmg "github.com/quintans/eventsourcing/test/mongodb"
	"github.com/quintans/eventsourcing/util/ids"
)

var logger = slog.New(slog.NewTextHandler(os.Stdout, nil))

var dbConfig tmg.DBConfig

func TestMain(m *testing.M) {
	dbConfig = tmg.Setup("../docker-compose.yaml")

	// run the tests
	code := m.Run()

	// exit with the code from the tests
	os.Exit(code)
}

type slot struct {
	low  uint32
	high uint32
}

func TestMongoListenere(t *testing.T) {
	t.Parallel()

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
			t.Parallel()

			eventsCollection := test.RandStr(tmg.CollEvents)
			snapshotsCollection := test.RandStr(tmg.CollSnapshots)
			repository, err := mongodb.NewStoreWithURI(
				context.Background(),
				dbConfig.URL(),
				dbConfig.Database,
				mongodb.WithEventsCollection[ids.AggID](eventsCollection),
				mongodb.WithSnapshotsCollection[ids.AggID](snapshotsCollection),
			)
			require.NoError(t, err)
			defer repository.Close(context.Background())

			data := test.NewMockSinkData[ids.AggID]()

			ctx, cancel := context.WithCancel(context.Background())
			errs := feeding(ctx, dbConfig, tt.partitionSlots, data, eventsCollection)

			es, err := eventsourcing.NewEventStore[*test.Account](repository, test.NewJSONCodec(), eventsourcing.EsSnapshotThreshold(3))
			require.NoError(t, err)

			acc, err := test.NewAccount("Paulo", 100)
			require.NoError(t, err)
			id := acc.GetID()
			acc.Deposit(10)
			acc.Deposit(20)
			err = es.Create(ctx, acc)
			require.NoError(t, err)

			time.Sleep(2 * time.Second)

			// cancel current listeners
			cancel()
			for i := 0; i < len(tt.partitionSlots); i++ {
				require.NoError(t, <-errs, "Error feeding #1")
			}

			events := data.GetEvents()

			require.Equal(t, 3, len(events), "event size")
			assert.Equal(t, "AccountCreated", events[0].Kind.String())
			assert.Equal(t, "MoneyDeposited", events[1].Kind.String())
			assert.Equal(t, "MoneyDeposited", events[2].Kind.String())

			// reconnecting
			ctx, cancel = context.WithCancel(context.Background())
			errs = feeding(ctx, dbConfig, tt.partitionSlots, data, eventsCollection)

			time.Sleep(time.Second)
			events = data.GetEvents()

			assert.Equal(t, 3, len(events), "event size")

			err = es.Update(ctx, id, func(acc *test.Account) (*test.Account, error) {
				acc.Withdraw(5)
				acc.Withdraw(10)
				return acc, nil
			})
			require.NoError(t, err)

			time.Sleep(500 * time.Millisecond)
			events = data.GetEvents()

			require.Equal(t, 5, len(events), "event size")
			assert.Equal(t, "MoneyWithdrawn", events[3].Kind.String())

			cancel()
			for i := 0; i < len(tt.partitionSlots); i++ {
				require.NoError(t, <-errs, "Error feeding #2")
			}

			// listening ALL from the beginning
			data = test.NewMockSinkData[ids.AggID]()

			// connecting
			ctx, cancel = context.WithCancel(context.Background())
			errs = feeding(ctx, dbConfig, tt.partitionSlots, data, eventsCollection)

			time.Sleep(500 * time.Millisecond)
			events = data.GetEvents()

			require.Equal(t, 5, len(events), "event size")

			cancel()
			for i := 0; i < len(tt.partitionSlots); i++ {
				require.NoError(t, <-errs, "Error feeding #3")
			}

			lastMessages := data.LastResumes()
			// listening messages from a specific message
			data = test.NewMockSinkData[ids.AggID]()
			data.SetLastResumes(lastMessages)

			// reconnecting
			ctx, cancel = context.WithCancel(context.Background())
			errs = feeding(ctx, dbConfig, tt.partitionSlots, data, eventsCollection)

			time.Sleep(500 * time.Millisecond)
			events = data.GetEvents()
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

func feeding(ctx context.Context, dbConfig tmg.DBConfig, slots []slot, data *test.MockSinkData[ids.AggID], coll string) chan error {
	partitions := partitionSize(slots)

	errCh := make(chan error, len(slots))
	var wg sync.WaitGroup
	for _, v := range slots {
		mockSink := test.NewMockSink(data, partitions, v.low, v.high)
		listener, err := mongodb.NewFeed(logger, dbConfig.URL(), dbConfig.Database, mockSink, mongodb.WithFeedEventsCollection[ids.AggID](coll))
		if err != nil {
			panic(err)
		}

		wg.Add(1)
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
