//go:build pg

package wal

import (
	"context"
	"errors"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/quintans/eventsourcing"
	"github.com/quintans/eventsourcing/store/postgresql"
	"github.com/quintans/eventsourcing/test"
	tpg "github.com/quintans/eventsourcing/test/pg"
	"github.com/quintans/eventsourcing/util/ids"
	"github.com/quintans/faults"
)

var dbConfig tpg.DBConfig

func TestMain(m *testing.M) {
	dbConfig = setup()

	// run the tests
	code := m.Run()

	// exit with the code from the tests
	os.Exit(code)
}

type slot struct {
	low  uint32
	high uint32
}

func TestListener(t *testing.T) {
	t.Parallel()

	testcases := []struct {
		name       string
		splitSlots []slot
	}{
		{
			name: "no_partition",
			splitSlots: []slot{
				{
					low:  1,
					high: 1,
				},
			},
		},
		{
			name: "two_single_partitions",
			splitSlots: []slot{
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
			splitSlots: []slot{
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
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			eventsTable := test.RandStr("events")
			snapshotsTable := test.RandStr("snapshots")
			repository, err := postgresql.NewStoreWithURL(
				dbConfig.URL(),
				postgresql.WithEventsTable[ids.AggID](eventsTable),
				postgresql.WithSnapshotsTable[ids.AggID](snapshotsTable),
			)
			require.NoError(t, err)

			quit := make(chan os.Signal, 1)
			signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

			es, err := eventsourcing.NewEventStore[*test.Account](repository, test.NewJSONCodec(), eventsourcing.EsSnapshotThreshold(3))
			require.NoError(t, err)

			data := test.NewMockSinkData[ids.AggID]()
			ctx, cancel := context.WithCancel(context.Background())
			err = feeding(ctx, dbConfig, tt.splitSlots, data, eventsTable)
			require.NoError(t, err)

			acc, err := test.NewAccount("Paulo", 100)
			require.NoError(t, err)
			acc.Deposit(10)
			err = es.Create(ctx, acc)
			require.NoError(t, err)
			err = es.Update(ctx, acc.GetID(), func(a *test.Account) (*test.Account, error) {
				a.Withdraw(20)
				return a, nil
			})
			require.NoError(t, err)

			time.Sleep(2 * time.Second)

			// simulating shutdown
			cancel()

			events := data.GetEvents()
			require.Len(t, events, 3, "event size")
			assert.Equal(t, "AccountCreated", events[0].Kind.String())
			assert.Equal(t, "MoneyDeposited", events[1].Kind.String())
			assert.Equal(t, "MoneyWithdrawn", events[2].Kind.String())

			ctx, cancel = context.WithCancel(context.Background())

			acc, err = test.NewAccount("Quintans", 100)
			require.NoError(t, err)
			acc.Deposit(30)
			err = es.Create(ctx, acc)
			require.NoError(t, err)

			// resume from the last position, by using the same sinker and a new connection
			err = feeding(ctx, dbConfig, tt.splitSlots, data, eventsTable)
			require.NoError(t, err)

			time.Sleep(2 * time.Second)
			events = data.GetEvents()
			assert.Len(t, events, 5, "event size")

			cancel()

			// resume from the begginning
			data = test.NewMockSinkData[ids.AggID]()
			ctx, cancel = context.WithCancel(context.Background())
			err = feeding(ctx, dbConfig, tt.splitSlots, data, eventsTable)
			require.NoError(t, err)

			time.Sleep(2 * time.Second)
			events = data.GetEvents()
			assert.Equal(t, 5, len(events), "event size")

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

func feeding(ctx context.Context, dbConfig tpg.DBConfig, slots []slot, data *test.MockSinkData[ids.AggID], coll string) error {
	partitions := partitionSize(slots)

	var wg sync.WaitGroup
	for k, v := range slots {
		mockSink := test.NewMockSink(data, partitions, v.low, v.high)
		listener, err := postgresql.NewFeed[ids.AggID](dbConfig.ReplicationURL(), k+1, len(slots), mockSink, postgresql.WithFeedEventsTable[ids.AggID](coll))
		if err != nil {
			return faults.Wrap(err)
		}

		wg.Add(1)
		go func() {
			wg.Done()
			err := listener.Run(ctx)
			if err != nil && !errors.Is(err, context.Canceled) {
				panic(err)
			}
		}()
	}
	// wait for all goroutines to start
	wg.Wait()
	time.Sleep(2 * time.Second)
	return nil
}
