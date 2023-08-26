//go:build mysql

package mysql

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/quintans/eventsourcing"
	"github.com/quintans/eventsourcing/sink"
	"github.com/quintans/eventsourcing/store/mysql"
	"github.com/quintans/eventsourcing/test"
	"github.com/quintans/eventsourcing/util"
)

type slot struct {
	low  uint32
	high uint32
}

func TestListener(t *testing.T) {
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
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			dbConfig := Setup(t)

			repository, err := mysql.NewStoreWithURL(dbConfig.URL())
			require.NoError(t, err)

			es := eventsourcing.NewEventStore[*test.Account](repository, test.NewJSONCodec(), esOptions)

			cfg := mysql.DBConfig{
				Host:     dbConfig.Host,
				Port:     dbConfig.Port,
				Database: dbConfig.Database,
				Username: dbConfig.Username,
				Password: dbConfig.Password,
			}

			partitions := partitionSize(tt.partitionSlots)
			sinker := test.NewMockSink(partitions)
			ctx, cancel := context.WithCancel(context.Background())
			errs := feeding(ctx, cfg, partitions, tt.partitionSlots, sinker)

			id := util.MustNewULID()
			acc, err := test.CreateAccount("Paulo", id, 100)
			require.NoError(t, err)
			acc.Deposit(10)
			acc.Deposit(20)
			err = es.Create(ctx, acc)
			require.NoError(t, err)

			time.Sleep(5 * time.Second)
			cancel()
			for i := 0; i < len(tt.partitionSlots); i++ {
				require.NoError(t, <-errs, "Error feeding #1: %d", i)
			}

			events := sinker.GetEvents()
			require.Equal(t, 3, len(events), "event size")
			assert.Equal(t, "AccountCreated", events[0].Kind.String())
			assert.Equal(t, "MoneyDeposited", events[1].Kind.String())
			assert.Equal(t, "MoneyDeposited", events[2].Kind.String())

			ctx, cancel = context.WithCancel(context.Background())

			id = util.MustNewULID()
			acc, err = test.CreateAccount("Quintans", id, 100)
			require.NoError(t, err)
			acc.Deposit(30)
			err = es.Create(ctx, acc)
			require.NoError(t, err)

			// resume from the last position, by using the same sinker and a new connection
			errs = feeding(ctx, cfg, partitions, tt.partitionSlots, sinker)

			time.Sleep(5 * time.Second)
			events = sinker.GetEvents()
			require.Equal(t, 5, len(events), "event size")

			cancel()
			for i := 0; i < len(tt.partitionSlots); i++ {
				require.NoError(t, <-errs, "Error feeding #2: %d", i)
			}

			// resume from the beginning
			sinker = test.NewMockSink(0)
			ctx, cancel = context.WithCancel(context.Background())
			errs = feeding(ctx, cfg, partitions, tt.partitionSlots, sinker)

			time.Sleep(5 * time.Second)
			events = sinker.GetEvents()
			require.Equal(t, 5, len(events), "event size")

			cancel()
			for i := 0; i < len(tt.partitionSlots); i++ {
				require.NoError(t, <-errs, "Error feeding #3: %d", i)
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

func feeding(ctx context.Context, dbConfig mysql.DBConfig, partitions uint32, slots []slot, sinker sink.Sinker) chan error {
	errCh := make(chan error, len(slots))
	var wg sync.WaitGroup
	for _, v := range slots {
		listener, err := mysql.NewFeed(logger, dbConfig, sinker, mysql.WithPartitions(partitions, v.low, v.high))
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
