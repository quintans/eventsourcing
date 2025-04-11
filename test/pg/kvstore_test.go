//go:build pg

package pg

import (
	"context"
	"testing"

	"github.com/quintans/eventsourcing/store/postgresql"
	"github.com/quintans/eventsourcing/test"
	"github.com/stretchr/testify/require"
)

func TestKVPutAndGet(t *testing.T) {
	t.Parallel()

	key := "one"

	kvStore, err := postgresql.NewKVStoreWithURL(dbConfig.URL(), test.RandStr("keyvalue"))
	require.NoError(t, err)
	err = kvStore.Put(context.Background(), key, "xyz") // insert
	require.NoError(t, err)
	val, err := kvStore.Get(context.Background(), key)
	require.NoError(t, err)
	require.Equal(t, "xyz", val)

	err = kvStore.Put(context.Background(), key, "abc") // update
	require.NoError(t, err)
	val, err = kvStore.Get(context.Background(), key)
	require.NoError(t, err)
	require.Equal(t, "abc", val)
}
