//go:build mongo

package mongodb

import (
	"context"
	"testing"

	"github.com/quintans/eventsourcing/store/mongodb"
	"github.com/stretchr/testify/require"
)

func TestKVPutAndGet(t *testing.T) {
	dbConfig := Setup(t, "./docker-compose.yaml")

	key := "one"

	kvStore, err := mongodb.NewKVStoreWithURI(dbConfig.URL(), dbConfig.Database, "keyvalue")
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
