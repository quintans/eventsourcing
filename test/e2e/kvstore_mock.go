package e2e

import (
	"context"
	"sync"

	"github.com/quintans/eventsourcing/store"
	"github.com/quintans/faults"
)

var _ store.KVRStore = (*MockKVStore)(nil)

type MockKVStore struct {
	store sync.Map
}

func (s *MockKVStore) Put(ctx context.Context, key string, token string) error {
	s.store.Store(key, token)
	return nil
}

func (s *MockKVStore) Get(ctx context.Context, key string) (_ string, e error) {
	defer faults.Catch(&e, "MockKVStore.Get(key=%s)", key)

	v, ok := s.store.Load(key)
	if !ok {
		return "", faults.Wrap(store.ErrResumeTokenNotFound)
	}
	return v.(string), nil
}
