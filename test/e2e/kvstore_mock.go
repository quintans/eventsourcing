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

func (s *MockKVStore) Put(_ context.Context, key, token string) (er error) {
	defer faults.Catch(&er, "MockKVStore.Put(key=%s, token=%s)", key, token)

	if key == "" {
		return faults.New("empty key")
	}

	s.store.Store(key, token)
	return nil
}

func (s *MockKVStore) Get(_ context.Context, key string) (_ string, er error) {
	defer faults.Catch(&er, "MockKVStore.Get(key=%s)", key)

	if key == "" {
		return "", faults.New("empty key")
	}

	v, ok := s.store.Load(key)
	if !ok {
		return "", faults.Wrap(store.ErrResumeTokenNotFound)
	}
	return v.(string), nil
}
