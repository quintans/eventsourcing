package integration

import (
	"context"
	"sync"

	"github.com/quintans/eventsourcing/store"
	"github.com/quintans/faults"
)

var _ store.KVRStore = (*MockKVStore)(nil)

type KV struct {
	Key, Value string
}

type MockKVStore struct {
	store sync.Map

	mu   sync.Mutex
	puts []KV
}

func (s *MockKVStore) Put(_ context.Context, key, token string) (er error) {
	defer faults.Catch(&er, "MockKVStore.Put(key=%s, token=%s)", key, token)

	if key == "" {
		return faults.New("empty key")
	}

	s.store.Store(key, token)

	s.mu.Lock()
	s.puts = append(s.puts, KV{Key: key, Value: token})
	s.mu.Unlock()

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

func (s *MockKVStore) Data() map[string]string {
	data := map[string]string{}
	s.store.Range(func(key, value any) bool {
		data[key.(string)] = value.(string)
		return true
	})
	return data
}

func (s *MockKVStore) Puts() []KV {
	kvs := []KV{}

	s.mu.Lock()
	for _, v := range s.puts {
		kvs = append(kvs, v)
	}
	s.mu.Unlock()

	return kvs
}
