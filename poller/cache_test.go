package poller

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/quintans/eventstore/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	events1 = []common.Event{
		{ID: "A", AggregateID: "1", AggregateType: "Test", Kind: "Created", Body: []byte(`{"message":"zero"}`)},
		{ID: "B", AggregateID: "1", AggregateType: "Test", Kind: "Updated", Body: []byte(`{"message":"one"}`)},
		{ID: "C", AggregateID: "1", AggregateType: "Test", Kind: "Updated", Body: []byte(`{"message":"two"}`)},
		{ID: "D", AggregateID: "1", AggregateType: "Test", Kind: "Updated", Body: []byte(`{"message":"three"}`)},
	}
	events2 = []common.Event{
		{ID: "E", AggregateID: "1", AggregateType: "Test", Kind: "Updated", Body: []byte(`{"message":"four"}`)},
		{ID: "F", AggregateID: "1", AggregateType: "Test", Kind: "Updated", Body: []byte(`{"message":"five"}`)},
		{ID: "G", AggregateID: "1", AggregateType: "Test", Kind: "Updated", Body: []byte(`{"message":"six"}`)},
		{ID: "H", AggregateID: "1", AggregateType: "Test", Kind: "Updated", Body: []byte(`{"message":"seven"}`)},
	}
	events3 = []common.Event{
		{ID: "I", AggregateID: "1", AggregateType: "Test", Kind: "Updated", Body: []byte(`{"message":"eight"}`)},
		{ID: "J", AggregateID: "1", AggregateType: "Test", Kind: "Updated", Body: []byte(`{"message":"nine"}`)},
		{ID: "K", AggregateID: "1", AggregateType: "Test", Kind: "Updated", Body: []byte(`{"message":"ten"}`)},
		{ID: "L", AggregateID: "1", AggregateType: "Test", Kind: "Updated", Body: []byte(`{"message":"eleven"}`)},
	}
)

type MockRepo struct {
	mu     sync.RWMutex
	events []common.Event
}

func NewMockRepo() *MockRepo {
	return &MockRepo{
		events: events1,
	}
}

func (r *MockRepo) GetLastEventID(ctx context.Context) (string, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.events[len(r.events)-1].ID, nil
}

func (r *MockRepo) GetEvents(ctx context.Context, afterEventID string, limit int, filter common.Filter) ([]common.Event, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	result := []common.Event{}
	for _, v := range r.events {
		if v.ID > afterEventID {
			result = append(result, v)
			if len(result) == limit {
				return result, nil
			}
		}
	}
	return result, nil
}

func (r *MockRepo) SetEvents(events []common.Event) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.events = events
}

func TestSingleConsumer(t *testing.T) {
	r := NewMockRepo()
	p := New(r, WithLimit(2))
	c := NewCache(p)
	count := 0

	fast := c.NewConsumer("single")
	lastID := ""
	go fast.Start("", func(ctx context.Context, e common.Event) error {
		time.Sleep(100)
		count++
		assert.Greater(t, e.ID, lastID)
		lastID = e.ID
		return nil
	})

	ctx, _ := context.WithTimeout(context.Background(), time.Second)
	err := c.Start(ctx, "")
	require.NoError(t, err)

	assert.Equal(t, len(r.events), count, "Consumer Count")
}

func TestBuffer(t *testing.T) {
	r := NewMockRepo()
	p := New(r, WithLimit(2))
	c := NewCache(p)
	fastCount := []string{}
	slowCount := []string{}

	fast := c.NewConsumer("fast")
	lastFastID := ""
	go fast.Start("", func(ctx context.Context, e common.Event) error {
		time.Sleep(100)
		fastCount = append(fastCount, e.ID)
		assert.Greater(t, e.ID, lastFastID, "Fast")
		lastFastID = e.ID
		return nil
	})

	slow := c.NewConsumer("slow")
	lastSlowID := ""
	go slow.Start("", func(ctx context.Context, e common.Event) error {
		time.Sleep(200)
		slowCount = append(slowCount, e.ID)
		assert.Greater(t, e.ID, lastSlowID, "Slow")
		lastSlowID = e.ID
		return nil
	})

	ctx, _ := context.WithTimeout(context.Background(), time.Second)
	err := c.Start(ctx, "")
	require.NoError(t, err)

	assert.Equal(t, len(r.events), len(fastCount), "Fast Count: %s", fastCount)
	assert.Equal(t, len(r.events), len(slowCount), "Slow Count: %s", slowCount)
}

func TestLateConsumer(t *testing.T) {
	r := NewMockRepo()
	p := New(r, WithLimit(2))
	c := NewCache(p)
	firstCount := []string{}
	lateCount := []string{}
	var mu sync.Mutex

	first := c.NewConsumer("first")
	lastFirstID := ""
	go first.Start(events1[1].ID, func(ctx context.Context, e common.Event) error {
		mu.Lock()
		defer mu.Unlock()

		time.Sleep(100)
		firstCount = append(firstCount, e.ID)
		assert.Greater(t, e.ID, lastFirstID, "First")
		lastFirstID = e.ID
		return nil
	})

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		err := c.Start(ctx, "")
		require.NoError(t, err)
	}()

	time.Sleep(time.Second)

	late := c.NewConsumer("late")
	lastLateID := ""
	go late.Start(events2[0].ID, func(ctx context.Context, e common.Event) error {
		mu.Lock()
		defer mu.Unlock()

		time.Sleep(100)
		lateCount = append(lateCount, e.ID)
		assert.Greater(t, e.ID, lastLateID, "Late")
		lastLateID = e.ID
		return nil
	})

	time.Sleep(time.Second)

	r.SetEvents(append(events1, events2...))

	time.Sleep(time.Second)
	cancel()

	mu.Lock()
	defer mu.Unlock()
	assert.Equal(t, len(events1)-2+len(events2), len(firstCount), "First Count: %s", firstCount)
	assert.Equal(t, len(events2)-1, len(lateCount), "Late Count: %s", lateCount)
}
