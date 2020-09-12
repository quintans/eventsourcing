package poller

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/quintans/eventstore"
	"github.com/quintans/eventstore/player"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	events1 = []eventstore.Event{
		{ID: "A", AggregateID: "1", AggregateType: "Test", Kind: "Created", Body: []byte(`{"message":"zero"}`)},
		{ID: "B", AggregateID: "1", AggregateType: "Test", Kind: "Updated", Body: []byte(`{"message":"one"}`)},
		{ID: "C", AggregateID: "1", AggregateType: "Test", Kind: "Updated", Body: []byte(`{"message":"two"}`)},
		{ID: "D", AggregateID: "1", AggregateType: "Test", Kind: "Updated", Body: []byte(`{"message":"three"}`)},
	}
	events2 = []eventstore.Event{
		{ID: "E", AggregateID: "1", AggregateType: "Test", Kind: "Updated", Body: []byte(`{"message":"four"}`)},
		{ID: "F", AggregateID: "1", AggregateType: "Test", Kind: "Updated", Body: []byte(`{"message":"five"}`)},
		{ID: "G", AggregateID: "1", AggregateType: "Test", Kind: "Updated", Body: []byte(`{"message":"six"}`)},
		{ID: "H", AggregateID: "1", AggregateType: "Test", Kind: "Updated", Body: []byte(`{"message":"seven"}`)},
	}
	events3 = []eventstore.Event{
		{ID: "I", AggregateID: "1", AggregateType: "Test", Kind: "Updated", Body: []byte(`{"message":"eight"}`)},
		{ID: "J", AggregateID: "1", AggregateType: "Test", Kind: "Updated", Body: []byte(`{"message":"nine"}`)},
		{ID: "K", AggregateID: "1", AggregateType: "Test", Kind: "Updated", Body: []byte(`{"message":"ten"}`)},
		{ID: "L", AggregateID: "1", AggregateType: "Test", Kind: "Updated", Body: []byte(`{"message":"eleven"}`)},
	}
)

const pollInterval = 200 * time.Millisecond

type MockRepo struct {
	mu     sync.RWMutex
	events []eventstore.Event
}

func NewMockRepo() *MockRepo {
	return &MockRepo{
		events: events1,
	}
}

func (r *MockRepo) GetLastEventID(ctx context.Context, filter player.Filter) (string, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.events[len(r.events)-1].ID, nil
}

func (r *MockRepo) GetEvents(ctx context.Context, afterEventID string, limit int, filter player.Filter) ([]eventstore.Event, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	result := []eventstore.Event{}
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

func (r *MockRepo) AddEvents(events []eventstore.Event) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.events = append(r.events, events...)
}

func TestSingleConsumer(t *testing.T) {
	t.Parallel()

	r := NewMockRepo()
	c := NewBufferedPoller(r, WithLimit(2))

	time.Sleep(100 * time.Millisecond)

	var mu sync.Mutex
	ids := []string{}
	lastID := ""
	single := c.NewConsumer("single", func(ctx context.Context, e eventstore.Event) error {
		mu.Lock()
		defer mu.Unlock()
		assert.Greater(t, e.ID, lastID)
		lastID = e.ID
		ids = append(ids, e.ID)
		return nil
	})
	go single.Start()

	time.Sleep(100 * time.Millisecond)

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		err := c.Start(ctx, pollInterval, "")
		require.NoError(t, err)
	}()

	time.Sleep(100 * time.Millisecond)

	single.Stop()

	time.Sleep(100 * time.Millisecond)

	r.AddEvents(events2)

	time.Sleep(100 * time.Millisecond)
	cancel()
	time.Sleep(100 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()
	assert.Equal(t, []string{"A", "B", "C", "D"}, ids, "Consumer IDs", ids)

	assert.Equal(t, "H", c.CurrentEvent().ID, "Current ID")
}

func TestBuffer(t *testing.T) {
	t.Parallel()

	for i := 0; i < 10; i++ {
		r := NewMockRepo()
		p := New(r, WithLimit(2))
		c := NewBuffer(p)
		fastIDs := []string{}
		slowIDs := []string{}

		lastFastID := ""
		fast := c.NewConsumer("fast", func(ctx context.Context, e eventstore.Event) error {
			time.Sleep(10 * time.Millisecond)
			fastIDs = append(fastIDs, e.ID)
			require.Greater(t, e.ID, lastFastID, "Fast %d - %s > %s", i, e.ID, lastFastID)
			lastFastID = e.ID
			return nil
		})
		go fast.Start()

		lastSlowID := ""
		slow := c.NewConsumer("slow", func(ctx context.Context, e eventstore.Event) error {
			time.Sleep(20 * time.Millisecond)
			slowIDs = append(slowIDs, e.ID)
			require.Greater(t, e.ID, lastSlowID, "Slow %d - %s > %s", i, e.ID, lastSlowID)
			lastSlowID = e.ID
			return nil
		})
		go slow.Start()

		time.Sleep(100 * time.Millisecond)

		ctx, _ := context.WithTimeout(context.Background(), 500*time.Millisecond)
		err := c.Start(ctx, pollInterval, "")
		require.NoError(t, err)

		time.Sleep(100 * time.Millisecond)

		assert.Equal(t, []string{"A", "B", "C", "D"}, fastIDs, "Fast: %d - %s", i, fastIDs)
		require.Equal(t, []string{"A", "B", "C", "D"}, slowIDs, "Slow: %d - %s", i, slowIDs)
	}
}

func TestSingleLateConsumer(t *testing.T) {
	t.Parallel()

	r := NewMockRepo()
	p := New(r, WithLimit(2))
	c := NewBuffer(p)
	var mu sync.Mutex

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		err := c.Start(ctx, pollInterval, "")
		require.NoError(t, err)
	}()

	time.Sleep(50 * time.Millisecond)

	ids := []string{}
	lastLateID := ""
	late := c.NewConsumer("late", func(ctx context.Context, e eventstore.Event) error {
		mu.Lock()
		defer mu.Unlock()

		assert.Greater(t, e.ID, lastLateID, "Late")
		lastLateID = e.ID
		ids = append(ids, e.ID)
		return nil
	})
	go late.Resume(events2[0].ID)

	time.Sleep(100 * time.Millisecond)

	r.AddEvents(events2)

	time.Sleep(time.Second)
	cancel()

	time.Sleep(100 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()
	assert.Equal(t, []string{"F", "G", "H"}, ids, "IDs: %s", ids)
}

func TestLateConsumer(t *testing.T) {
	t.Parallel()

	r := NewMockRepo()
	p := New(r, WithLimit(2))
	c := NewBuffer(p)
	firstIDs := []string{}
	lateIDs := []string{}
	var mu sync.Mutex

	lastFirstID := ""
	first := c.NewConsumer("first", func(ctx context.Context, e eventstore.Event) error {
		mu.Lock()
		defer mu.Unlock()

		time.Sleep(100 * time.Millisecond)
		firstIDs = append(firstIDs, e.ID)
		assert.Greater(t, e.ID, lastFirstID, "First")
		lastFirstID = e.ID
		return nil
	})
	go first.Resume(events1[1].ID)

	time.Sleep(100 * time.Millisecond)

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		err := c.Start(ctx, pollInterval, "")
		require.NoError(t, err)
	}()

	time.Sleep(100 * time.Millisecond)

	lastLateID := ""
	late := c.NewConsumer("late", func(ctx context.Context, e eventstore.Event) error {
		mu.Lock()
		defer mu.Unlock()

		time.Sleep(100 * time.Millisecond)
		lateIDs = append(lateIDs, e.ID)
		assert.Greater(t, e.ID, lastLateID, "Late")
		lastLateID = e.ID
		return nil
	})
	go late.Resume(events2[0].ID)

	time.Sleep(100 * time.Millisecond)

	r.AddEvents(events2)

	time.Sleep(time.Second)
	cancel()

	time.Sleep(100 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()
	assert.Equal(t, []string{"C", "D", "E", "F", "G", "H"}, firstIDs, "First: %s", firstIDs)
	assert.Equal(t, []string{"F", "G", "H"}, lateIDs, "Late: %s", lateIDs)
}

func TestStopConsumer(t *testing.T) {
	t.Parallel()

	r := NewMockRepo()
	p := New(r, WithLimit(2))
	c := NewBuffer(p)
	var mu sync.Mutex

	firstIDs := []string{}
	lastFirstID := ""
	first := c.NewConsumer("first", func(ctx context.Context, e eventstore.Event) error {
		mu.Lock()
		defer mu.Unlock()

		time.Sleep(100 * time.Millisecond)
		assert.Greater(t, e.ID, lastFirstID, "First")
		lastFirstID = e.ID
		firstIDs = append(firstIDs, e.ID)
		return nil
	})
	go first.Resume(events1[1].ID)

	lateIDs := []string{}
	lastLateID := ""
	late := c.NewConsumer("late", func(ctx context.Context, e eventstore.Event) error {
		mu.Lock()
		defer mu.Unlock()

		time.Sleep(100 * time.Millisecond)
		assert.Greater(t, e.ID, lastLateID, "Late")
		lastLateID = e.ID
		lateIDs = append(lateIDs, e.ID)
		return nil
	})
	go late.Start()

	time.Sleep(100 * time.Millisecond)

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		err := c.Start(ctx, pollInterval, "")
		require.NoError(t, err)
	}()

	time.Sleep(time.Second)

	late.Stop()

	time.Sleep(time.Second)

	r.AddEvents(events2)

	time.Sleep(time.Second)
	cancel()

	time.Sleep(500 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()
	assert.Equal(t, []string{"C", "D", "E", "F", "G", "H"}, firstIDs, "First IDs: %s", firstIDs)
	assert.Equal(t, []string{"A", "B", "C", "D"}, lateIDs, "Late IDs: %s", lateIDs)
}

func TestRestartSingleConsumer(t *testing.T) {
	t.Parallel()

	r := NewMockRepo()
	p := New(r, WithLimit(2))
	c := NewBuffer(p)
	count := []string{}
	var mu sync.Mutex

	ids := []string{}
	lastID := ""
	single := c.NewConsumer("single", func(ctx context.Context, e eventstore.Event) error {
		mu.Lock()
		defer mu.Unlock()

		count = append(count, e.ID)
		assert.Greater(t, e.ID, lastID, "Second")
		lastID = e.ID
		ids = append(ids, e.ID)
		return nil
	})
	go single.Start()

	time.Sleep(100 * time.Millisecond)

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		err := c.Start(ctx, pollInterval, "")
		require.NoError(t, err)
	}()

	time.Sleep(100 * time.Millisecond)

	single.Stop()

	time.Sleep(100 * time.Millisecond)

	mu.Lock()
	assert.Equal(t, len(events1), len(count), "Count: %s", count)
	mu.Unlock()

	time.Sleep(100 * time.Millisecond)
	r.AddEvents(events2)

	time.Sleep(time.Second)

	go single.Resume(events3[0].ID)
	time.Sleep(100 * time.Millisecond)

	r.AddEvents(events3)

	time.Sleep(3 * time.Second)

	cancel()

	mu.Lock()
	assert.Equal(t, []string{"A", "B", "C", "D", "J", "K", "L"}, ids, "IDs: %s", ids)
	mu.Unlock()
}

func TestRestartConsumer(t *testing.T) {
	t.Parallel()

	r := NewMockRepo()
	p := New(r, WithLimit(2))
	c := NewBuffer(p)
	var mu sync.Mutex

	firstIDs := []string{}
	lastFirstID := ""
	first := c.NewConsumer("first", func(ctx context.Context, e eventstore.Event) error {
		mu.Lock()
		defer mu.Unlock()

		assert.Greater(t, e.ID, lastFirstID, "First")
		lastFirstID = e.ID
		firstIDs = append(firstIDs, e.ID)
		return nil
	})
	go first.Resume(events1[1].ID)

	secondIDs := []string{}
	lastSecondID := ""
	second := c.NewConsumer("second", func(ctx context.Context, e eventstore.Event) error {
		mu.Lock()
		defer mu.Unlock()

		assert.Greater(t, e.ID, lastSecondID, "Second")
		lastSecondID = e.ID
		secondIDs = append(secondIDs, e.ID)
		return nil
	})
	go second.Start()

	time.Sleep(100 * time.Millisecond)

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		err := c.Start(ctx, pollInterval, "")
		require.NoError(t, err)
	}()

	time.Sleep(100 * time.Millisecond)

	second.Stop()

	time.Sleep(100 * time.Millisecond)

	second.Attach()
	r.AddEvents(events2)

	time.Sleep(time.Second)

	go second.Resume(events3[0].ID)
	r.AddEvents(events3)

	time.Sleep(500 * time.Millisecond)

	cancel()

	mu.Lock()
	defer mu.Unlock()
	assert.Equal(t, []string{"C", "D", "E", "F", "G", "H", "I", "J", "K", "L"}, firstIDs, "First IDs: %s", firstIDs)
	assert.Equal(t, []string{"A", "B", "C", "D", "J", "K", "L"}, secondIDs, "Second IDs: %s", secondIDs)
}
