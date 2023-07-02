package test

import (
	"bytes"
	"context"
	"strconv"
	"sync"
	"time"

	"github.com/oklog/ulid/v2"

	"github.com/quintans/eventsourcing"
	"github.com/quintans/eventsourcing/encoding"
	"github.com/quintans/eventsourcing/eventid"
	"github.com/quintans/eventsourcing/projection"
	"github.com/quintans/eventsourcing/sink"
	"github.com/quintans/eventsourcing/store"
	"github.com/quintans/eventsourcing/worker"
	"github.com/quintans/faults"
)

var _ projection.Repository = (*InMemDB)(nil)

type InMemDB struct {
	mu      sync.RWMutex
	events  []*eventsourcing.Event
	cursor  *Cursor
	entropy *ulid.MonotonicEntropy
}

func NewInMemDB() *InMemDB {
	return &InMemDB{
		entropy: eventid.EntropyFactory(time.Now()),
	}
}

func (db *InMemDB) Add(event *eventsourcing.Event) *eventsourcing.Event {
	db.mu.Lock()
	defer db.mu.Unlock()

	event.ID = NewID(db.entropy)
	event.CreatedAt = time.Now().UTC()
	db.events = append(db.events, event)

	// signal all cursors of a change
	c := db.cursor
	for c != nil {
		select {
		case c.changed <- true:
		default:
		}
		c = c.next
	}

	return event
}

func NewID(entropy *ulid.MonotonicEntropy) eventid.EventID {
	now := time.Now()
	return eventid.MustNew(now, entropy)
}

func (db *InMemDB) GetFrom(id eventid.EventID) []*eventsourcing.Event {
	db.mu.RLock()
	defer db.mu.RUnlock()

	found := -1
	for k, v := range db.events {
		if v.ID.Compare(id) == 0 {
			found = k
			break
		}
	}
	if found == -1 {
		return nil
	}
	size := len(db.events)
	events := make([]*eventsourcing.Event, size-found)
	for k := found; k < size; k++ {
		events[k-found] = db.events[k]
	}
	return events
}

func (db *InMemDB) WatchAfter(resumeToken []byte) (*Cursor, error) {
	after := -1
	if len(resumeToken) > 0 {
		idx, err := strconv.Atoi(string(resumeToken))
		if err != nil {
			return nil, err
		}
		after = idx
	}
	cursor := &Cursor{
		db:      db,
		lastPos: after,
		changed: make(chan bool, 1),
	}

	db.mu.Lock()
	if db.cursor != nil {
		db.cursor.previous = cursor
		cursor.next = db.cursor
	}
	db.cursor = cursor
	db.mu.Unlock()
	return cursor, nil
}

func (db *InMemDB) RemoveCursor(c *Cursor) {
	db.mu.Lock()
	defer db.mu.Unlock()
	if c.next != nil {
		c.next.previous = c.previous
	}
	if c.previous != nil {
		c.previous.next = c.next
	}
}

func (db *InMemDB) ReadAt(idx int) (*eventsourcing.Event, bool) {
	db.mu.RLock()
	defer db.mu.RUnlock()
	if idx >= len(db.events) {
		return nil, false
	}
	return db.events[idx], true
}

func (db *InMemDB) GetMaxSeq(ctx context.Context, filter store.Filter) (uint64, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	var maxSeq uint64
	for _, v := range db.events {
		if v.Sequence > maxSeq {
			maxSeq = v.Sequence
		}
	}

	return maxSeq, nil
}

func (db *InMemDB) GetEvents(ctx context.Context, afterSeq uint64, limit int, filter store.Filter) ([]*eventsourcing.Event, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	var events []*eventsourcing.Event
	for _, v := range db.events {
		if v.Sequence > afterSeq {
			events = append(events, v)
		}
		if len(events) == limit {
			return events, nil
		}
	}
	return events, nil
}

func (db *InMemDB) SetSinkSeq(ctx context.Context, id eventid.EventID, seq uint64) error {
	db.mu.RLock()
	defer db.mu.RUnlock()

	for _, v := range db.events {
		if v.ID.Compare(id) == 0 {
			v.Sequence = seq
			return nil
		}
	}

	return faults.Errorf("unknown sequence: %s", id)
}

type Cursor struct {
	next     *Cursor
	previous *Cursor

	mu      sync.Mutex
	db      *InMemDB
	lastPos int
	changed chan bool
}

func (c *Cursor) Next() (*eventsourcing.Event, <-chan bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	e, ok := c.db.ReadAt(c.lastPos + 1)
	if !ok {
		return nil, c.changed
	}
	c.lastPos++
	return e, nil
}

func (c *Cursor) Close() {
	c.db.RemoveCursor(c)
}

var _ worker.Tasker = (*InMemDBFeed)(nil)

type InMemDBFeed struct {
	db     *InMemDB
	sinker sink.Sinker
}

func InMemDBNewFeed(db *InMemDB, sinker sink.Sinker) worker.Tasker {
	return InMemDBFeed{
		db:     db,
		sinker: sinker,
	}
}

func (f InMemDBFeed) Run(ctx context.Context) error {
	var lastResumeToken []byte
	err := store.ForEachSequenceInSinkPartitions(ctx, f.sinker, 0, 0, func(_ uint64, message *sink.Message) error {
		if bytes.Compare(message.ResumeToken, lastResumeToken) > 0 {
			lastResumeToken = message.ResumeToken
		}
		return nil
	})
	if err != nil {
		return err
	}

	cursor, err := f.db.WatchAfter(lastResumeToken)
	if err != nil {
		return err
	}

	for {
		e, change := cursor.Next()
		if change == nil {
			select {
			case <-ctx.Done():
				return nil
			default:
			}
			seq, err := f.sinker.Sink(ctx, e, sink.Meta{ResumeToken: encoding.Base64(e.ID.String())})
			if err != nil {
				return err
			}

			err = f.db.SetSinkSeq(ctx, e.ID, seq)
			if err != nil {
				return err
			}

		} else {
			select {
			case <-ctx.Done():
				return nil
			case <-cursor.changed:
			}
		}
	}
}

func (f InMemDBFeed) Cancel(ctx context.Context, hard bool) {}
