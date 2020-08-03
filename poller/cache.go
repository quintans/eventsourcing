package poller

import (
	"container/list"
	"context"
	"log"
	"sync"

	"github.com/quintans/eventstore/common"
)

// Cache tracks events that were not yet consumed by ALL consumers.
// The gaol is to provide a fast cache for consumer handlers only.
type Cache struct {
	mu        sync.Mutex
	poller    *Poller
	buffer    *list.List
	consumers *list.List
	events    chan common.Event
	waiting   []*LockerChan
}

// LockerChan reusable lock that uses a channel to wait for the release of the lock
type LockerChan struct {
	ch     chan struct{}
	closed bool
	mu     sync.RWMutex
}

// NewLockerChan creates a new LockerChan
func NewLockerChan() *LockerChan {
	return &LockerChan{}
}

// Lock locks the release of wait
func (c *LockerChan) Lock() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.ch = make(chan struct{})
	c.closed = false
}

// Unlock releases the lock
func (c *LockerChan) Unlock() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.closed {
		close(c.ch)
		c.closed = true
	}
}

// Wait wait for the lock to be released
func (c *LockerChan) Wait() <-chan struct{} {
	return c.ch
}

// NewCache creates a cache instance
func NewCache(poller *Poller) *Cache {
	c := Cache{
		poller:    poller,
		buffer:    list.New(),
		consumers: list.New(),
		events:    make(chan common.Event, 1),
	}
	return &c
}

func (c *Cache) next(quit *LockerChan, elem *list.Element) *list.Element {
	quit.Lock()

	c.mu.Lock()

	e := c.innerNext(elem)

	if e != nil {
		c.mu.Unlock()
		return e
	}

	if c.waiting == nil {
		c.waiting = []*LockerChan{}
		go func() {
			// event is zero if channel is closed
			event := <-c.events
			c.mu.Lock()
			if !event.IsZero() {
				c.buffer.PushBack(event)
			}
			for _, v := range c.waiting {
				v.Unlock()
			}
			c.waiting = nil
			c.mu.Unlock()
		}()
	}

	c.waiting = append(c.waiting, quit)
	c.mu.Unlock()
	<-quit.Wait()

	return c.innerNext(elem)
}

func (c *Cache) innerNext(elem *list.Element) *list.Element {
	if elem != nil {
		return elem.Next()
	}
	return c.buffer.Front()
}

// consumed advances the cursor for this consumer
func (c *Cache) consumed(consumer *list.Element, eventID string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// update consumer cursor
	consumer.Value = eventID

	oldest := c.oldestEvent()

	// trim cache
	for e := c.buffer.Front(); e != nil; e = e.Next() {
		evt := e.Value.(common.Event)
		if evt.ID < oldest {
			c.buffer.Remove(e)
		} else {
			break
		}
	}

	return nil
}

func (c *Cache) oldestEvent() string {
	oldest := ""
	for e := c.consumers.Front(); e != nil; e = e.Next() {
		v := e.Value.(string)
		if oldest == "" || v < oldest {
			oldest = v
		}
	}
	return oldest
}

func (c *Cache) registerConsumer(startAt string) *list.Element {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.consumers.PushBack(startAt)
}

func (c *Cache) unregisterConsumer(consumer *list.Element) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.consumers.Remove(consumer)
}

// Consumer creates a consumer for this cache.
func (c *Cache) NewConsumer(name string, handler EventHandler) *CacheConsumer {
	return &CacheConsumer{
		name:    name,
		cache:   c,
		handler: handler,
	}
}

func (c *Cache) Start(ctx context.Context, startAt string) error {
	return c.poller.Handle(ctx, StartAt(startAt), func(ctx2 context.Context, e common.Event) error {
		defer func() {
			if recover() != nil {
				// don't care
			}
		}()
		c.events <- e
		return nil
	})
}

// CacheConsumer cache consumer
type CacheConsumer struct {
	name     string
	cache    *Cache
	consumer *list.Element
	handler  EventHandler
	mu       sync.RWMutex
	quit     *LockerChan
	active   bool
}

// Hold creates a cursor to the most recent event
func (cc *CacheConsumer) Hold() {
	cc.HoldAt("")
}

// HoldAt creates a cursor at a specific event
func (cc *CacheConsumer) HoldAt(startAt string) {
	cc.mu.Lock()
	cc.consumer = cc.cache.registerConsumer(startAt)
	cc.mu.Unlock()
}

// Resume will make the consumer start processing events after the startAt event
func (cc *CacheConsumer) Resume(startAt string) {
	cc.Stop()

	cc.mu.Lock()
	quit := NewLockerChan()
	cc.quit = quit
	cc.mu.Unlock()

	cc.active = true
	for elem := cc.cache.next(quit, nil); elem != nil && cc.active; elem = cc.cache.next(quit, elem) {
		event := elem.Value.(common.Event)
		if event.ID > startAt {
			err := cc.handler(context.Background(), event)
			if err != nil {
				log.Printf("Something went wrong with %s consumer: %v", cc.name, err)
				break
			}
		}
		cc.mu.Lock()
		cc.cache.consumed(cc.consumer, event.ID)
		cc.mu.Unlock()
	}
	cc.cache.unregisterConsumer(cc.consumer)

	cc.mu.Lock()
	cc.consumer = nil
	cc.mu.Unlock()
}

// Start starts consuming events from the current position
func (cc *CacheConsumer) Start() {
	cc.StartAt("")
}

// StartAt starts consuming events from the startAt position
func (cc *CacheConsumer) StartAt(startAt string) {
	cc.HoldAt(startAt)
	cc.Resume(startAt)
}

// Stop stops consuming events
func (cc *CacheConsumer) Stop() {
	cc.mu.Lock()
	defer cc.mu.Unlock()
	if cc.quit != nil {
		cc.quit.Unlock()
	}
	cc.quit = nil
	cc.active = false
}
