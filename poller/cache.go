package poller

import (
	"container/list"
	"context"
	"fmt"
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
	waiting   bool
	cond      *sync.Cond
}

// NewCache creates a cache instance
func NewCache(poller *Poller) *Cache {
	c := Cache{
		poller:    poller,
		buffer:    list.New(),
		consumers: list.New(),
		events:    make(chan common.Event, 1),
	}
	c.cond = sync.NewCond(&c.mu)
	return &c
}

func (c *Cache) next(elem *list.Element) *list.Element {
	c.cond.L.Lock()
	defer c.cond.L.Unlock()

	e := c.innerNext(elem)

	if e != nil {
		return e
	}

	if !c.waiting {
		c.waiting = true
		go func() {
			// event is zero if channel is closed
			event := <-c.events
			c.cond.L.Lock()
			if !event.IsZero() {
				fmt.Println("===> Pushing", event.ID)
				c.buffer.PushBack(event)
			}
			c.waiting = false
			c.cond.L.Unlock()
			c.cond.Broadcast()
		}()
	}

	c.cond.Wait()

	e = c.innerNext(elem)
	fmt.Printf("===> Nexting %+v\n", e)
	return e
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
func (c *Cache) NewConsumer(name string) *CacheConsumer {
	return &CacheConsumer{
		name:  name,
		cache: c,
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
	active   bool
	mu       sync.RWMutex
}

// Start starts consuming events
func (cc *CacheConsumer) Start(startAt string, handler EventHandler) {
	cc.setActive(true)
	cc.consumer = cc.cache.registerConsumer(startAt)
	for elem := cc.cache.next(nil); elem != nil && cc.isActive(); elem = cc.cache.next(elem) {
		event := elem.Value.(common.Event)
		if event.ID <= startAt {
			continue
		}
		err := handler(context.Background(), event)
		if err != nil {
			log.Printf("Something went wrong with  of %s consumer: %v", cc.name, err)
			break
		}
		cc.cache.consumed(cc.consumer, event.ID)
	}
	cc.cache.unregisterConsumer(cc.consumer)
	cc.consumer = nil
}

// Stop stops consuming events
func (cc *CacheConsumer) Stop() {
	cc.setActive(false)
}

func (cc *CacheConsumer) setActive(active bool) {
	cc.mu.Lock()
	defer cc.mu.Unlock()
	cc.active = active
}

func (cc *CacheConsumer) isActive() bool {
	cc.mu.RLock()
	defer cc.mu.RUnlock()

	return cc.active
}
