package poller

import (
	"container/list"
	"context"
	"sync"

	"github.com/quintans/eventstore/common"
)

// Buffer manager a list of events.
// Attached consumers consume theses events.
// New events are added to the buffer only if requested by a consumer.
type Buffer struct {
	mu        sync.Mutex
	events    *list.List
	consumers *list.List
	eventsCh  chan common.Event
	poller    *Poller
	wait      chan struct{}
}

func NewBuffer(poller *Poller) *Buffer {
	return &Buffer{
		events:    list.New(),
		consumers: list.New(),
		eventsCh:  make(chan common.Event),
		poller:    poller,
	}
}

func (c *Buffer) Start(ctx context.Context, startAt string) error {
	return c.poller.Handle(ctx, StartAt(startAt), func(ctx2 context.Context, e common.Event) error {
		defer func() {
			if recover() != nil {
				// don't care
			}
		}()
		c.eventsCh <- e
		return nil
	})
}

func (c *Buffer) next(e *list.Element) (*list.Element, chan struct{}) {
	c.mu.Lock()
	defer c.mu.Unlock()

	var n *list.Element
	if e == nil {
		n = c.events.Front()
	} else {
		n = e.Next()
	}
	if n == nil {
		if c.wait == nil {
			c.wait = make(chan struct{})

			// wait for an available event
			go func() {
				evt := <-c.eventsCh

				c.mu.Lock()
				c.events.PushBack(evt)
				if c.wait != nil {
					close(c.wait)
				}
				c.wait = nil
				c.mu.Unlock()
			}()
		}

		return nil, c.wait
	}

	// tail event
	tail := ""
	for e := c.consumers.Front(); e != nil; e = e.Next() {
		v := e.Value.(*Consumer)
		eID := v.EventID()
		if tail == "" || eID < tail {
			tail = eID
		}
	}

	// trim buffer
	for e := c.events.Front(); e != nil; e = e.Next() {
		evt := e.Value.(common.Event)
		if evt.ID < tail {
			c.events.Remove(e)
		} else {
			break
		}
	}

	return n, nil
}

func (c *Buffer) NewConsumer(name string, handler EventHandler) *Consumer {
	consu := &Consumer{
		name:    name,
		handler: handler,
		buffer:  c,
	}
	return consu
}

func (c *Buffer) Close() {
	c.mu.Lock()
	defer c.mu.Unlock()
	close(c.eventsCh)
}

func (c *Buffer) register(consu *Consumer) {
	c.mu.Lock()
	defer c.mu.Unlock()
	consu.fifo = c.events.Front()
	consu.consumer = c.consumers.PushBack(consu)
	c.consumers.PushBack(consu)
}

func (c *Buffer) unregister(consu *Consumer) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if consu.consumer != nil {
		c.consumers.Remove(consu.consumer)
		consu.consumer = nil
	}
}

type Consumer struct {
	mu       sync.Mutex
	buffer   *Buffer
	name     string
	startAt  string
	fifo     *list.Element
	handler  EventHandler
	quit     chan struct{}
	consumer *list.Element
}

func (c *Consumer) EventID() string {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.fifo == nil {
		return ""
	}
	e := c.fifo.Value.(common.Event)
	return e.ID
}

func (c *Consumer) Attach() {
	c.buffer.register(c)
}

func (c *Consumer) Start() {
	c.Resume("")
}

func (c *Consumer) Resume(startAt string) {
	c.mu.Lock()
	if c.consumer == nil {
		c.Attach()
	}
	var quit chan struct{}
	if c.quit == nil {
		quit = make(chan struct{})
		c.quit = quit
	} else {
		quit = c.quit
	}
	c.mu.Unlock()

	for {
		e, wait := c.buffer.next(c.fifo)
		// it is only nil when closing
		if e == nil {
			select {
			case <-wait:
			case <-quit:
				return
			}
		} else {
			evt := e.Value.(common.Event)
			if evt.ID > startAt {
				c.handler(context.Background(), evt)
			}
			c.mu.Lock()
			c.fifo = e
			c.mu.Unlock()
		}
	}
}

// Stop stops collecting
func (c *Consumer) Stop() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.buffer.unregister(c)
	if c.quit != nil {
		close(c.quit)
		c.quit = nil
	}
}

func (c *Consumer) IsActive() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.quit == nil
}
