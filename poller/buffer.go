package poller

import (
	"container/list"
	"context"
	"sync"

	"github.com/quintans/eventstore"
)

// Buffer manager a list of events.
// Attached consumers consume theses events.
// New events are added to the buffer only if requested by a consumer.
type Buffer struct {
	mu        sync.Mutex
	events    *list.List
	consumers *list.List
	eventsCh  chan eventstore.Event
	poller    *Poller
	wait      chan struct{}
}

func NewBuffer(poller *Poller) *Buffer {
	return &Buffer{
		events:    list.New(),
		consumers: list.New(),
		eventsCh:  make(chan eventstore.Event),
		poller:    poller,
	}
}

func (n *Buffer) Start(ctx context.Context, startAt string) error {
	return n.poller.Handle(ctx, StartAt(startAt), func(ctx2 context.Context, e eventstore.Event) error {
		defer func() {
			if recover() != nil {
				// don't care
			}
		}()
		n.eventsCh <- e
		return nil
	})
}

func (b *Buffer) next(e *list.Element) (*list.Element, chan struct{}) {
	b.mu.Lock()
	defer b.mu.Unlock()

	var n *list.Element
	if e == nil {
		n = b.events.Front()
	} else {
		n = e.Next()
	}
	if n == nil {
		if b.wait == nil {
			b.wait = make(chan struct{})

			// wait for an available event
			go func() {
				evt := <-b.eventsCh

				b.mu.Lock()
				b.events.PushBack(evt)
				if b.wait != nil {
					close(b.wait)
				}
				b.wait = nil
				b.mu.Unlock()
			}()
		}

		return nil, b.wait
	}

	// tail event
	tail := ""
	for e := b.consumers.Front(); e != nil; e = e.Next() {
		v := e.Value.(*Consumer)
		eID := v.EventID()
		if tail == "" || eID < tail {
			tail = eID
		}
	}

	// trim buffer
	for e := b.events.Front(); e != nil; e = e.Next() {
		evt := e.Value.(eventstore.Event)
		if evt.ID < tail {
			b.events.Remove(e)
		} else {
			break
		}
	}

	return n, nil
}

func (b *Buffer) NewConsumer(name string, handler EventHandler) *Consumer {
	consu := &Consumer{
		name:    name,
		handler: handler,
		buffer:  b,
	}
	return consu
}

func (c *Buffer) Close() {
	c.mu.Lock()
	defer c.mu.Unlock()
	close(c.eventsCh)
}

func (b *Buffer) register(consu *Consumer) {
	b.mu.Lock()
	defer b.mu.Unlock()
	consu.fifo = b.events.Front()
	consu.consumer = b.consumers.PushBack(consu)
	b.consumers.PushBack(consu)
}

func (b *Buffer) seek(consu *Consumer, startAt string) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if consu.consumer == nil {
		consu.consumer = b.consumers.PushBack(consu)
	}

	e := b.events.Front()

	// skipping forward
	if startAt != "" {
		for elem := e; elem != nil; elem = elem.Next() {
			evt := elem.Value.(eventstore.Event)
			if evt.ID >= startAt {
				e = elem
				break
			}
		}
	}
	consu.fifo = e
}

func (b *Buffer) unregister(consu *Consumer) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if consu.consumer != nil {
		b.consumers.Remove(consu.consumer)
		consu.consumer = nil
	}
}

type Consumer struct {
	mu       sync.Mutex
	buffer   *Buffer
	name     string
	fifo     *list.Element
	handler  EventHandler
	quit     chan struct{}
	consumer *list.Element
}

func (c *Consumer) Name() string {
	return c.name
}

func (c *Consumer) EventID() string {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.fifo == nil {
		return ""
	}
	e := c.fifo.Value.(eventstore.Event)
	return e.ID
}

func (c *Consumer) Attach() {
	c.buffer.register(c)
}

func (c *Consumer) Seek(startAt string) {
	c.buffer.seek(c, startAt)
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
			evt := e.Value.(eventstore.Event)
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
