package poller

import (
	"container/list"
	"context"
	"sync"
	"time"

	"github.com/quintans/eventstore"
	"github.com/quintans/eventstore/player"
	"github.com/quintans/eventstore/repo"
)

// Buffer manager a list of events.
// Attached consumers consume theses events.
// New events are added to the buffer only if requested by a consumer.
type Buffer struct {
	mu        sync.Mutex
	events    *list.List
	consumers *list.List
	eventsCh  chan eventstore.Event
	poller    Poller
	wait      chan struct{}
	drainer   *Consumer
}

func NewBufferedPoller(r player.Repository, options ...Option) *Buffer {
	return NewBuffer(New(r, options...))
}

func NewBuffer(p Poller) *Buffer {
	b := &Buffer{
		events:    list.New(),
		consumers: list.New(),
		eventsCh:  make(chan eventstore.Event, 1),
		poller:    p,
	}
	// when there are no other consumers, the drainer consumer kicks in to move the events forward
	b.drainer = b.NewConsumer("__drainer__", func(ctx context.Context, e eventstore.Event) error {
		return nil
	})
	go b.drainer.Start()
	return b
}

func (n *Buffer) Start(ctx context.Context, pollInterval time.Duration, startAt string, filters ...repo.FilterOption) error {
	return n.poller.Poll(ctx, player.StartAt(startAt), func(ctx2 context.Context, e eventstore.Event) error {
		defer func() {
			if recover() != nil {
				// don't care
			}
		}()
		n.eventsCh <- e
		return nil
	}, filters...)
}

func (n *Buffer) CurrentEvent() eventstore.Event {
	n.mu.Lock()
	defer n.mu.Unlock()
	elem := n.events.Back()
	if elem == nil {
		return eventstore.Event{}
	}
	return elem.Value.(eventstore.Event)
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
		select {
		case evt := <-b.eventsCh:
			n = b.pushEvent(evt)
		default:
		}
	}

	if n == nil {
		if b.wait != nil {
			return nil, b.wait
		}

		b.wait = make(chan struct{})
		// wait for an available event
		go func() {
			evt := <-b.eventsCh

			b.mu.Lock()
			b.pushEvent(evt)
			b.mu.Unlock()
		}()
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

	return n, b.wait
}

func (b *Buffer) pushEvent(evt eventstore.Event) *list.Element {
	e := b.events.PushBack(evt)
	if b.wait != nil {
		close(b.wait)
	}
	b.wait = nil
	return e
}

func (b *Buffer) NewConsumer(name string, handler player.EventHandler, aggregateFilter ...string) *Consumer {
	consu := &Consumer{
		name:            name,
		handler:         handler,
		buffer:          b,
		aggregateFilter: aggregateFilter,
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

	consu.fifo = b.events.Front()
	consu.consumer = b.consumers.PushBack(consu)

	first := b.consumers.Len() == 2
	b.mu.Unlock()

	if first {
		go b.drainer.Stop()
	}
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

	if consu.consumer != nil {
		b.consumers.Remove(consu.consumer)
		consu.consumer = nil
	}
	last := b.consumers.Len() == 0
	b.mu.Unlock()

	if last {
		go b.drainer.Start()
	}
}

type Consumer struct {
	mu              sync.Mutex
	buffer          *Buffer
	name            string
	fifo            *list.Element
	handler         player.EventHandler
	quit            chan struct{}
	consumer        *list.Element
	aggregateFilter []string
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
			if evt.ID > startAt && allowEvent(evt, c.aggregateFilter) {
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
	c.buffer.unregister(c)
	c.mu.Lock()
	defer c.mu.Unlock()
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

func allowEvent(evt eventstore.Event, aggregateTypes []string) bool {
	if len(aggregateTypes) == 0 {
		return true
	}
	for _, v := range aggregateTypes {
		if v == evt.AggregateType {
			return true
		}
	}
	return false
}
