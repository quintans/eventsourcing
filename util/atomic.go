package util

import "sync"

type Atomic struct {
	mu sync.RWMutex
}

func (a *Atomic) RLock(fn func()) {
	a.mu.RLock()
	defer a.mu.RUnlock()
	fn()
}

func (a *Atomic) Lock(fn func()) {
	a.mu.Lock()
	defer a.mu.Unlock()
	fn()
}

type Channel[T any] struct {
	mu     sync.Mutex
	closed bool
	ch     chan T
}

func NewChannel[T any](size int) *Channel[T] {
	return &Channel[T]{
		ch: make(chan T, size),
	}
}

func (c *Channel[T]) Range(fn func(T)) {
	for x := range c.ch {
		fn(x)
	}
}

func (c *Channel[T]) Push(v T) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return
	}

	c.ch <- v
}

func (c *Channel[T]) Close() {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return
	}
	close(c.ch)
	c.closed = true
}
