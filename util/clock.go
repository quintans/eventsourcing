package util

import "time"

// Clocker defines a logic clock interface
//
// https://medium.com/distributed-knowledge/time-synchronization-in-distributed-systems-a21808928bc8
type Clocker interface {
	Now() time.Time
	// After returns a time where the minimum is t + 1ms
	After(t time.Time) time.Time
}

// Clock implements a logical clock
type Clock struct {
	last time.Time
}

func NewClock() *Clock {
	return NewClockAfter(time.Now())
}

func NewClockAfter(t time.Time) *Clock {
	return &Clock{
		last: t.UTC(),
	}
}

func (c *Clock) Now() time.Time {
	t := time.Now().UTC()
	t = max(t, c.last)
	c.last = t
	return t
}

func (c *Clock) After(after time.Time) time.Time {
	t := time.Now().UTC()
	t = max(t, c.last)
	t = max(t, after)
	c.last = t
	return t
}

func max(t, last time.Time) time.Time {
	// due to clock skews, 't' can be less or equal than the last update
	// so we make sure that it will be at least 1ms after.
	if t.UnixMilli() <= last.UnixMilli() {
		t = last.Add(time.Millisecond)
	}
	// we only need millisecond precision
	t = t.Truncate(time.Millisecond)
	return t
}
