package sink

import (
	"context"

	"github.com/quintans/eventsourcing"
)

type Sinker interface {
	Sink(ctx context.Context, e eventsourcing.Event) error
	LastMessage(ctx context.Context, partition uint32) (*eventsourcing.Event, error)
	Close()
}
