package sink

import (
	"context"

	"github.com/quintans/eventstore"
)

type Sinker interface {
	Init() error
	Sink(ctx context.Context, e eventstore.Event) error
	LastMessage(ctx context.Context, partition uint32) (*eventstore.Event, error)
	Close()
}
