package worker

import (
	"context"
	"sync"

	"github.com/quintans/eventsourcing/log"
)

var _ Balancer = (*NoBalancer)(nil)

type NoBalancer struct {
	name    string
	logger  log.Logger
	member  Memberlister
	workers []Worker
	mu      sync.Mutex
	done    chan struct{}
}

func NewNoBalancer(logger log.Logger, name string, member Memberlister, workers []Worker) *NoBalancer {
	return &NoBalancer{
		name: name,
		logger: logger.WithTags(log.Tags{
			"name": name,
		}),
		member:  member,
		workers: workers,
	}
}

func (b *NoBalancer) Name() string {
	return b.name
}

func (b *NoBalancer) Start(ctx context.Context) {
	done := make(chan struct{})
	b.mu.Lock()
	b.done = done
	b.mu.Unlock()
	go func() {
		err := b.run(ctx)
		if err != nil {
			b.logger.Warnf("Error while balancing %s's partitions: %v", b.name, err)
		}
		select {
		case <-done:
		case <-ctx.Done():
			b.Stop(context.Background(), false)
		}
	}()
}

func (b *NoBalancer) run(ctx context.Context) error {
	for _, v := range b.workers {
		if !v.IsRunning() {
			v.Start(ctx)
		}
	}
	return b.member.Register(ctx, []string{})
}

func (b *NoBalancer) Stop(ctx context.Context, hard bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.done == nil {
		return
	}
	// shutdown
	for _, w := range b.workers {
		w.Stop(ctx, hard)
	}
	if err := b.member.Unregister(context.Background()); err != nil {
		b.logger.Warnf("Error while cleaning register for %s on shutdown: %v", b.name, err)
	}
	close(b.done)
	b.done = nil
}
