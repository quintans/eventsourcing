package worker

import (
	"context"

	"github.com/quintans/eventsourcing/log"
)

var _ Balancer = (*NoBalancer)(nil)

type NoBalancer struct {
	name    string
	logger  log.Logger
	member  Memberlister
	workers []Worker
}

func NewNoBalancer(logger log.Logger, name string, member Memberlister, workers []Worker) *MembersBalancer {
	return &MembersBalancer{
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

func (b *NoBalancer) Start(ctx context.Context) <-chan struct{} {
	done := make(chan struct{})
	go func() {
		err := b.run(ctx)
		if err != nil {
			b.logger.Warnf("Error while balancing partitions: %v", err)
		}
		<-ctx.Done()
		b.shutdown()
		close(done)
		return
	}()
	return done
}

func (b *NoBalancer) run(ctx context.Context) error {
	for _, v := range b.workers {
		if !v.IsRunning() {
			v.Start(ctx)
		}
	}
	return b.member.Register(ctx, []string{})
}

func (b *NoBalancer) shutdown() error {
	// shutdown
	for _, w := range b.workers {
		w.Stop(context.Background())
	}
	if err := b.member.Unregister(context.Background()); err != nil {
		b.logger.Warnf("Error while cleaning register on shutdown: %v", err)
	}

	return nil
}
