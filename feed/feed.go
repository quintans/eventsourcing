package feed

import (
	"context"
	"sync"

	"github.com/quintans/eventstore/repo"
	"github.com/quintans/eventstore/sink"
	log "github.com/sirupsen/logrus"
)

type Feeder interface {
	Feed(ctx context.Context, sink sink.Sinker, filters ...repo.FilterOption) error
}

type Feed struct {
	feeder  Feeder
	sinker  sink.Sinker
	release chan struct{}
	mu      sync.RWMutex
}

func New(feeder Feeder, sinker sink.Sinker) *Feed {
	return &Feed{
		feeder: feeder,
		sinker: sinker,
	}
}

func (f *Feed) OnBoot(ctx context.Context) error {
	f.release = make(chan struct{})
	go func() {
		defer func() {
			f.mu.Lock()
			close(f.release)
			f.mu.Unlock()
		}()

		log.Println("Initialising Sink")
		err := f.sinker.Init()
		if err != nil {
			log.Error("Error initialising Sink:", err)
		}

		log.Println("Starting Seed")
		err = f.feeder.Feed(ctx, f.sinker)
		if err != nil {
			log.Error("Error feeding:", err)
		}
	}()
	return nil
}

func (f *Feed) Wait() <-chan struct{} {
	f.mu.Lock()
	defer f.mu.Unlock()

	return f.release
}

func (f *Feed) Cancel() {}
