package feed

import (
	"context"

	"github.com/quintans/eventstore/player"
	"github.com/quintans/eventstore/sink"
	log "github.com/sirupsen/logrus"
)

type Feeder interface {
	Feed(ctx context.Context, sink sink.Sinker, filters ...player.FilterOption) error
}

type Feed struct {
	feeder Feeder
	sinker sink.Sinker
}

func New(feeder Feeder, sinker sink.Sinker) Feed {
	return Feed{
		feeder: feeder,
		sinker: sinker,
	}
}

func (f Feed) OnBoot(ctx context.Context) error {
	go func() {
		log.Println("Starting Polling feed")
		f.feeder.Feed(ctx, f.sinker)
	}()
	return nil
}

func (f Feed) Wait() <-chan struct{} {
	return nil
}

func (f Feed) Cancel() {}
