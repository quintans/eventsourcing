package store

import (
	"context"
	"fmt"
	"sync"

	"github.com/quintans/eventstore/common"
	"github.com/quintans/eventstore/sink"
	log "github.com/sirupsen/logrus"
)

type Feeder interface {
	Feed(ctx context.Context, sink sink.Sinker, filters ...FilterOption) error
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
			log.Error("Error initialising Sink on boot:", err)
		}

		log.Println("Starting Seed")
		err = f.feeder.Feed(ctx, f.sinker)
		if err != nil {
			log.Error("Error feeding on boot:", err)
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

func LastEventIDInSink(ctx context.Context, sinker sink.Sinker, partitions int) (afterEventID string, resumeToken []byte, err error) {
	// looking for the lowest ID in all partitions
	if partitions == 0 {
		message, err := sinker.LastMessage(ctx, 0)
		if err != nil {
			return "", nil, fmt.Errorf("Unable to get the last event ID in sink (unpartitioned): %w", err)
		}
		if message != nil {
			afterEventID = message.ID
			resumeToken = message.ResumeToken
		}
	} else {
		afterEventID = common.MaxEventID
		for i := 1; i <= partitions; i++ {
			message, err := sinker.LastMessage(ctx, i)
			if err != nil {
				return "", nil, fmt.Errorf("Unable to get the last event ID in sink from partition %d: %w", i, err)
			}
			// lowest
			if message != nil && message.ID < afterEventID {
				afterEventID = message.ID
				resumeToken = message.ResumeToken
			} else {
				afterEventID = common.MinEventID
			}
		}
	}

	return
}
