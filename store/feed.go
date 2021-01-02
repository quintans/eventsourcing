package store

import (
	"context"
	"sync"

	"github.com/quintans/eventstore/common"
	"github.com/quintans/eventstore/sink"
	"github.com/quintans/faults"
	log "github.com/sirupsen/logrus"
)

type Feeder interface {
	Feed(ctx context.Context, sink sink.Sinker) error
}

type Forwarder struct {
	feeder Feeder
	sinker sink.Sinker
	frozen chan struct{}
	mu     sync.RWMutex
}

func NewForwarder(feeder Feeder, sinker sink.Sinker) *Forwarder {
	return &Forwarder{
		feeder: feeder,
		sinker: sinker,
		frozen: make(chan struct{}),
	}
}

func (f *Forwarder) Run(ctx context.Context) error {
	go func() {
		defer func() {
			f.mu.Lock()
			close(f.frozen)
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

func (f *Forwarder) Cancel() {}

// LastEventIDInSink retrieves the highest event ID and resume token found in the partition range
func LastEventIDInSink(ctx context.Context, sinker sink.Sinker, partitionLow, partitionHi uint32) (afterEventID string, resumeToken []byte, err error) {
	// looking for the highest message ID in all partitions.
	// Sending a message to partitions is done synchronously, so we should start from the last successful sent message.
	if partitionLow == 0 {
		message, err := sinker.LastMessage(ctx, 0)
		if err != nil {
			return "", nil, faults.Errorf("Unable to get the last event ID in sink (unpartitioned): %w", err)
		}
		if message != nil {
			afterEventID = message.ID
			resumeToken = message.ResumeToken
		}
	} else {
		afterEventID = common.MinEventID
		for i := partitionLow; i <= partitionHi; i++ {
			message, err := sinker.LastMessage(ctx, i)
			if err != nil {
				return "", nil, faults.Errorf("Unable to get the last event ID in sink from partition %d: %w", i, err)
			}
			// highest
			if message != nil && message.ID > afterEventID {
				afterEventID = message.ID
				resumeToken = message.ResumeToken
			}
		}
	}

	return
}
