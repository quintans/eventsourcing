package store

import (
	"context"

	"github.com/quintans/eventstore/sink"
	"github.com/quintans/faults"
	log "github.com/sirupsen/logrus"
)

type Feeder interface {
	Feed(ctx context.Context, sink sink.Sinker) error
}

type Forwarder struct {
	name   string
	feeder Feeder
	sinker sink.Sinker
}

func NewForwarder(name string, feeder Feeder, sinker sink.Sinker) *Forwarder {
	return &Forwarder{
		name:   name,
		feeder: feeder,
		sinker: sinker,
	}
}

func (f *Forwarder) Run(ctx context.Context) error {
	log.Printf("Initialising Sink '%s'", f.name)
	defer func() {
		f.sinker.Close()
	}()

	log.Printf("Starting Seed '%s'", f.name)
	err := f.feeder.Feed(ctx, f.sinker)
	if err != nil {
		return faults.Errorf("Error feeding '%s' on boot: %w", f.name, err)
	}
	return nil
}

func (f *Forwarder) Cancel() {
	f.sinker.Close()
}

// ForEachResumeTokenInSinkPartitions retrieves the highest event ID and resume token found in the partition range
func ForEachResumeTokenInSinkPartitions(ctx context.Context, sinker sink.Sinker, partitionLow, partitionHi uint32, forEach func(resumeToken []byte) error) error {
	if partitionLow == 0 {
		partitionHi = 0
	}

	// looking for the highest message ID in all partitions.
	// Sending a message to partitions is done synchronously, so we should start from the last successful sent message.
	for i := partitionLow; i <= partitionHi; i++ {
		message, err := sinker.LastMessage(ctx, i)
		if err != nil {
			return faults.Errorf("Unable to get the last event ID in sink from partition %d: %w", i, err)
		}
		// highest
		if message != nil && len(message.ResumeToken) > 0 {
			err := forEach(message.ResumeToken)
			if err != nil {
				return faults.Wrap(err)
			}
		}
	}

	return nil
}
