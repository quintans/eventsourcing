package store

import (
	"context"

	"github.com/quintans/faults"

	"github.com/quintans/eventsourcing"
	"github.com/quintans/eventsourcing/log"
	"github.com/quintans/eventsourcing/sink"
)

type Feeder interface {
	Feed(ctx context.Context, sink sink.Sinker) error
}

type Forwarder struct {
	logger log.Logger
	name   string
	feeder Feeder
	sinker sink.Sinker
}

func NewForwarder(logger log.Logger, name string, feeder Feeder, sinker sink.Sinker) *Forwarder {
	return &Forwarder{
		logger: logger,
		name:   name,
		feeder: feeder,
		sinker: sinker,
	}
}

func (f *Forwarder) Run(ctx context.Context) error {
	f.logger.Infof("Starting Feed '%s'", f.name)
	err := f.feeder.Feed(ctx, f.sinker)
	if err != nil {
		return faults.Errorf("Error feeding '%s' on boot: %w", f.name, err)
	}
	return nil
}

func (f *Forwarder) Cancel() {}

// ForEachResumeTokenInSinkPartitions retrieves the last message for all the partitions
func ForEachResumeTokenInSinkPartitions(ctx context.Context, sinker sink.Sinker, partitionLow, partitionHi uint32, forEach func(*eventsourcing.Event) error) error {
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
		if message != nil {
			err := forEach(message)
			if err != nil {
				return faults.Wrap(err)
			}
		}
	}

	return nil
}
