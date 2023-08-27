package store

import (
	"context"

	"github.com/quintans/faults"

	"github.com/quintans/eventsourcing/encoding"
	"github.com/quintans/eventsourcing/sink"
)

// ForEachSequenceInSinkPartitions retrieves the last message for all the partitions
func ForEachSequenceInSinkPartitions(ctx context.Context, sinker sink.Sinker, partitionLow, partitionHi uint32, forEach func(encoding.Base64) error) error {
	if partitionLow < 1 {
		return faults.Errorf("partitionLow is less than 1: %d", partitionLow)
	}
	if partitionHi < partitionLow {
		return faults.Errorf("partitionHi is less than partitionLow: %d < %d", partitionHi, partitionLow)
	}

	// getting all resume tokens over
	for i := partitionLow; i <= partitionHi; i++ {
		resumeToken, err := sinker.ResumeToken(ctx, i)
		if err != nil {
			return faults.Errorf("Unable to get the last event ID in sink from partition %d: %w", i, err)
		}
		if resumeToken != nil {
			err := forEach(resumeToken)
			if err != nil {
				return faults.Wrap(err)
			}
		}
	}

	return nil
}
