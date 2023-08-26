package store

import (
	"context"

	"github.com/quintans/faults"

	"github.com/quintans/eventsourcing/encoding"
	"github.com/quintans/eventsourcing/sink"
)

// ForEachSequenceInSinkPartitions retrieves the last message for all the partitions
func ForEachSequenceInSinkPartitions(ctx context.Context, sinker sink.Sinker, partitionLow, partitionHi uint32, forEach func(encoding.Base64) error) error {
	if partitionLow == 0 {
		partitionHi = 0
	}

	// looking for the highest sequence in all partitions.
	// Sending a message to partitions is done synchronously and in order, so we should start from the last successful sent message.
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
