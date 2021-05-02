package projection

import (
	"context"

	"github.com/quintans/faults"

	"github.com/quintans/eventsourcing/lock"
	"github.com/quintans/eventsourcing/log"
)

type Action int

const (
	Release Action = iota + 1
)

type Notification struct {
	Projection string `json:"projection"`
	Action
}

// Rebuilder is responsible for the projection restart.
// Any running projection needs to stop until restartFn returns.
// restartFn() is responsible for cleaning the projection
type Rebuilder interface {
	Rebuild(ctx context.Context, projection string, listenerCount int, beforeRecordingTokens func(ctx context.Context) (string, error), afterRecordingTokens func(ctx context.Context, afterEventID string) (string, error)) error
}

type ResumeTokenUpdater interface {
	UpdateResumeTokens(context.Context) error
}

type NotifierLockRebuilder struct {
	logger             log.Logger
	lock               lock.Locker
	notifier           Notifier
	recordResumeTokens func(ctx context.Context) error
}

func NewNotifierLockRestarter(logger log.Logger, lock lock.Locker, notifier Notifier, updateResumeTokens func(ctx context.Context) error) *NotifierLockRebuilder {
	return &NotifierLockRebuilder{
		logger:             logger,
		lock:               lock,
		notifier:           notifier,
		recordResumeTokens: updateResumeTokens,
	}
}

func (r *NotifierLockRebuilder) Rebuild(ctx context.Context, projection string, listenerCount int, beforeRecordingTokens func(ctx context.Context) (string, error), afterRecordingTokens func(ctx context.Context, afterEventID string) (string, error)) error {
	logger := r.logger.WithTags(log.Tags{
		"method":     "NotifierLockRebuilder.Rebuild",
		"projection": projection,
	})

	logger.Info("Acquiring rebuild projection lock")
	released, err := r.lock.Lock(ctx)
	if err != nil {
		return faults.Errorf("Failed to acquire rebuild lock for projection %s: %w", projection, err)
	}
	if released == nil {
		return faults.Errorf("Unable to acquire rebuild lock for projection %s", projection)
	}

	go func() {
		ctx := context.Background()
		defer func() {
			logger.Info("Releasing rebuild projection lock")
			err := r.lock.Unlock(ctx)
			if err != nil {
				logger.WithError(err).Info("Failed to release rebuild projection lock")
			}
		}()

		logger.Info("Signalling to STOP projection listener")
		err = r.notifier.CancelProjection(ctx, projection, listenerCount)
		if err != nil {
			logger.WithError(err).Error("Error while freezing projection")
			return
		}

		logger.Info("Before recording BUS tokens")
		afterEventID, err := beforeRecordingTokens(ctx)
		if err != nil {
			logger.WithError(err).Error("Error while restarting projection")
		}
		logger.Info("Recording BUS tokens")
		err = r.recordResumeTokens(ctx)
		if err != nil {
			logger.WithError(err).Error("Error while restarting projection")
		}
		logger.Infof("After recording BUS tokens. Last mile after %s", afterEventID)
		_, err = afterRecordingTokens(ctx, afterEventID)
		if err != nil {
			logger.WithError(err).Error("Error while restarting projection")
		}
	}()

	return nil
}
