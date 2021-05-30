package projection

import (
	"context"

	"github.com/quintans/faults"

	"github.com/quintans/eventsourcing/eventid"
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
	Rebuild(ctx context.Context, projection string, beforeRecordingTokens func(ctx context.Context) (eventid.EventID, error), afterRecordingTokens func(ctx context.Context, afterEventID eventid.EventID) (eventid.EventID, error)) error
}

type ResumeTokenUpdater interface {
	UpdateResumeTokens(context.Context) error
}

type NotifierLockRebuilder struct {
	logger        log.Logger
	lock          lock.Locker
	notifier      Notifier
	subscriber    Subscriber
	streamResumer StreamResumer
	tokenStreams  []StreamResume
}

func NewNotifierLockRestarter(
	logger log.Logger,
	lock lock.Locker,
	notifier Notifier,
	subscriber Subscriber,
	streamResumer StreamResumer,
	tokenStreams []StreamResume,
) *NotifierLockRebuilder {
	return &NotifierLockRebuilder{
		logger:        logger,
		lock:          lock,
		notifier:      notifier,
		subscriber:    subscriber,
		streamResumer: streamResumer,
		tokenStreams:  tokenStreams,
	}
}

func (r *NotifierLockRebuilder) Rebuild(ctx context.Context, projection string, beforeRecordingTokens func(ctx context.Context) (eventid.EventID, error), afterRecordingTokens func(ctx context.Context, afterEventID eventid.EventID) (eventid.EventID, error)) error {
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
		err = r.notifier.CancelProjection(ctx, projection, len(r.tokenStreams))
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

func (r *NotifierLockRebuilder) recordResumeTokens(ctx context.Context) error {
	for _, ts := range r.tokenStreams {
		token, err := r.subscriber.GetResumeToken(ctx, ts.Topic)
		if err != nil {
			return faults.Wrap(err)
		}
		err = r.streamResumer.SetStreamResumeToken(ctx, ts.String(), token)
		if err != nil {
			return faults.Wrap(err)
		}
	}
	return nil
}
