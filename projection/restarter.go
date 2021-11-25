package projection

import (
	"context"

	"github.com/quintans/faults"

	"github.com/quintans/eventsourcing/eventid"
	"github.com/quintans/eventsourcing/lock"
	"github.com/quintans/eventsourcing/log"
	"github.com/quintans/eventsourcing/worker"
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
	notifier      CancelPublisher
	subscriber    Subscriber
	streamResumer StreamResumer
	tokenStreams  []StreamResume
	memberLister  worker.Memberlister
}

func NewNotifierLockRestarter(
	logger log.Logger,
	lock lock.Locker,
	notifier CancelPublisher,
	subscriber Subscriber,
	streamResumer StreamResumer,
	tokenStreams []StreamResume,
	memberLister worker.Memberlister,
) *NotifierLockRebuilder {
	return &NotifierLockRebuilder{
		logger:        logger,
		lock:          lock,
		notifier:      notifier,
		subscriber:    subscriber,
		streamResumer: streamResumer,
		tokenStreams:  tokenStreams,
		memberLister:  memberLister,
	}
}

func (r *NotifierLockRebuilder) Rebuild(
	ctx context.Context,
	projection string,
	beforeRecordingTokens func(ctx context.Context) (eventid.EventID, error),
	afterRecordingTokens func(ctx context.Context, afterEventID eventid.EventID) (eventid.EventID, error),
) error {
	logger := r.logger.WithTags(log.Tags{
		"method":     "NotifierLockRebuilder.Rebuild",
		"projection": projection,
	})

	logger.Info("Acquiring rebuild projection lock")
	_, err := r.lock.Lock(ctx)
	if err != nil {
		return faults.Errorf("failed to acquire rebuild lock for projection %s: %w", projection, err)
	}

	logger.Info("Signalling to STOP projection listener")
	members, err := r.memberLister.List(ctx)
	if err != nil {
		return faults.Errorf("failed to members list for projection %s: %w", projection, err)
	}
	err = r.notifier.PublishCancel(ctx, projection, len(members))
	if err != nil {
		logger.WithError(err).Error("Error while freezing projection")
		r.unlock(ctx, logger)
		return err
	}

	go func() {
		ctx := context.Background()
		defer r.unlock(ctx, logger)

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

func (r *NotifierLockRebuilder) unlock(ctx context.Context, logger log.Logger) {
	logger.Info("Releasing rebuild projection lock")
	err := r.lock.Unlock(ctx)
	if err != nil {
		logger.WithError(err).Info("Failed to release rebuild projection lock")
	}
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
