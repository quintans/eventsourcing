package projection

import (
	"context"
	"fmt"

	"github.com/quintans/faults"
	"github.com/teris-io/shortid"

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
	logger              log.Logger
	lock                lock.Locker
	projectionCanceller CancelPublisher
	subscribers         []Subscriber
	memberLister        worker.Memberlister
}

func NewNotifierLockRestarter(
	logger log.Logger,
	lock lock.Locker,
	projectionCanceller CancelPublisher,
	subscribers []Subscriber,
	memberLister worker.Memberlister,
) *NotifierLockRebuilder {
	logger = logger.WithTags(log.Tags{
		"id": shortid.MustGenerate(),
	})
	return &NotifierLockRebuilder{
		logger:              logger,
		lock:                lock,
		projectionCanceller: projectionCanceller,
		subscribers:         subscribers,
		memberLister:        memberLister,
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

	members, err := r.memberLister.List(ctx)
	if err != nil {
		return faults.Errorf("failed to members list for projection %s: %w", projection, err)
	}
	logger.Infof("Signalling '%d' members to STOP projection listener", len(members))
	err = r.projectionCanceller.PublishCancel(ctx, projection, len(members))
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
			logger.
				WithTags(log.Tags{
					"error": fmt.Sprintf("%+v", err),
				}).Error("Error while restarting projection before recording BUS tokens")
		}
		logger.Info("Moving subscriptions to last position")
		err = r.recordResumeTokens(ctx)
		if err != nil {
			logger.
				WithTags(log.Tags{
					"error": fmt.Sprintf("%+v", err),
				}).
				Errorf("Error while restarting projection when moving subscriptions to last position")
		}
		logger.Infof("After recording BUS tokens. Last mile after %s", afterEventID)
		_, err = afterRecordingTokens(ctx, afterEventID)
		if err != nil {
			logger.
				WithTags(log.Tags{
					"error": fmt.Sprintf("%+v", err),
				}).Error("Error while restarting projection after recording BUS tokens")
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
	for _, sub := range r.subscribers {
		if err := sub.RecordLastResume(ctx); err != nil {
			return faults.Wrap(err)
		}
	}
	return nil
}
