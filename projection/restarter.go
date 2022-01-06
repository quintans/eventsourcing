package projection

import (
	"context"
	"time"

	"github.com/quintans/faults"
	"github.com/teris-io/shortid"

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
	logger              log.Logger
	lock                lock.Locker
	projectionCanceller CancelPublisher
	subscribers         []Subscriber
	membersCount        int
}

func NewNotifierLockRestarter(
	logger log.Logger,
	lock lock.Locker,
	projectionCanceller CancelPublisher,
	subscribers []Subscriber,
	membersCount int,
) *NotifierLockRebuilder {
	logger = logger.WithTags(log.Tags{
		"id": shortid.MustGenerate(),
	})
	return &NotifierLockRebuilder{
		logger:              logger,
		lock:                lock,
		projectionCanceller: projectionCanceller,
		subscribers:         subscribers,
		membersCount:        membersCount,
	}
}

func (r *NotifierLockRebuilder) Rebuild(
	ctx context.Context,
	projection string,
	catchUp func(context.Context, []Resume) error,
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

	logger.Infof("Signalling '%d' members to STOP projection listener", r.membersCount)
	err = r.projectionCanceller.PublishCancel(ctx, projection, r.membersCount)
	if err != nil {
		logger.WithError(err).Error("Error while freezing projection")
		r.unlock(ctx, logger)
		return err
	}

	go func() {
		ctx := context.Background()
		defer r.unlock(ctx, logger)

		logger.Info("Retrieving subscriptions last position")
		resumes, err := r.retrieveResumes(ctx)
		if err != nil {
			logger.
				WithError(err).
				Errorf("Error while retrieving subscriptions last position")
		}

		logger.Info("Catching up projection")
		err = catchUp(ctx, resumes)
		if err != nil {
			logger.
				WithError(err).
				Error("Error while catching up projection")
		}

		logger.Info("Recording subscriptions positions")
		err = r.recordResumeTokens(ctx, resumes)
		if err != nil {
			logger.
				WithError(err).
				Error("Error while recording subscriptions positions")
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

func (r *NotifierLockRebuilder) retrieveResumes(ctx context.Context) ([]Resume, error) {
	var max time.Time
	var resumes []Resume
	for _, sub := range r.subscribers {
		resume, err := sub.RetrieveLastResume(ctx)
		if err != nil {
			return nil, faults.Wrap(err)
		}
		// add offset to compensate clock skews
		resume.EventID = resume.EventID.OffsetTime(time.Second)
		resumes = append(resumes, resume)

		// max time
		t := resume.EventID.Time()
		if t.After(max) {
			max = t
		}
	}

	// wait for the safety offset to have passed
	now := time.Now()
	if max.After(now) {
		time.Sleep(max.Sub(now))
	}

	return resumes, nil
}

func (r *NotifierLockRebuilder) recordResumeTokens(ctx context.Context, resumes []Resume) error {
	for k, sub := range r.subscribers {
		if err := sub.RecordLastResume(ctx, resumes[k].Token); err != nil {
			return faults.Wrap(err)
		}
	}
	return nil
}
