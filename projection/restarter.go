package projection

import (
	"context"

	"github.com/quintans/eventstore/worker"
	"github.com/quintans/faults"
	log "github.com/sirupsen/logrus"
)

type Action int

const (
	Release Action = iota + 1
)

type Notification struct {
	Projection string `json:"projection"`
	Action
}

// Restarter is responsible for the projection restart.
// Any running projection needs to stop until restartFn returns.
// restartFn() is responsible for cleaning the projection
type Restarter interface {
	Restart(ctx context.Context, projection string, partitions int, restartFn func(ctx context.Context) error) error
}

type NotifierLockRestarter struct {
	lock     worker.Locker
	notifier Notifier
}

func NewNotifierLockRestarter(lock worker.Locker, notifier Notifier) *NotifierLockRestarter {
	return &NotifierLockRestarter{
		lock:     lock,
		notifier: notifier,
	}
}

func (r *NotifierLockRestarter) Restart(ctx context.Context, projection string, partitions int, restartFn func(ctx context.Context) error) error {
	logger := log.WithFields(log.Fields{
		"method":     "Restarter.Restart",
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
		err = r.notifier.CancelProjection(ctx, projection, partitions)
		if err != nil {
			log.WithError(err).Errorf("Error while freezing projection %s", projection)
			return
		}

		logger.Info("Restarting...")
		err = restartFn(ctx)
		if err != nil {
			log.WithError(err).Errorf("Error while restarting projection %s", projection)
		}
	}()

	return nil
}
