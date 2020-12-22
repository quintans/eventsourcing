package projection

import (
	"context"
	"fmt"

	"github.com/quintans/eventstore/common"
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
	lock     common.Locker
	notifier Notifier
}

func NewNotifierLockRestarter(lock common.Locker, notifier Notifier) *NotifierLockRestarter {
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
	_, err := r.lock.Lock(ctx)
	if err != nil {
		return fmt.Errorf("Unable to acquire rebuild lock for projection %s: %w", projection, err)
	}

	defer func() {
		logger.Info("Releasing rebuild projection lock")
		r.lock.Unlock(ctx)
	}()

	logger.Info("Signalling to STOP projection listener")
	err = r.notifier.CancelProjection(ctx, projection, partitions)
	if err != nil {
		log.WithError(err).Errorf("Error while freezing projection %s", projection)
		return err
	}

	return restartFn(ctx)
}
