package lock

import (
	"context"
	"errors"
)

var (
	ErrLockAlreadyHeld     = errors.New("lock already held")
	ErrLockNotHeld         = errors.New("lock not held")
	ErrLockAlreadyAcquired = errors.New("lock already acquired by other")
)

type Locker interface {
	// Lock acquires a lock. The return channel will be closed when the lock is released.
	Lock(context.Context) (<-chan struct{}, error)
	// Unlock releases the lock, leading to the closing the channel returned in Lock()
	Unlock(context.Context) error
	// WaitForUnlock blocks until this lock is released
	WaitForUnlock(context.Context) error
}
