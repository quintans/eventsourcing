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

type WaitLocker interface {
	Locker
	WaitForUnlocker
}

type Locker interface {
	/*
		Lock acquires a lock.

		The returned channel will be closed when the lock is released.
			The following errors can occur:
			* ErrLockAlreadyHeld: it already had the lock
			* ErrLockAlreadyAcquired: the lock is already held by another lock
	*/
	Lock(context.Context) (<-chan struct{}, error)
	/*
		Unlock releases the lock, leading to the closing the channel returned in Lock()

			The following errors can occur:
			* ErrLockNotHeld: releasing a lock that it was not previously acquired
	*/
	Unlock(context.Context) error
}

type WaitForUnlocker interface {
	// WaitForUnlock blocks until this lock is released
	WaitForUnlock(context.Context) error
}
