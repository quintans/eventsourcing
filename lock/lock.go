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

		The returned context will be cancelled when the lock is released.
			The following errors can occur:
			* ErrLockAlreadyHeld: it already had the lock
			* ErrLockAlreadyAcquired: the lock is already held by another lock
	*/
	Lock(context.Context) (context.Context, error)
	/*
		Unlock releases the lock, leading to the cancelling the context returned in Lock()

			The following errors can occur:
			* ErrLockNotHeld: releasing a lock that it was not previously acquired
	*/
	Unlock(context.Context) error

	// WaitForLock lock or wait until it locks or the context is cancelled.
	// The returned context will be cancelled when the lock is released.
	WaitForLock(context.Context) (context.Context, error)
}

type WaitForUnlocker interface {
	// WaitForUnlock blocks until this lock is released
	WaitForUnlock(context.Context) error
}
