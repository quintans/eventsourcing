package lock

import (
	"context"
	"errors"
)

var (
	ErrLockHeld     = errors.New("lock already held")
	ErrLockNotHeld  = errors.New("lock not held")
	ErrLockAcquired = errors.New("lock already acquired by other")
)

type Locker interface {
	Lock(context.Context) (<-chan struct{}, error)
	Unlock(context.Context) error
	WaitForUnlock(context.Context) error
}
