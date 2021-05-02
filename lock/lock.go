package lock

import "context"

type Locker interface {
	Lock(context.Context) (chan struct{}, error)
	Unlock(context.Context) error
}

type WaitForUnlocker interface {
	WaitForUnlock(context.Context) error
}
