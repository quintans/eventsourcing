package lock

import "context"

type Locker interface {
	Lock(context.Context) (chan struct{}, error)
	Unlock(context.Context) error
	WaitForUnlock(context.Context) error
}
