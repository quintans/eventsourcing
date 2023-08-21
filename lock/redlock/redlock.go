package redlock

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"sync"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/quintans/faults"

	"github.com/quintans/eventsourcing/lock"
)

var _ lock.Locker = (*Lock)(nil)

const retries = 3

// ErrExtendFailed is the error resulting if Redsync fails to extend the
// lock.
var ErrExtendFailed = errors.New("redis: failed to extend lock")

// A DelayFunc is used to decide the amount of time to wait between retries.
type DelayFunc func(tries int) time.Duration

// A Lock is a distributed mutual exclusion lock.
type Lock struct {
	name   string
	expiry time.Duration

	factor float64

	quorum int

	value string

	done chan struct{}
	mu   sync.Mutex
	pool *Pool
}

func (m *Lock) isLockAcquired() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.done != nil
}

func (m *Lock) lockAcquired(done chan struct{}) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.done = done
}

func (m *Lock) iAmDone() {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.done == nil {
		return
	}
	close(m.done)
	m.done = nil
}

// Lock locks m. In case it returns an error on failure, you may retry to acquire the lock by calling this method again.
func (m *Lock) Lock(ctx context.Context) (context.Context, error) {
	if m.isLockAcquired() {
		return nil, faults.Errorf("failed to acquire lock: '%s': %w", m.name, lock.ErrLockAlreadyHeld)
	}

	value, err := genValue()
	if err != nil {
		return nil, err
	}

	start := time.Now()

	n, err := m.pool.DoAsync(func(client *redis.Client) (bool, error) {
		return m.acquire(ctx, client, value)
	})
	if n == 0 && err != nil {
		return nil, err
	}

	now := time.Now()
	until := now.Add(m.expiry - now.Sub(start) - time.Duration(int64(float64(m.expiry)*m.factor)))
	if n >= m.quorum && now.Before(until) {
		m.value = value

		// auto renew session
		ctx = m.heartbeat(ctx, m.expiry/2)
		return ctx, nil
	}
	// release any previously acquired lock
	_, _ = m.pool.DoAsync(func(client *redis.Client) (bool, error) {
		return m.release(ctx, client, value)
	})

	return nil, faults.Wrap(lock.ErrLockAlreadyAcquired)
}

func (m *Lock) heartbeat(ctx context.Context, expiry time.Duration) context.Context {
	done := make(chan struct{})
	m.lockAcquired(done)
	ticker := time.NewTicker(expiry)
	ctx, cancel := context.WithCancel(ctx)
	go func() {
		defer cancel()
		defer ticker.Stop()
		for {
			select {
			case <-done:
				return
			case <-ticker.C:
				err := m.extend(context.Background())
				if err != nil {
					_ = m.Unlock(context.Background())
					return
				}
			}
		}
	}()
	return ctx
}

// Unlock unlocks m and returns the status of unlock.
func (m *Lock) Unlock(ctx context.Context) error {
	if !m.isLockAcquired() {
		return faults.Wrap(lock.ErrLockNotHeld)
	}

	n, err := m.pool.DoAsync(func(client *redis.Client) (bool, error) {
		return m.release(ctx, client, m.value)
	})
	if n < m.quorum {
		return err
	}

	m.iAmDone()

	return nil
}

// extend resets the lock's expiry and returns the status of expiry extension.
func (m *Lock) extend(ctx context.Context) error {
	if !m.isLockAcquired() {
		return faults.Wrap(lock.ErrLockNotHeld)
	}

	start := time.Now()
	n, err := m.pool.DoAsync(func(client *redis.Client) (bool, error) {
		return m.touch(ctx, client, m.value, int(m.expiry/time.Millisecond))
	})
	if n < m.quorum {
		return err
	}
	now := time.Now()
	until := now.Add(m.expiry - now.Sub(start) - time.Duration(int64(float64(m.expiry)*m.factor)))
	if now.Before(until) {
		return nil
	}
	return faults.Wrap(ErrExtendFailed)
}

func genValue() (string, error) {
	b := make([]byte, 16)
	_, err := rand.Read(b)
	if err != nil {
		return "", faults.Wrap(err)
	}
	return base64.StdEncoding.EncodeToString(b), nil
}

func (m *Lock) acquire(ctx context.Context, client *redis.Client, value string) (bool, error) {
	reply, err := client.SetNX(ctx, m.name, value, m.expiry).Result()
	if err != nil {
		return false, faults.Wrap(err)
	}
	return reply, nil
}

const deleteScript = `
	if redis.call("GET", KEYS[1]) == ARGV[1] then
		return redis.call("DEL", KEYS[1])
	else
		return 0
	end
`

func (m *Lock) release(ctx context.Context, client *redis.Client, value string) (bool, error) {
	status, err := client.Eval(ctx, deleteScript, []string{m.name}, value).Int64()
	if err != nil {
		return false, faults.Wrap(err)
	}
	return status != 0, nil
}

const touchScript = `
	if redis.call("GET", KEYS[1]) == ARGV[1] then
		return redis.call("PEXPIRE", KEYS[1], ARGV[2])
	else
		return 0
	end
`

func (m *Lock) touch(ctx context.Context, client *redis.Client, value string, expiry int) (bool, error) {
	status, err := client.Eval(ctx, touchScript, []string{m.name}, value, expiry).Int64()
	if err != nil {
		return false, faults.Wrap(err)
	}
	return status != 0, nil
}

func (m *Lock) WaitForUnlock(ctx context.Context) error {
	done := make(chan error, 1)
	heartbeat := m.expiry / 2

	go func() {
		ticker := time.NewTicker(heartbeat)
		defer ticker.Stop()
		for {
			ctx2, cancel := context.WithTimeout(ctx, heartbeat)
			n, err := m.pool.DoUntilQuorumAsync(m.quorum, func(c *redis.Client) (bool, error) {
				return m.exists(ctx2, c)
			})
			cancel()
			if err != nil {
				done <- faults.Wrap(err)
				return
			}
			// no quorum, no lock
			if n < m.quorum {
				done <- nil
				return
			}
			<-ticker.C
		}
	}()
	err := <-done

	return err
}

func (m *Lock) exists(ctx context.Context, client *redis.Client) (bool, error) {
	reply, err := client.Exists(ctx, m.name).Result()
	if err != nil {
		return false, faults.Errorf("failed to check if key '%s' exists: %w", m.name, err)
	}
	return reply != 0, nil
}

func (m *Lock) WaitForLock(ctx context.Context) (context.Context, error) {
	for {
		ctx2, err := m.Lock(ctx)
		if errors.Is(err, lock.ErrLockAlreadyAcquired) {
			_ = m.WaitForUnlock(ctx)
			continue
		} else if err != nil {
			return nil, faults.Wrap(err)
		}

		return ctx2, nil
	}
}
