package redlock

import (
	"strings"
	"time"

	"github.com/avast/retry-go/v3"
	"github.com/go-redis/redis/v8"
	"github.com/hashicorp/go-multierror"
)

type Pool struct {
	pool []*redis.Client
}

func NewPool(addresses string) *Pool {
	addrs := strings.Split(addresses, ",")
	pool := make([]*redis.Client, 0, len(addrs))
	for _, addr := range addrs {
		pool = append(pool, redis.NewClient(&redis.Options{
			Addr: strings.TrimSpace(addr),
		}))
	}
	return &Pool{
		pool: pool,
	}
}

// NewMutex returns a new distributed mutex with given name.
func (r *Pool) NewLock(name string, options ...RedisOption) *Lock {
	m := &Lock{
		name:   name,
		expiry: 8 * time.Second,
		factor: 0.01,
		quorum: len(r.pool)/2 + 1,
		pool:   r,
	}
	for _, o := range options {
		o(m)
	}
	return m
}

// OptionFunc is a function that configures a mutex.
type RedisOption func(*Lock)

// WithExpiry can be used to set the expiry of a mutex to the given value.
func WithExpiry(expiry time.Duration) RedisOption {
	return RedisOption(func(m *Lock) {
		m.expiry = expiry
	})
}

// WithDriftFactor can be used to set the clock drift factor.
func WithDriftFactor(factor float64) RedisOption {
	return func(m *Lock) {
		m.factor = factor
	}
}

func (p *Pool) DoUntilQuorumAsync(quorum int, actFn func(*redis.Client) (bool, error)) (int, error) {
	return p.doAsync(quorum, actFn)
}

func (p *Pool) DoAsync(actFn func(*redis.Client) (bool, error)) (int, error) {
	return p.doAsync(0, actFn)
}

func (p *Pool) doAsync(quorum int, actFn func(*redis.Client) (bool, error)) (int, error) {
	type result struct {
		Status bool
		Err    error
	}

	ch := make(chan result, len(p.pool))
	for _, client := range p.pool {
		go func(client *redis.Client) {
			r := result{}
			retry.Do(
				func() error {
					r.Status, r.Err = actFn(client)
					return r.Err
				},
				retry.Attempts(retries),
			)
			ch <- r
		}(client)
	}
	n := 0
	var err error
	for range p.pool {
		r := <-ch
		if r.Status {
			n++
		} else if r.Err != nil {
			err = multierror.Append(err, r.Err)
		}
		if quorum != 0 && n >= quorum {
			break
		}
	}
	return n, err
}
