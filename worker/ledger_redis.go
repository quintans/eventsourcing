package worker

import (
	"context"
	"strings"
	"time"

	"github.com/avast/retry-go/v3"
	"github.com/go-redis/redis/v8"
	"github.com/hashicorp/go-multierror"
	"github.com/quintans/faults"
	"github.com/teris-io/shortid"
)

var _ Ledger = (*RedisLedger)(nil)

const (
	separator = ","
	maxScan   = 10
	retries   = 3
)

type RedisLedger struct {
	pool       []*redis.Client
	prefix     string
	name       string
	expiration time.Duration
	quorum     int
}

func NewRedisLedger(addresses, prefix string, expiration time.Duration) *RedisLedger {
	addrs := strings.Split(addresses, separator)
	pool := make([]*redis.Client, 0, len(addrs))
	for _, addr := range addrs {
		pool = append(pool, redis.NewClient(&redis.Options{
			Addr: strings.TrimSpace(addr),
		}))
	}
	return &RedisLedger{
		pool:       pool,
		prefix:     prefix,
		name:       prefix + "-" + shortid.MustGenerate(),
		expiration: expiration,
		quorum:     len(pool)/2 + 1,
	}
}

func (r *RedisLedger) Name() string {
	return r.name
}

func (r *RedisLedger) Peers(ctx context.Context) ([]Peer, error) {
	results, err := doAsync(r, func(c *redis.Client) ([]Peer, error) {
		return r.list(ctx, c)
	})
	if len(results) < r.quorum {
		return nil, err
	}

	// merge all workers of a peer
	peerSet := map[string]map[string]bool{}
	for _, result := range results {
		for _, mw := range result {
			workerSet, ok := peerSet[mw.Name]
			if !ok {
				workerSet = map[string]bool{}
				peerSet[mw.Name] = workerSet
			}
			for _, w := range mw.Workers {
				workerSet[w] = true
			}
		}
	}
	mws := make([]Peer, 0, len(peerSet))
	for name, workers := range peerSet {
		mw := Peer{
			Name: name,
		}
		for name := range workers {
			mw.Workers = append(mw.Workers, name)
		}
		mws = append(mws, mw)
	}
	return mws, nil
}

func (r *RedisLedger) list(ctx context.Context, c *redis.Client) ([]Peer, error) {
	var cursor uint64
	peersWorkers := []Peer{}

	type result struct {
		MemberID string
		Val      string
		Err      error
	}

	var allErr error
	for {
		var peers []string
		var err error
		peers, cursor, err = c.Scan(ctx, cursor, r.prefix+"-*", maxScan).Result()
		if err != nil {
			return nil, faults.Wrap(err)
		}
		ch := make(chan result, len(peers))
		for _, peerID := range peers {
			go func(m string) {
				var v string
				err = retry.Do(
					func() error {
						var er error
						v, er = c.Get(ctx, m).Result()
						return er
					},
					retry.Attempts(retries),
				)
				ch <- result{MemberID: m, Val: v, Err: faults.Wrap(err)}
			}(peerID)
		}
		for range peers {
			r := <-ch
			if r.Err != nil {
				allErr = multierror.Append(err, r.Err)
			} else {
				s := strings.Split(r.Val, separator)
				peersWorkers = append(peersWorkers, Peer{
					Name:    r.MemberID,
					Workers: s,
				})
			}
		}
		if cursor == 0 {
			break
		}
	}
	if allErr != nil {
		return nil, allErr
	}
	return peersWorkers, nil
}

func (r *RedisLedger) Register(ctx context.Context, workers []string) error {
	s := strings.Join(workers, separator)
	_, err := doAsync(r, func(c *redis.Client) (string, error) {
		status, err := c.Set(ctx, r.name, s, r.expiration).Result()
		if err != nil {
			return "", faults.Wrap(err)
		}
		return status, nil
	})
	return faults.Wrap(err)
}

func (r *RedisLedger) Close(ctx context.Context) error {
	_, err := doAsync(r, func(c *redis.Client) (int64, error) {
		status, err := c.Del(ctx, r.name).Result()
		if err != nil {
			return 0, faults.Wrap(err)
		}
		return status, nil
	})
	return faults.Wrap(err)
}

type result[T any] struct {
	Val T
	Err error
}

func doAsync[T any](r *RedisLedger, actFn func(*redis.Client) (T, error)) ([]T, error) {
	ch := make(chan result[T], len(r.pool))
	for _, client := range r.pool {
		go func(client *redis.Client) {
			var r result[T]
			retry.Do(
				func() error {
					r.Val, r.Err = actFn(client)
					return r.Err
				},
				retry.Attempts(retries),
			)
			ch <- r
		}(client)
	}
	results := make([]T, 0, len(r.pool))
	var err error
	for range r.pool {
		r := <-ch
		if r.Err != nil {
			err = multierror.Append(err, r.Err)
		} else {
			results = append(results, r.Val)
		}
	}
	return results, err
}
