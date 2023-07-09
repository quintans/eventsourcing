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

var _ Memberlister = (*RedisMemberList)(nil)

const (
	separator = ","
	maxScan   = 10
	retries   = 3
)

type RedisMemberList struct {
	pool       []*redis.Client
	prefix     string
	name       string
	expiration time.Duration
	quorum     int
}

func NewRedisMemberlist(addresses, prefix string, expiration time.Duration) *RedisMemberList {
	addrs := strings.Split(addresses, separator)
	pool := make([]*redis.Client, 0, len(addrs))
	for _, addr := range addrs {
		pool = append(pool, redis.NewClient(&redis.Options{
			Addr: strings.TrimSpace(addr),
		}))
	}
	return &RedisMemberList{
		pool:       pool,
		prefix:     prefix,
		name:       prefix + "-" + shortid.MustGenerate(),
		expiration: expiration,
		quorum:     len(pool)/2 + 1,
	}
}

func (r *RedisMemberList) Name() string {
	return r.name
}

func (r *RedisMemberList) List(ctx context.Context) ([]MemberWorkers, error) {
	results, err := doAsync(r, func(c *redis.Client) ([]MemberWorkers, error) {
		return r.list(ctx, c)
	})
	if len(results) < r.quorum {
		return nil, err
	}

	// merge all workers of a member
	memberSet := map[string]map[string]bool{}
	for _, result := range results {
		for _, mw := range result {
			workerSet, ok := memberSet[mw.Name]
			if !ok {
				workerSet = map[string]bool{}
				memberSet[mw.Name] = workerSet
			}
			for _, w := range mw.Workers {
				workerSet[w] = true
			}
		}
	}
	mws := make([]MemberWorkers, 0, len(memberSet))
	for name, workers := range memberSet {
		mw := MemberWorkers{
			Name: name,
		}
		for name := range workers {
			mw.Workers = append(mw.Workers, name)
		}
		mws = append(mws, mw)
	}
	return mws, nil
}

func (r *RedisMemberList) list(ctx context.Context, c *redis.Client) ([]MemberWorkers, error) {
	var cursor uint64
	membersWorkers := []MemberWorkers{}

	type result struct {
		MemberID string
		Val      string
		Err      error
	}

	var allErr error
	for {
		var members []string
		var err error
		members, cursor, err = c.Scan(ctx, cursor, r.prefix+"-*", maxScan).Result()
		if err != nil {
			return nil, faults.Wrap(err)
		}
		ch := make(chan result, len(members))
		for _, memberID := range members {
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
			}(memberID)
		}
		for range members {
			r := <-ch
			if r.Err != nil {
				allErr = multierror.Append(err, r.Err)
			} else {
				s := strings.Split(r.Val, separator)
				membersWorkers = append(membersWorkers, MemberWorkers{
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
	return membersWorkers, nil
}

func (r *RedisMemberList) Register(ctx context.Context, workers []string) error {
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

func (r *RedisMemberList) Unregister(ctx context.Context) error {
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

func doAsync[T any](r *RedisMemberList, actFn func(*redis.Client) (T, error)) ([]T, error) {
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
