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

func NewRedisMemberlist(addresses string, prefix string, expiration time.Duration) *RedisMemberList {
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
	results, err := r.doAsync(func(c *redis.Client) (interface{}, error) {
		return r.list(ctx, c)
	})
	if len(results) < r.quorum {
		return []MemberWorkers{}, err
	}

	// merge all workers of a member
	memberSet := map[string]map[string]bool{}
	for _, result := range results {
		mw := result.(MemberWorkers)
		workerSet, ok := memberSet[mw.Name]
		if !ok {
			workerSet = map[string]bool{}
			memberSet[mw.Name] = workerSet
		}
		for _, w := range mw.Workers {
			workerSet[w] = true
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
		Val string
		Err error
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
				r := result{}
				retry.Do(
					func() error {
						r.Val, r.Err = c.Get(ctx, m).Result()
						return r.Err
					},
					retry.Attempts(retries),
				)
				r.Err = faults.Wrap(r.Err)
				ch <- r
			}(memberID)
		}
		for _, memberID := range members {
			r := <-ch
			if r.Err != nil {
				allErr = multierror.Append(err, r.Err)
			} else {
				s := strings.Split(r.Val, separator)
				membersWorkers = append(membersWorkers, MemberWorkers{
					Name:    memberID,
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
	_, err := r.doAsync(func(c *redis.Client) (interface{}, error) {
		status, err := c.Set(ctx, r.name, s, r.expiration).Result()
		if err != nil {
			return nil, faults.Wrap(err)
		}
		return status, nil
	})
	return faults.Wrap(err)
}

func (r *RedisMemberList) Unregister(ctx context.Context) error {
	_, err := r.doAsync(func(c *redis.Client) (interface{}, error) {
		status, err := c.Del(ctx, r.name).Result()
		if err != nil {
			return nil, faults.Wrap(err)
		}
		return status, nil
	})
	return faults.Wrap(err)
}

func (r *RedisMemberList) doAsync(actFn func(*redis.Client) (interface{}, error)) ([]interface{}, error) {
	type result struct {
		Val interface{}
		Err error
	}

	ch := make(chan result, len(r.pool))
	for _, client := range r.pool {
		go func(client *redis.Client) {
			r := result{}
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
	results := make([]interface{}, 0, len(r.pool))
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
