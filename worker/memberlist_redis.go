package worker

import (
	"context"
	"strings"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/google/uuid"
	"github.com/quintans/faults"
)

var _ Memberlister = (*RedisMemberList)(nil)

type RedisMemberList struct {
	rdb        *redis.Client
	prefix     string
	name       string
	expiration time.Duration
}

func NewRedisMemberlist(address string, prefix string, expiration time.Duration) *RedisMemberList {
	rdb := redis.NewClient(&redis.Options{
		Addr:     address,
		Password: "", // no password set
		DB:       0,  // use default DB
	})
	return &RedisMemberList{
		rdb:        rdb,
		prefix:     prefix,
		name:       prefix + "-" + uuid.New().String(),
		expiration: expiration,
	}
}

func (r *RedisMemberList) Name() string {
	return r.name
}

func (r *RedisMemberList) List(ctx context.Context) ([]MemberWorkers, error) {
	var cursor uint64
	members := []MemberWorkers{}
	for {
		var keys []string
		var err error
		keys, cursor, err = r.rdb.Scan(ctx, cursor, r.prefix+"-*", 10).Result()
		if err != nil {
			return nil, faults.Wrap(err)
		}
		for _, v := range keys {
			val, err := r.rdb.Get(ctx, v).Result()
			if err != nil {
				return nil, faults.Wrap(err)
			}
			s := strings.Split(val, ",")
			members = append(members, MemberWorkers{
				Name:    v,
				Workers: s,
			})
		}
		if cursor == 0 {
			break
		}
	}
	return members, nil
}

func (r *RedisMemberList) Register(ctx context.Context, workers []string) error {
	s := strings.Join(workers, ",")
	err := r.rdb.Set(ctx, r.name, s, r.expiration).Err()
	return faults.Wrap(err)
}

func (r *RedisMemberList) Unregister(ctx context.Context) error {
	err := r.rdb.Del(ctx, r.name).Err()
	return faults.Wrap(err)
}
