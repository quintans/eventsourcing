package common

import (
	"context"
	"strings"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
)

type MemberWorkers struct {
	Name    string
	Workers []string
}

type Memberlister interface {
	Name() string
	List(context.Context) ([]MemberWorkers, error)
	Register(context.Context, []string) error
}

var _ Memberlister = (*RedisMemberlist)(nil)

type RedisMemberlist struct {
	rdb        *redis.Client
	prefix     string
	name       string
	expiration time.Duration
}

func NewRedisMemberlist(address string, prefix string, expiration time.Duration) *RedisMemberlist {
	rdb := redis.NewClient(&redis.Options{
		Addr:     address,
		Password: "", // no password set
		DB:       0,  // use default DB
	})
	return &RedisMemberlist{
		rdb:        rdb,
		prefix:     prefix,
		name:       prefix + "-" + uuid.New().String(),
		expiration: expiration,
	}
}

func (r *RedisMemberlist) Name() string {
	return r.name
}

func (r *RedisMemberlist) List(ctx context.Context) ([]MemberWorkers, error) {
	var cursor uint64
	members := []MemberWorkers{}
	for {
		var keys []string
		var err error
		keys, cursor, err = r.rdb.Scan(ctx, cursor, r.prefix+"-*", 10).Result()
		if err != nil {
			return nil, err
		}
		for _, v := range keys {
			val, err := r.rdb.Get(ctx, v).Result()
			if err != nil {
				return nil, err
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

func (r *RedisMemberlist) Register(ctx context.Context, workers []string) error {
	s := strings.Join(workers, ",")
	err := r.rdb.Set(ctx, r.name, s, r.expiration).Err()
	return err
}

type Worker interface {
	Name() string
	IsRunning() bool
	Start(context.Context, chan struct{})
	Stop()
}

type LockWorker struct {
	Lock   Locker
	Worker Worker
}

func BalanceWorkers(ctx context.Context, member Memberlister, lms []LockWorker, heartbeat time.Duration) {
	ticker := time.NewTicker(heartbeat)
	defer ticker.Stop()
	for {
		err := run(ctx, member, lms)
		if err != nil {
			log.Warnf("Error while balancing partitions: %v", err)
			continue
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func run(ctx context.Context, member Memberlister, workers []LockWorker) error {
	members, err := member.List(ctx)
	if err != nil {
		return err
	}

	// if current member is not in the list, add it to the member count
	membersCount := len(members)
	present := false
	for _, v := range members {
		if v.Name == member.Name() {
			present = true
		}
	}
	if !present {
		membersCount++
	}

	monitorsNo := len(workers)
	workersToAcquire := monitorsNo / membersCount

	// check if all nodes have the minimum workers. Only after that can the any extra worker be picked up.
	hasMinWorkers := true
	workersInUse := map[string]bool{}
	for _, m := range members {
		if workersToAcquire > len(m.Workers) {
			hasMinWorkers = false
		}
		for _, v := range m.Workers {
			workersInUse[v] = true
		}
	}

	if hasMinWorkers && monitorsNo%membersCount != 0 {
		workersToAcquire++
	}

	locks := balance(ctx, workers, workersToAcquire, workersInUse)
	member.Register(ctx, locks)

	return nil
}

func balance(ctx context.Context, workers []LockWorker, workersToAcquire int, workersInUse map[string]bool) []string {
	myRunningWorkers := map[string]bool{}
	for _, v := range workers {
		if v.Worker.IsRunning() {
			myRunningWorkers[v.Worker.Name()] = true
			workersInUse[v.Worker.Name()] = true
		}
	}

	running := len(myRunningWorkers)
	if running == workersToAcquire {
		return mapToString(myRunningWorkers)
	}

	for _, v := range workers {
		if running > workersToAcquire {
			if !v.Worker.IsRunning() {
				continue
			}

			v.Worker.Stop()
			v.Lock.Unlock()
			delete(myRunningWorkers, v.Worker.Name())
			running--
		} else {
			if workersInUse[v.Worker.Name()] {
				continue
			}

			release, _ := v.Lock.Lock()
			if release != nil {
				go v.Worker.Start(ctx, release)
				myRunningWorkers[v.Worker.Name()] = true
				running++
			}
		}
		if running == workersToAcquire {
			break
		}
	}
	return mapToString(myRunningWorkers)
}

func mapToString(m map[string]bool) []string {
	s := make([]string, 0, len(m))
	for k := range m {
		s = append(s, k)
	}
	return s
}
