package common

import (
	"context"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
)

type Booter interface {
	OnBoot(context.Context) error
	Wait() <-chan struct{} // block if not ready
	Cancel()
}

type Locker interface {
	Lock() (bool, error)
	Extend() (bool, error)
	Unlock() (bool, error)
}

type WaitResult int

const (
	Done WaitResult = iota + 1
	Extend
	Released
)

// BootMonitor is responsible for refreshing the lease
type BootMonitor struct {
	name     string
	refresh  time.Duration
	lockable Booter
	cancel   context.CancelFunc
	mu       sync.RWMutex
}

type Option func(r *BootMonitor)

func WithRefreshInterval(refresh time.Duration) func(r *BootMonitor) {
	return func(r *BootMonitor) {
		r.refresh = refresh
	}
}

func NewBootMonitor(name string, lockable Booter, options ...Option) BootMonitor {
	l := BootMonitor{
		name:     name,
		refresh:  10 * time.Second,
		lockable: lockable,
	}

	for _, o := range options {
		o(&l)
	}

	return l
}

func (l BootMonitor) IsRunning() bool {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.cancel != nil
}

func (l *BootMonitor) Stop() {
	l.mu.Lock()
	if l.cancel != nil {
		l.cancel()
		l.cancel = nil
	}
	l.mu.Unlock()
}

func (l BootMonitor) Start(ctx context.Context, locker Locker) {
	ctx, cancel := context.WithCancel(ctx)

	l.mu.Lock()
	l.cancel = cancel
	l.mu.Unlock()

	defer func() {
		l.Stop()
	}()

	release := l.lockable.Wait()
	// acquired lock
	// OnBoot may take some time (minutes) to finish since it will be doing synchronisation
	go func() {
		err := l.lockable.OnBoot(ctx)
		if err != nil {
			log.Error("Error booting:", err)
		}
	}()
	for {
		switch l.waitOn(ctx.Done(), release) {
		case Done:
			return
		case Extend:
			ok, _ := locker.Extend()
			if !ok {
				l.lockable.Cancel()
				return
			}
		case Released:
			locker.Unlock()
			return
		}
	}
}

func (l BootMonitor) waitOn(done <-chan struct{}, release <-chan struct{}) WaitResult {
	timer := time.NewTimer(l.refresh)
	defer timer.Stop()
	select {
	// happens when the latch was cancelled
	case <-done:
		return Done
	case <-timer.C:
		return Extend
	case <-release:
		return Released
	}
}

type PartitionSlot struct {
	From uint32
	To   uint32
}

func (ps PartitionSlot) Size() uint32 {
	return ps.To - ps.From + 1
}

func ParseSlots(slots []string) ([]PartitionSlot, error) {
	pslots := make([]PartitionSlot, len(slots))
	for k, v := range slots {
		s, err := ParseSlot(v)
		if err != nil {
			return nil, err
		}
		pslots[k] = s
	}
	return pslots, nil
}

func ParseSlot(slot string) (PartitionSlot, error) {
	ps := strings.Split(slot, "-")
	s := PartitionSlot{}
	from, err := strconv.Atoi(ps[0])
	if err != nil {
		return PartitionSlot{}, err
	}
	s.From = uint32(from)
	if len(ps) == 2 {
		to, err := strconv.Atoi(ps[1])
		if err != nil {
			return PartitionSlot{}, err
		}
		s.To = uint32(to)
	} else {
		s.To = s.From
	}
	return s, nil
}

type Memberlister interface {
	PingAndCount(context.Context) (int, error)
}

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

func (r *RedisMemberlist) PingAndCount(ctx context.Context) (int, error) {
	err := r.rdb.Set(ctx, r.name, r.prefix, r.expiration).Err()
	if err != nil {
		return 0, err
	}

	var cursor uint64
	var n int
	for {
		var keys []string
		var err error
		keys, cursor, err = r.rdb.Scan(ctx, cursor, r.prefix+"-*", 10).Result()
		if err != nil {
			panic(err)
		}
		n += len(keys)
		if cursor == 0 {
			break
		}
	}
	return n, nil
}

type LockMonitor struct {
	Lock    Locker
	Monitor *BootMonitor
}

func BalancePartitions(ctx context.Context, member Memberlister, lms []LockMonitor, heartbeat time.Duration) {
	ticker := time.NewTicker(heartbeat)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			run(ctx, member, lms)
			err := run(ctx, member, lms)
			if err != nil {
				log.Warnf("Error while balancing partitions: %v", err)
				continue
			}
		}
	}
}

func run(ctx context.Context, member Memberlister, lms []LockMonitor) error {
	count, err := member.PingAndCount(ctx)
	if err != nil {
		return err
	}

	monitorsNo := len(lms)
	slots := monitorsNo / count
	if monitorsNo%count != 0 {
		slots++
	}

	balance(ctx, lms, slots)

	return nil
}

func balance(ctx context.Context, lms []LockMonitor, slots int) {
	running := 0
	for _, v := range lms {
		if v.Monitor.IsRunning() {
			running++
		}
	}

	if running == slots {
		return
	}

	for _, v := range lms {
		if running > slots {
			if v.Monitor.IsRunning() {
				v.Monitor.Stop()
				running--
			}
		} else {
			if !v.Monitor.IsRunning() {
				ok, _ := v.Lock.Lock()
				if ok {
					go v.Monitor.Start(ctx, v.Lock)
					running++
				}
			}
		}
		if running == slots {
			return
		}
	}
}
