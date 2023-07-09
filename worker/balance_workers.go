package worker

import (
	"context"
	"sync"
	"time"

	"github.com/quintans/eventsourcing/log"
)

var turbo = time.Second

type MemberWorkers struct {
	Name    string
	Workers []string
}

// Memberlister represents a member of a list to which the a worker may be assigned
type Memberlister interface {
	Name() string
	// List lists all servers and the workers under each of them
	List(context.Context) ([]MemberWorkers, error)
	// Register registers the workers under this member
	Register(context.Context, []string) error
	// Unregister removes itself from the list
	Unregister(context.Context) error
}

// Worker represents an execution that can be started or stopped
type Worker interface {
	Name() string
	Group() string
	IsRunning() bool
	IsBalanceable() bool
	Start(context.Context) bool
	Stop(ctx context.Context)
}

type Balancer interface {
	Name() string
	Start(ctx context.Context)
	Stop(ctx context.Context)
}

var _ Balancer = (*MembersBalancer)(nil)

type MembersBalancerOption func(*MembersBalancer)

// MembersBalancer manages the lifecycle of workers as well balance them over all members.
//
// A worker can be unbalanced or paused.
//   - Unbalanced worker will never be balanced across the different members and will always run.
//   - Paused worker will hold its execution and will not be balanced-
type MembersBalancer struct {
	name             string
	logger           log.Logger
	member           Memberlister
	workers          []Worker
	heartbeat        time.Duration
	currentHeartbeat time.Duration
	turbo            time.Duration

	mu   sync.Mutex
	done chan struct{}
}

func NewMembersBalancer(logger log.Logger, name string, member Memberlister, workers []Worker, options ...MembersBalancerOption) *MembersBalancer {
	mb := &MembersBalancer{
		name: name,
		logger: logger.WithTags(log.Tags{
			"name": name,
		}),
		member:           member,
		workers:          workers,
		heartbeat:        5 * time.Second,
		currentHeartbeat: 5 * time.Second,
		turbo:            turbo,
	}
	for _, o := range options {
		o(mb)
	}
	return mb
}

func WithHeartbeat(heartbeat time.Duration) MembersBalancerOption {
	return func(b *MembersBalancer) {
		b.heartbeat = heartbeat
		b.currentHeartbeat = heartbeat
	}
}

func WithTurboHeartbeat(heartbeat time.Duration) MembersBalancerOption {
	return func(b *MembersBalancer) {
		b.turbo = heartbeat
	}
}

func (b *MembersBalancer) Name() string {
	return b.name
}

func (b *MembersBalancer) Start(ctx context.Context) {
	done := make(chan struct{})
	b.mu.Lock()
	b.done = done
	b.mu.Unlock()
	go func() {
		ticker := time.NewTicker(b.heartbeat)
		defer ticker.Stop()
		for {
			err := b.run(ctx, ticker)
			if err != nil {
				b.logger.Warnf("Error while balancing %s's partitions: %+v", b.name, err)
			}
			select {
			case <-done:
				return
			case <-ctx.Done():
				// shutdown
				b.Stop(context.Background())
				return
			case <-ticker.C:
			}
		}
	}()
}

func (b *MembersBalancer) Stop(ctx context.Context) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.done == nil {
		return
	}

	for _, w := range b.workers {
		w.Stop(ctx)
	}

	err := b.member.Unregister(context.Background())
	if err != nil {
		b.logger.Warnf("Error while cleaning register for %s: %+v", b.name, err)
	}

	close(b.done)
	b.done = nil
}

func (b *MembersBalancer) run(ctx context.Context, ticker *time.Ticker) error {
	members, err := b.member.List(ctx)
	if err != nil {
		return err
	}
	balancedWorkers := b.filterBalancedWorkers()
	members = filterMemberWorkers(members, balancedWorkers)

	// if current member is not in the list, add it to the member count
	present := false
	for _, v := range members {
		if v.Name == b.member.Name() {
			present = true
			break
		}
	}
	membersCount := len(members)
	if !present {
		membersCount++
	}

	monitorsNo := len(balancedWorkers)
	minWorkersToAcquire := monitorsNo / membersCount

	// check if all members have the minimum workers. Only after that, any additional can be picked up.
	allHaveMinWorkers := true
	workersInUse := map[string]bool{}
	for _, m := range members {
		// checking if others have min required workers running.
		// This member might be included
		if len(m.Workers) < minWorkersToAcquire {
			allHaveMinWorkers = false
		}
		// map only other members workers
		if m.Name != b.member.Name() {
			for _, v := range m.Workers {
				workersInUse[v] = true
			}
		}
	}
	// mapping my current workers
	myRunningWorkers := map[string]bool{}
	for _, v := range balancedWorkers {
		if v.IsRunning() {
			workersInUse[v.Name()] = true
			myRunningWorkers[v.Name()] = true
		}
	}
	// if my current running workers are less, then not all members have the min workers
	if len(myRunningWorkers) < minWorkersToAcquire {
		allHaveMinWorkers = false
	}

	if allHaveMinWorkers && monitorsNo%membersCount != 0 {
		minWorkersToAcquire++
	}

	// make sure that all unpaused and unbalanced workers are running
	for _, w := range b.workers {
		if !w.IsBalanceable() {
			w.Start(ctx)
		}
	}

	locks := b.balance(ctx, balancedWorkers, minWorkersToAcquire, workersInUse, myRunningWorkers)

	// reset ticker if needed
	if b.enableTurbo(balancedWorkers, members, locks, minWorkersToAcquire) {
		// turbo is activated when there is a need to rebalance workers
		if b.currentHeartbeat != b.turbo {
			b.currentHeartbeat = b.turbo
			ticker.Reset(b.turbo)
		}
	} else {
		if b.currentHeartbeat != b.heartbeat {
			b.currentHeartbeat = b.heartbeat
			ticker.Reset(b.heartbeat)
		}
	}

	return b.member.Register(ctx, locks)
}

func (b *MembersBalancer) enableTurbo(workers []Worker, members []MemberWorkers, myWorkers []string, minWorkersToAcquire int) bool {
	total := len(workers)
	acquired := len(myWorkers)
	if acquired < minWorkersToAcquire {
		return true
	}
	for _, m := range members {
		if m.Name != b.member.Name() {
			if len(m.Workers) < minWorkersToAcquire {
				return true
			}
			acquired += len(m.Workers)
		}
	}
	return total != acquired
}

func (b *MembersBalancer) filterBalancedWorkers() []Worker {
	var balanceable []Worker
	for _, w := range b.workers {
		if w.IsBalanceable() {
			balanceable = append(balanceable, w)
		}
	}
	return balanceable
}

func filterMemberWorkers(members []MemberWorkers, workers []Worker) []MemberWorkers {
	for _, m := range members {
		newWorkers := []string{}
		for _, v := range m.Workers {
			for _, w := range workers {
				if v == w.Name() {
					newWorkers = append(newWorkers, v)
					break
				}
			}
		}
		m.Workers = newWorkers
	}
	return members
}

func (b *MembersBalancer) balance(ctx context.Context, workers []Worker, workersToAcquire int, workersInUse, myRunningWorkers map[string]bool) []string {
	running := len(myRunningWorkers)
	if running == workersToAcquire {
		return mapToString(myRunningWorkers)
	}

	for _, w := range workers {
		if running > workersToAcquire {
			if !w.IsRunning() {
				continue
			}

			w.Stop(ctx)
			delete(myRunningWorkers, w.Name())
			running--
		} else {
			if workersInUse[w.Name()] {
				continue
			}

			if w.Start(ctx) {
				myRunningWorkers[w.Name()] = true
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

var _ Balancer = (*SingleBalancer)(nil)

type SingleBalancer struct {
	logger    log.Logger
	name      string
	worker    Worker
	heartbeat time.Duration

	mu   sync.Mutex
	done chan struct{}
}

func NewSingleBalancer(logger log.Logger, name string, worker Worker, heartbeat time.Duration) *SingleBalancer {
	return &SingleBalancer{
		logger:    logger,
		name:      name,
		worker:    worker,
		heartbeat: heartbeat,
	}
}

func (b *SingleBalancer) Name() string {
	return b.name
}

func (b *SingleBalancer) Start(ctx context.Context) {
	done := make(chan struct{})
	b.mu.Lock()
	b.done = done
	b.mu.Unlock()
	go func() {
		ticker := time.NewTicker(b.heartbeat)
		defer ticker.Stop()
		for {
			b.worker.Start(ctx)
			select {
			case <-done:
				return
			case <-ctx.Done():
				// shutdown
				b.Stop(context.Background())
				return
			case <-ticker.C:
			}
		}
	}()
}

func (b *SingleBalancer) Stop(ctx context.Context) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.done == nil {
		return
	}

	b.worker.Stop(ctx)

	close(b.done)
	b.done = nil
}
