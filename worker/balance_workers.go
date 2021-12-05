package worker

import (
	"context"
	"time"

	"github.com/quintans/eventsourcing/log"
)

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
	IsRunning() bool
	Start(context.Context) bool
	Stop(context.Context)
}

type Balancer interface {
	Name() string
	Start(ctx context.Context) <-chan struct{}
}

var _ Balancer = (*MembersBalancer)(nil)

type MembersBalancer struct {
	name      string
	logger    log.Logger
	member    Memberlister
	workers   []Worker
	heartbeat time.Duration
}

func NewSingleBalancer(logger log.Logger, name string, worker Worker, heartbeat time.Duration) *MembersBalancer {
	return NewMembersBalancer(logger, name, NewInMemMemberList(), []Worker{worker}, heartbeat)
}

func NewMembersBalancer(logger log.Logger, name string, member Memberlister, workers []Worker, heartbeat time.Duration) *MembersBalancer {
	return &MembersBalancer{
		name: name,
		logger: logger.WithTags(log.Tags{
			"name": name,
		}),
		member:    member,
		workers:   workers,
		heartbeat: heartbeat,
	}
}

func (b *MembersBalancer) Name() string {
	return b.name
}

func (b *MembersBalancer) Start(ctx context.Context) <-chan struct{} {
	done := make(chan struct{})
	go func() {
		ticker := time.NewTicker(b.heartbeat)
		defer ticker.Stop()
		for {
			err := b.run(ctx)
			if err != nil {
				b.logger.Warnf("Error while balancing partitions: %v", err)
			}
			select {
			case <-ctx.Done():
				// shutdown
				for _, w := range b.workers {
					w.Stop(context.Background())
				}
				err = b.member.Unregister(context.Background())
				if err != nil {
					b.logger.Warnf("Error while cleaning register on shutdown: %v", err)
				}
				close(done)
				return
			case <-ticker.C:
			}
		}
	}()
	return done
}

func (b *MembersBalancer) run(ctx context.Context) error {
	members, err := b.member.List(ctx)
	if err != nil {
		return err
	}

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

	monitorsNo := len(b.workers)
	workersToAcquire := monitorsNo / membersCount

	// check if all members have the minimum workers. Only after that, any additional can be picked up.
	allHaveMinWorkers := true
	workersInUse := map[string]bool{}
	for _, m := range members {
		// checking if others have min required workers running.
		// This member might be included
		if len(m.Workers) < workersToAcquire {
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
	for _, v := range b.workers {
		if v.IsRunning() {
			workersInUse[v.Name()] = true
			myRunningWorkers[v.Name()] = true
		}
	}
	// if my current running workers are less, then not all members have the min workers
	if len(myRunningWorkers) < workersToAcquire {
		allHaveMinWorkers = false
	}

	if allHaveMinWorkers && monitorsNo%membersCount != 0 {
		workersToAcquire++
	}

	locks := b.balance(ctx, workersToAcquire, workersInUse, myRunningWorkers)
	return b.member.Register(ctx, locks)
}

func (b *MembersBalancer) balance(ctx context.Context, workersToAcquire int, workersInUse, myRunningWorkers map[string]bool) []string {
	running := len(myRunningWorkers)
	if running == workersToAcquire {
		return mapToString(myRunningWorkers)
	}

	for _, v := range b.workers {
		if running > workersToAcquire {
			if !v.IsRunning() {
				continue
			}

			v.Stop(ctx)
			delete(myRunningWorkers, v.Name())
			running--
		} else {
			if workersInUse[v.Name()] {
				continue
			}

			if v.Start(ctx) {
				myRunningWorkers[v.Name()] = true
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

type InMemMemberList struct{}

func NewInMemMemberList() *InMemMemberList {
	return &InMemMemberList{}
}

func (m InMemMemberList) Name() string {
	return "InMemMemberList"
}

func (m InMemMemberList) List(context.Context) ([]MemberWorkers, error) {
	return []MemberWorkers{}, nil
}

func (m *InMemMemberList) Register(context.Context, []string) error {
	return nil
}

func (m *InMemMemberList) Unregister(context.Context) error {
	return nil
}
