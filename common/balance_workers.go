package common

import (
	"context"
	"time"

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

type Worker interface {
	Name() string
	IsRunning() bool
	Start(context.Context)
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

	// check if all nodes have the minimum workers. Only after that, remaining workers, at most one, be picked up.
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
			v.Lock.Unlock(ctx)
			delete(myRunningWorkers, v.Worker.Name())
			running--
		} else {
			if workersInUse[v.Worker.Name()] {
				continue
			}

			release, _ := v.Lock.Lock(ctx)
			if release != nil {
				go func() {
					ctx, cancel := context.WithCancel(ctx)
					select {
					case <-release:
					case <-ctx.Done():
						v.Lock.Unlock(ctx)
					}
					cancel()
				}()
				go v.Worker.Start(ctx)
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
